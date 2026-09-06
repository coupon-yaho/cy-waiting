package com.kafkick.waiting.gateway;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 서킷의 상태 전이를 <b>진입·해제 쌍으로</b> 남긴다 (LG-2).
 *
 * <p>지표는 초 단위로 뭉개져 남고 보존도 짧다. 장애가 걷힌 뒤 "언제 열려 얼마나
 * 오래, 몇 건을 막았는가" 는 전이 로그만 답한다 — 회복 판정이 그 위에 선다.
 */
final class CircuitTransitionLog {

    private static final Logger log = LoggerFactory.getLogger(CircuitTransitionLog.class);

    /**
     * 열린 구간.
     *
     * @param since   열린 시각(단조 나노). 벽시계는 NTP 가 되돌리면 음수가 된다
     * @param blocked 그동안 막은 호출 수
     */
    private record Opened(long since, LongAdder blocked) {
    }

    /** 이름별로 따로 센다 — 서킷은 인스턴스별이다 (R-10). 크기는 뒷단 수로 묶인다. */
    private final ConcurrentMap<String, Opened> opened = new ConcurrentHashMap<>();

    /** 반쯤 열린 창마다의 프로브 수. 창이 끝나면 걷는다. */
    private final ConcurrentMap<String, Probes> probes = new ConcurrentHashMap<>();

    private final LongSupplier nanoTicker;

    private CircuitTransitionLog(LongSupplier nanoTicker) {
        this.nanoTicker = Objects.requireNonNull(nanoTicker, "nanoTicker 는 필수다");
    }

    static CircuitTransitionLog create() {
        return new CircuitTransitionLog(System::nanoTime);
    }

    /** 구간 시계를 받는다. 고정하지 못하면 지속 시간이 시험에서 늘 0 이다 (TS-4). */
    static CircuitTransitionLog of(LongSupplier nanoTicker) {
        return new CircuitTransitionLog(nanoTicker);
    }

    /**
     * <b>나중에 생기는 서킷도 받는다.</b> 서킷은 인스턴스별이라 뒷단이 늘면 이름도
     * 는데, 붙일 때 있던 것만 보면 새 인스턴스의 장애가 통째로 조용하다.
     */
    void watch(CircuitBreakerRegistry registry) {
        Objects.requireNonNull(registry, "registry 는 필수다");
        registry.getAllCircuitBreakers().forEach(this::attach);
        registry.getEventPublisher().onEntryAdded(added -> attach(added.getAddedEntry()));
    }

    private void attach(CircuitBreaker breaker) {
        breaker.getEventPublisher()
                .onStateTransition(event ->
                        moved(breaker.getName(), event.getStateTransition().getToState()))
                // **막은 건수는 우리가 센다.** 라이브러리 쪽 값은 전이와 함께 새
                // 상태로 갈리므로, 해제 시점에는 이미 0 이다.
                .onCallNotPermitted(event -> blocked(breaker.getName()))
                // **프로브도 우리가 센다.** 같은 이유다 — 전이 순간에 라이브러리
                // 값을 읽으면 이미 새 상태의 것이라, 방금 끝난 창이 몇 건을
                // 모았는지는 거기 없다.
                .onSuccess(event -> probed(breaker, event.getElapsedDuration(), false))
                .onError(event -> probed(breaker, event.getElapsedDuration(), true));
    }

    /**
     * 반쯤 열린 창에서만 센다. 닫힌 구간의 정상 호출까지 세면 뜻이 없다.
     *
     * <p><b>느림과 오류를 갈라 센다.</b> 창은 두 길로 열리고 고칠 자리가 다르다 —
     * 오류는 뒷단이 죽은 것이고, 느림은 자극 이전의 호출이 창에 남은 쪽일 수 있다.
     */
    private void probed(CircuitBreaker breaker, Duration elapsed, boolean failed) {
        Probes window = probes.get(breaker.getName());
        if (window == null) {
            return;
        }
        window.count().increment();
        if (failed) {
            window.failed().increment();
        }
        // 느림은 성공이어도 느림이다. 판정하는 임계는 서킷이 든 그 값을 그대로 쓴다.
        if (elapsed.compareTo(breaker.getCircuitBreakerConfig()
                .getSlowCallDurationThreshold()) >= 0) {
            window.slow().increment();
        }
    }

    /**
     * 반쯤 열린 창에서 모은 프로브와 그 창이 산 시간.
     *
     * <p><b>시간이 계수보다 단단하다.</b> 창 길이가 시한 근방이면 표본을 못 채워
     * 만료된 것이고, 훨씬 짧으면 채우고 실패한 것이다 — 계수 한 건이 경계에서
     * 어긋나도 이 판단은 안 눕는다.
     */
    private record Probes(long since, LongAdder count, LongAdder slow, LongAdder failed) {

        static Probes opened(long since) {
            return new Probes(since, new LongAdder(), new LongAdder(), new LongAdder());
        }

        /** 창이 무엇을 모았는지. 총계만으로는 태운 사유가 안 갈린다. */
        String describe() {
            return "%d건(느림 %d · 오류 %d)".formatted(
                    count.sum(), slow.sum(), failed.sum());
        }
    }

    private void blocked(String name) {
        Opened window = opened.get(name);
        if (window != null) {
            window.blocked().increment();
        }
    }

    private void moved(String name, CircuitBreaker.State to) {
        switch (to) {
            case OPEN, FORCED_OPEN -> entered(name, to);
            case CLOSED -> exited(name);
            // 프로브 구간도 남긴다. 열림과 닫힘만 보면 회복을 몇 번 시도했는지가 빈다.
            case HALF_OPEN -> {
                // 창마다 처음부터 센다. 안 그러면 앞 창의 수가 다음 판단에 섞인다.
                probes.put(name, Probes.opened(nanoTicker.getAsLong()));
                log.info("서킷 반쯤 열림 — {} 로 프로브를 보낸다. 실패하면 다시 연다", name);
            }
            default -> {
                // 반쯤 열림도 열림도 아닌 곳으로 가면 그 창은 끝난 것이다.
                // 위와 같은 이유로 걷는다 — 값이 아니라 비용이다.
                probes.remove(name);
                log.info("서킷 상태 전이 — {} 가 {} 로 갔다", name, to);
            }
        }
    }

    /**
     * <b>자동으로 걷히는 전이라 WARN 이다</b> (LG-7). ERROR 로 올리면 사람을 부르는
     * 알람이 매 진동마다 운다.
     */
    private void entered(String name, CircuitBreaker.State to) {
        // **다시 열리는 것은 새 구간이 아니다.** OPEN → HALF_OPEN → OPEN 은 회복을
        // 시도했다 실패한 것이므로, 덮어쓰면 원래 시작 시각과 그동안 막은 건수가
        // 사라진다. 그러면 닫힘 로그가 장애를 실제보다 짧고 가볍게 말한다.
        //
        // **시각은 한 번만 읽는다.** 두 번 읽으면 첫 열림에서도 값이 달라져,
        // 회복을 시도한 적이 없는데 실패했다는 줄이 남는다.
        long now = nanoTicker.getAsLong();
        Opened before = opened.putIfAbsent(name, new Opened(now, new LongAdder()));
        log.warn("서킷 열림({}) — {} 로 가는 발급을 막는다. 그 인스턴스의 지연과 오류율을 확인하라",
                to, name);
        // **프로브 수를 같이 남긴다.** 창을 다 채우고 실패한 것과 못 채운 채
        // 시한이 만료된 것은 고칠 값이 다르다 — 앞엣것은 뒷단이고 뒤엣것은
        // 프로브 공급이다. 수가 없으면 로그로는 그 둘이 같은 줄이다.
        Probes window = probes.remove(name);
        if (before != null) {
            log.warn("회복 시도가 실패했다 — {} 가 {}초째 열려 있다, 프로브 {} · 창 {}초",
                    name, NANOSECONDS.toSeconds(now - before.since()),
                    window == null ? "0건" : window.describe(),
                    window == null ? 0 : NANOSECONDS.toSeconds(now - window.since()));
        }
    }

    private void exited(String name) {
        // **창을 여기서도 걷는다.** 회복이 성공해 닫히는 것도 창의 끝이다.
        //
        // **숫자가 틀려서가 아니다** — 다음 창은 반쯤 열릴 때 새로 만들므로 옛
        // 계수가 로그에 실릴 길은 없다. 안 걷으면 그 계수가 살아남아 닫힌 구간의
        // 정상 호출마다 맵 조회와 증가가 붙는다. 100K 구간에서 요청마다다.
        probes.remove(name);
        Opened window = opened.remove(name);
        if (window == null) {
            log.info("서킷 닫힘 — {} 가 다시 받는다", name);
            return;
        }
        log.info("서킷 닫힘 — {} 가 {}초 동안 {}건을 막았다", name,
                NANOSECONDS.toSeconds(nanoTicker.getAsLong() - window.since()),
                window.blocked().sum());
    }
}

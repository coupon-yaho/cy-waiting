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

    /** half-open 구간마다의 프로브 수. 그 구간이 끝나면 걷는다. */
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
                // 이름만 넘기면 설정에 든 상한을 못 찾는다.
                .onStateTransition(event ->
                        moved(breaker, event.getStateTransition().getToState()))
                // **막은 건수는 우리가 센다.** 라이브러리 쪽 값은 전이와 함께 새
                // 상태로 갈리므로, 해제 시점에는 이미 0 이다.
                .onCallNotPermitted(event -> blocked(breaker.getName()))
                .onSuccess(event -> probed(breaker, event.getElapsedDuration(), false))
                .onError(event -> probed(breaker, event.getElapsedDuration(), true));
    }

    /**
     * half-open 구간에서만 센다.
     *
     * <p>느림과 오류를 가른다. half-open 은 두 길로 실패하고 고칠 자리가 다르다.
     *
     * <p><b>이 수는 완료다.</b> 허가는 취득 시점에 깎이므로 만료 때 비행 중이던
     * 호출은 안 들어온다 — 그 차이는 {@code notPermitted} 가 답한다.
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
        // 서킷과 같은 부등호를 쓴다. 경계가 갈리면 서킷의 판단을 재구성할 수 없다.
        if (elapsed.compareTo(breaker.getCircuitBreakerConfig()
                .getSlowCallDurationThreshold()) > 0) {
            window.slow().increment();
        }
    }

    /**
     * half-open 구간이 모은 프로브와 그 구간이 산 시간.
     *
     * <p>길이가 {@code maxWaitDurationInHalfOpenState} 근방이면 표본을 못 채운
     * 것이고, 훨씬 짧으면 채우고 실패한 것이다.
     */
    private record Probes(long since, LongAdder count, LongAdder slow, LongAdder failed,
            LongAdder notPermitted) {

        static Probes halfOpened(long since) {
            return new Probes(since, new LongAdder(), new LongAdder(), new LongAdder(),
                    new LongAdder());
        }

        /** 구간이 무엇을 모았는지. 없을 때도 여기서 답한다. */
        static String describe(Probes window) {
            if (window == null) {
                return "probes=0 slow=0 errors=0 notPermitted=0";
            }
            return "probes=%d slow=%d errors=%d notPermitted=%d".formatted(
                    window.count.sum(), window.slow.sum(), window.failed.sum(),
                    window.notPermitted.sum());
        }

        /** 구간이 산 시간(ms). 상한이 1초 미만일 수 있어 초로 자르면 0 이 된다. */
        static long aliveMs(Probes window, long now) {
            return window == null ? 0 : NANOSECONDS.toMillis(now - window.since());
        }
    }

    private void blocked(String name) {
        Opened window = opened.get(name);
        if (window != null) {
            window.blocked().increment();
        }
        // half-open 에서 막혔다면 허가가 소진된 것이다. 공급이 없어 못 채운 구간과
        // 프로브가 매달려 못 채운 구간이 이 값으로 갈린다.
        Probes probe = probes.get(name);
        if (probe != null) {
            probe.notPermitted().increment();
        }
    }

    private void moved(CircuitBreaker breaker, CircuitBreaker.State to) {
        String name = breaker.getName();
        switch (to) {
            case OPEN, FORCED_OPEN -> entered(breaker, to);
            case CLOSED -> exited(breaker);
            // 프로브 구간도 남긴다. 열림과 닫힘만 보면 회복을 몇 번 시도했는지가 빈다.
            case HALF_OPEN -> {
                // 구간마다 처음부터 센다.
                probes.put(name, Probes.halfOpened(nanoTicker.getAsLong()));
                log.info("서킷 반쯤 열림 — {} 로 프로브를 보낸다. 실패하면 다시 연다", name);
            }
            default -> {
                probes.remove(name);
                log.info("서킷 상태 전이 — {} 가 {} 로 갔다", name, to);
            }
        }
    }

    /**
     * <b>자동으로 걷히는 전이라 WARN 이다</b> (LG-7). ERROR 로 올리면 사람을 부르는
     * 알람이 매 진동마다 운다.
     */
    private void entered(CircuitBreaker breaker, CircuitBreaker.State to) {
        String name = breaker.getName();
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
        // 다 채우고 실패한 것과 못 채운 채 만료된 것은 고칠 자리가 다르다.
        Probes window = probes.remove(name);
        if (before != null) {
            log.warn("회복 시도가 실패했다 — {} 가 {}초째 열려 있다, {} window={}/{}ms",
                    name, NANOSECONDS.toSeconds(now - before.since()),
                    Probes.describe(window), Probes.aliveMs(window, now),
                    breaker.getCircuitBreakerConfig()
                            .getMaxWaitDurationInHalfOpenState().toMillis());
        }
    }

    private void exited(CircuitBreaker breaker) {
        String name = breaker.getName();
        // 성공으로 닫히는 것도 구간의 끝이다. 닫힌 뒤에 계속 세면 요청마다 붙는다.
        Probes probe = probes.remove(name);
        long at = nanoTicker.getAsLong();
        Opened window = opened.remove(name);
        if (window == null) {
            log.info("서킷 닫힘 — {} 가 다시 받는다, {} window={}ms", name,
                    Probes.describe(probe), Probes.aliveMs(probe, at));
            return;
        }
        log.info("서킷 닫힘 — {} 가 {}초 동안 {}건을 막았다, {} window={}ms", name,
                NANOSECONDS.toSeconds(at - window.since()),
                window.blocked().sum(),
                Probes.describe(probe), Probes.aliveMs(probe, at));
    }
}

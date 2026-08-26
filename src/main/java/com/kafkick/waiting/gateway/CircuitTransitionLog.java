package com.kafkick.waiting.gateway;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
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
                .onCallNotPermitted(event -> blocked(breaker.getName()));
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
            case HALF_OPEN -> log.info(
                    "서킷 반쯤 열림 — {} 로 프로브를 보낸다. 실패하면 다시 연다", name);
            default -> log.info("서킷 상태 전이 — {} 가 {} 로 갔다", name, to);
        }
    }

    /**
     * <b>자동으로 걷히는 전이라 WARN 이다</b> (LG-7). ERROR 로 올리면 사람을 부르는
     * 알람이 매 진동마다 운다.
     */
    private void entered(String name, CircuitBreaker.State to) {
        opened.put(name, new Opened(nanoTicker.getAsLong(), new LongAdder()));
        log.warn("서킷 열림({}) — {} 로 가는 발급을 막는다. 그 인스턴스의 지연과 오류율을 확인하라",
                to, name);
    }

    private void exited(String name) {
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

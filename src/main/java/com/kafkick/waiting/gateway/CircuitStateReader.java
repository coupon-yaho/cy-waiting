package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.CircuitState;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * 뒷단 서킷의 상태를 판정에 넘긴다 (F3).
 *
 * <p><b>메모리 안의 값이다.</b> resilience4j 레지스트리는 이 프로세스가 들고
 * 있으므로 요청 경로에서 읽어도 레디스를 안 친다 (불변식 1).
 */
public final class CircuitStateReader {

    private final CircuitBreakerRegistry circuits;
    private final String circuitName;

    private CircuitStateReader(CircuitBreakerRegistry circuits, String circuitName) {
        this.circuits = circuits;
        this.circuitName = circuitName;
    }

    /** 레지스트리가 없으면 <b>안 본 것으로</b> 친다 — 시험과 서킷을 안 붙인 배치다. */
    public static CircuitStateReader of(CircuitBreakerRegistry circuits, String circuitName) {
        return new CircuitStateReader(circuits, circuitName);
    }

    /**
     * 지금 상태. <b>모르면 정상으로 본다.</b>
     *
     * <p>모른다고 줄로 보내면 서킷을 안 붙인 배치에서 전 요청이 큐로 간다 —
     * 없는 장애를 만드는 셈이다. 서킷이 실제로 열렸을 때만 조인다.
     */
    public CircuitState now() {
        if (circuits == null) {
            return CircuitState.CLOSED;
        }
        return switch (circuits.circuitBreaker(circuitName).getState()) {
            case OPEN, FORCED_OPEN -> CircuitState.OPEN;
            case HALF_OPEN -> CircuitState.HALF_OPEN;
            // **DISABLED 도 정상이다.** 운영자가 서킷을 끈 것이지 뒷단이 죽은
            // 것이 아니다. 여기서 조이면 끄는 것이 곧 조이는 것이 된다.
            case CLOSED, DISABLED, METRICS_ONLY -> CircuitState.CLOSED;
        };
    }

    /** 서킷 상태를 읽는 이름. 폴백이 쓰는 것과 같아야 한다. */
    public static CircuitBreaker.State stateOf(CircuitBreakerRegistry circuits, String name) {
        return circuits.circuitBreaker(name).getState();
    }
}

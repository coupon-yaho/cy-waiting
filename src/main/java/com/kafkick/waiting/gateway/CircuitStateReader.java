package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.CircuitState;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * 뒷단 서킷의 상태를 판정에 넘긴다 (F3).
 *
 * <p><b>메모리 안의 값이다.</b> resilience4j 레지스트리는 이 프로세스가 들고
 * 있으므로 요청 경로에서 읽어도 레디스를 안 친다 (불변식 1).
 */
public final class CircuitStateReader {

    /**
     * 서킷을 보고 있는가. <b>1 이 아니면 F3 이 꺼져 있다.</b>
     *
     * <p>안 보는 것과 닫혀 있는 것이 같은 값을 내므로, 배선이 빠지면 판정도
     * 배분도 조용히 평소대로 돈다 — 다음 장애 때만 드러난다.
     */
    public static final String WIRED = "waiting.circuit.wired";

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
     * 서킷을 보고 있는가. <b>이름이 없으면 안 보는 것이다.</b>
     *
     * <p>레지스트리가 없거나 그 이름의 서킷이 아직 없으면 이 리더는 영원히
     * 닫힘을 낸다. 그 사실이 밖에서 보여야 한다.
     */
    public boolean wired() {
        return circuits != null && circuits.find(circuitName).isPresent();
    }

    /** 지표에 건다. <b>게이지다</b> — 지금 보고 있는지가 알고 싶은 것이다. */
    public CircuitStateReader bind(MeterRegistry meters) {
        Gauge.builder(WIRED, this, r -> r.wired() ? 1 : 0)
                .description("서킷을 판정·배분이 보고 있는가. 1 이 아니면 F3 이 꺼져 있다")
                .register(meters);
        return this;
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
        // **없는 이름을 만들지 않는다.** `circuitBreaker(name)` 은 없으면
        // 새로 만드는데, 그 유령은 영원히 닫혀 있다 — 이름이 어긋나면 F3 이
        // 켜진 것처럼 보이면서 실제로는 죽는다.
        return circuits.find(circuitName)
                .map(breaker -> switch (breaker.getState()) {
                    case OPEN -> CircuitState.OPEN;
                    case HALF_OPEN -> CircuitState.HALF_OPEN;
                    // **DISABLED 도 정상이다.** 운영자가 서킷을 끈 것이지 뒷단이
                    // 죽은 것이 아니다. 여기서 조이면 끄는 것이 곧 조이는 것이 된다.
                    //
                    // **FORCED_OPEN 도 여기다.** 그 상태에는 해제 조건이 없다 —
                    // 사람이 풀기 전까지 영원하다. 줄로 돌리면 전 쿠폰이 무기한
                    // 큐에 갇히고, 한산한 쿠폰에도 없던 줄이 생겨 스스로 유지된다.
                    // 킬스위치는 기존 폴백이 받는 것이 맞다.
                    case CLOSED, DISABLED, METRICS_ONLY, FORCED_OPEN -> CircuitState.CLOSED;
                })
                .orElse(CircuitState.CLOSED);
    }
}

package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.CircuitState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 하트비트를 놓친 회차의 배선 (CY-838).
 *
 * <p>레지스트리는 관측 실패를 받으면 감소 연속 수를 지운다. <b>그 호출이 배선에서 빠지면</b> 레디스가
 * 흔들리는 동안 늦은 하트비트로 적게 센 관측이 실패를 사이에 두고도 연속으로 쌓여 멀쩡한 노드가
 * 분모에서 빠진다.
 */
class GatewayPresenceMissTest {

    private static final int 감소_확정_수 = 3;
    private static final int 노드 = 3;

    private final GatewayPresenceConfig config = new GatewayPresenceConfig();

    @Test
    @DisplayName("놓친_회차가_감소_연속을_끊는다")
    void 놓친_회차가_감소_연속을_끊는다() {
        GatewayRegistry registry = GatewayRegistry.of(감소_확정_수, 노드);
        Runnable 놓침 = config.missStep(() -> CircuitState.CLOSED, registry);

        registry.observed(노드 - 1);
        registry.observed(노드 - 1);
        놓침.run();
        registry.observed(노드 - 1);

        assertThat(registry.count()).as("실패를 사이에 둔 관측은 연속이 아니다").isEqualTo(노드);
    }

    @Test
    @DisplayName("놓친_회차가_통과_수를_모름으로_둔다")
    void 놓친_회차가_통과_수를_모름으로_둔다() {
        GatewayRegistry registry = GatewayRegistry.of(감소_확정_수, 노드);
        registry.passObserved(100, 노드, 노드);

        config.missStep(() -> CircuitState.CLOSED, registry).run();

        assertThat(registry.passRate()).isNegative();
    }
}

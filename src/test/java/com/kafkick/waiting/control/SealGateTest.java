package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 문을 잠글 때까지는 리더로 안 친다 (CY-892).
 *
 * <p>잠금이 끝나기 전에 회차가 돌면, 새 리더가 안 만지는 쿠폰에 유령의 지연된 몫이
 * 그대로 들어간다 — 같은 초에 두 리더의 몫이 나가면 초과 발급이다.
 */
@Tag("unit")
class SealGateTest {

    /** 기동 직후에는 잠글 것이 없다. 첫 리더가 여기서 막히면 아무도 안 돈다. */
    @Test
    @DisplayName("승계_전에는_리더면_바로_돈다")
    void 승계_전에는_리더면_바로_돈다() {
        assertThat(SealGate.of(() -> true).getAsBoolean()).isTrue();
    }

    @Test
    @DisplayName("잠그는_동안은_안_돈다")
    void 잠그는_동안은_안_돈다() {
        SealGate gate = SealGate.of(() -> true);

        gate.sealing();

        assertThat(gate.getAsBoolean()).as("리더가 됐어도 문을 안 잠갔으면 아직 아니다")
                .isFalse();
    }

    @Test
    @DisplayName("잠근_뒤에는_돈다")
    void 잠근_뒤에는_돈다() {
        SealGate gate = SealGate.of(() -> true);
        gate.sealing();

        gate.sealed();

        assertThat(gate.getAsBoolean()).isTrue();
    }

    /** 리더가 아니면 잠겼어도 안 돈다. 게이트가 리더 판정을 덮으면 안 된다. */
    @Test
    @DisplayName("리더가_아니면_잠겼어도_안_돈다")
    void 리더가_아니면_잠겼어도_안_돈다() {
        AtomicBoolean 리더인가 = new AtomicBoolean(false);
        SealGate gate = SealGate.of(리더인가::get);

        assertThat(gate.getAsBoolean()).isFalse();

        리더인가.set(true);
        assertThat(gate.getAsBoolean()).isTrue();
    }
}

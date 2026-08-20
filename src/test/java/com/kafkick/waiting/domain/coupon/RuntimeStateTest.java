package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RuntimeStateTest {

    @Test
    @DisplayName("런타임_상태는_네_가지다")
    void 런타임_상태는_네_가지다() {
        assertThat(RuntimeState.values())
                .containsExactly(
                        RuntimeState.IDLE,
                        RuntimeState.QUEUEING,
                        RuntimeState.DRAINING,
                        RuntimeState.CLOSED);
    }

    @Test
    @DisplayName("대기열_모드는_세_가지다")
    void 대기열_모드는_세_가지다() {
        assertThat(QueueMode.values())
                .containsExactly(QueueMode.OFF, QueueMode.ADAPTIVE, QueueMode.ALWAYS);
    }
}

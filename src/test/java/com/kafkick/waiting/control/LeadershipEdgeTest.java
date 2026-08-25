package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 매 틱 참인 것과 <b>방금 참이 된 것</b>은 다르다. 그 구별 없이 초기화하면
 * 판마다 초기화되고, 안 하면 비리더 구간의 상태를 자기 것으로 이어 쓴다.
 */
class LeadershipEdgeTest {

    private final AtomicBoolean 리더 = new AtomicBoolean();
    private final AtomicInteger 알림 = new AtomicInteger();
    private final LeadershipEdge edge =
            LeadershipEdge.of(리더::get, 알림::incrementAndGet);

    @Test
    @DisplayName("리더가_되면_한_번_알린다")
    void 리더가_되면_한_번_알린다() {
        리더.set(true);

        edge.getAsBoolean();
        edge.getAsBoolean();
        edge.getAsBoolean();

        assertThat(알림).hasValue(1);
    }

    @Test
    @DisplayName("리더가_아니면_안_알린다")
    void 리더가_아니면_안_알린다() {
        edge.getAsBoolean();

        assertThat(알림).hasValue(0);
    }

    @Test
    @DisplayName("잃었다_다시_잡으면_또_알린다")
    void 잃었다_다시_잡으면_또_알린다() {
        리더.set(true);
        edge.getAsBoolean();
        리더.set(false);
        edge.getAsBoolean();

        리더.set(true);
        edge.getAsBoolean();

        assertThat(알림).hasValue(2);
    }

    @Test
    @DisplayName("원래_값을_그대로_돌려준다")
    void 원래_값을_그대로_돌려준다() {
        assertThat(edge.getAsBoolean()).isFalse();
        리더.set(true);
        assertThat(edge.getAsBoolean()).isTrue();
    }
}

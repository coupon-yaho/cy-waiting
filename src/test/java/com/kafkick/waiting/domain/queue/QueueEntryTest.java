package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 줄에서의 자리. <b>없는 것과 맨 앞인 것을 뭉치지 않는다.</b> */
class QueueEntryTest {

    private QueueEntry 자리(QueueState state) {
        return new QueueEntry(state, 0, 1, false, false);
    }

    @Test
    @DisplayName("거절만_자리가_없다")
    void 거절만_자리가_없다() {
        // 줄에 없는 것은 아직 안 선 것이지 거절당한 것이 아니다 — 다시 서면 된다.
        assertThat(자리(QueueState.WAITING).accepted()).isTrue();
        assertThat(자리(QueueState.ADMITTED).accepted()).isTrue();
        assertThat(자리(QueueState.NOT_QUEUED).accepted()).isTrue();
        assertThat(자리(QueueState.REJECTED).accepted()).isFalse();
    }

    @Test
    @DisplayName("차례가_온_것만_입장이다")
    void 차례가_온_것만_입장이다() {
        assertThat(자리(QueueState.ADMITTED).admitted()).isTrue();
        assertThat(자리(QueueState.WAITING).admitted()).isFalse();
        assertThat(자리(QueueState.NOT_QUEUED).admitted()).isFalse();
    }

    @Test
    @DisplayName("상태_없이는_안_만들어진다")
    void 상태_없이는_안_만들어진다() {
        // 상태가 비면 읽는 쪽이 저마다 다르게 해석한다.
        assertThatThrownBy(() -> new QueueEntry(null, 0, 1, false, false))
                .isInstanceOf(NullPointerException.class);
    }
}

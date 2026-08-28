package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 줄에서의 자리. <b>없는 것과 맨 앞인 것을 뭉치지 않는다.</b> */
class QueueEntryTest {

    private QueueEntry 줄에_있는(QueueState state) {
        return new QueueEntry(state, 0, 1, false, false, false);
    }

    @Test
    @DisplayName("거절만_자리가_없다")
    void 거절만_자리가_없다() {
        // 줄에 없는 것은 아직 안 선 것이지 거절당한 것이 아니다 — 다시 서면 된다.
        assertThat(줄에_있는(QueueState.WAITING).accepted()).isTrue();
        assertThat(줄에_있는(QueueState.ADMITTED).accepted()).isTrue();
        assertThat(QueueEntry.notQueued().accepted()).isTrue();
        assertThat(QueueEntry.rejected().accepted()).isFalse();
    }

    @Test
    @DisplayName("차례가_온_것만_입장이다")
    void 차례가_온_것만_입장이다() {
        assertThat(줄에_있는(QueueState.ADMITTED).admitted()).isTrue();
        assertThat(줄에_있는(QueueState.WAITING).admitted()).isFalse();
        assertThat(QueueEntry.notQueued().admitted()).isFalse();
    }

    @Test
    @DisplayName("상태_없이는_안_만들어진다")
    void 상태_없이는_안_만들어진다() {
        // 상태가 비면 읽는 쪽이 저마다 다르게 해석한다.
        assertThatThrownBy(() -> new QueueEntry(null, 0, 1, false, false, false))
                .isInstanceOf(NullPointerException.class);
    }

    /**
     * 줄에 없다는 뜻의 상태가 자리를 들고 있으면, 그 조합을 전제로 통과하는
     * 시험이 생긴다. 운영이 못 만드는 것을 재게 된다.
     */
    @Test
    @DisplayName("줄에_없는데_자리를_들면_안_만들어진다")
    void 줄에_없는데_자리를_들면_안_만들어진다() {
        assertThatThrownBy(() -> new QueueEntry(QueueState.NOT_QUEUED, 0, 1, false, false, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueueEntry(QueueState.REJECTED, 3, -1, false, false, false))
                .isInstanceOf(IllegalArgumentException.class);
        // 순번만 들고 있어도 마찬가지다. 둘 중 하나만 보면 나머지가 새어 나간다.
        assertThatThrownBy(() -> new QueueEntry(QueueState.NOT_QUEUED, -1, 5, false, false, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("줄에_있는데_자리가_없으면_안_만들어진다")
    void 줄에_있는데_자리가_없으면_안_만들어진다() {
        assertThatThrownBy(() -> new QueueEntry(QueueState.WAITING, -1, -1, false, false, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueueEntry(QueueState.WAITING, 0, -1, false, false, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 차례가 오면 큐에서 빠진다. 유예 기록으로 되읽으면 순번을 모르지만 앞에
     * 아무도 없다는 것은 안다.
     */
    @Test
    @DisplayName("입장은_순번을_몰라도_된다")
    void 입장은_순번을_몰라도_된다() {
        assertThat(new QueueEntry(QueueState.ADMITTED, 0, QueueEntry.NONE, true, false, false).admitted())
                .isTrue();
        // 앞에 사람이 있는 입장은 없다.
        assertThatThrownBy(() -> new QueueEntry(QueueState.ADMITTED, 3, 1, false, false, false))
                .isInstanceOf(IllegalArgumentException.class);
        // 모른다는 뜻의 값이 아니면 음수도 안 된다.
        assertThatThrownBy(() -> new QueueEntry(QueueState.ADMITTED, 0, -5, false, false, false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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

    /**
     * <b>재방문은 새로 선 대기자만입니다.</b>
     *
     * <p>이미 줄에 있던 사람은 스크립트가 먼저 돌아가 기록을 안 보고, 조회는
     * 그 사실을 아예 안 싣습니다. 조합을 안 막으면 그것을 전제로 통과하는
     * 시험이 생기고, 운영이 못 만드는 상태를 재게 됩니다.
     */
    @Test
    @DisplayName("재방문은_새로_선_대기자만이다")
    void 재방문은_새로_선_대기자만이다() {
        assertThatCode(() -> new QueueEntry(QueueState.WAITING, 0, 1, false, false, true))
                .doesNotThrowAnyException();

        // **던지지 않고 낮춘다.** 등록 결과를 만들다 던지면 부르는 쪽이 그것을
        // 삼켜 fail-open 으로 흘린다 — 보고용 값 하나 때문에 줄이 통째로 열린다.
        assertThat(new QueueEntry(QueueState.WAITING, 0, 1, true, false, true).rejoined())
                .as("이미 서 있던 사람").isFalse();
        assertThat(new QueueEntry(QueueState.NOT_QUEUED, -1, -1, false, false, true).rejoined())
                .as("줄에 없는 사람").isFalse();
        assertThat(new QueueEntry(QueueState.ADMITTED, 0, 1, true, false, true).rejoined())
                .as("차례가 온 사람").isFalse();
    }

    /**
     * <b>내 뒤는 총원과 앞 인원을 같은 기준으로 센 값에서만 나온다</b> (CY-827). 따로 읽으면
     * "앞에 100명인데 총 80명" 이 되고, 그 상태로 뺄셈을 하면 음수가 화면에 나간다.
     */
    @Test
    @DisplayName("뒷사람_수는_같은_기준에서만_낸다")
    void 뒷사람_수는_같은_기준에서만_낸다() {
        QueueEntry 기다리는_사람 = new QueueEntry(QueueState.WAITING, 1, 100, true, false, false, 3);
        assertThat(기다리는_사람.behind()).as("총원 3 · 앞 1 · 나 1").isEqualTo(1);

        QueueEntry 총원을_모르는_사람 =
                new QueueEntry(QueueState.WAITING, 1, 100, true, false, false);
        assertThat(총원을_모르는_사람.behind()).as("모르면 모른다고 낸다")
                .isEqualTo(QueueEntry.UNKNOWN_TOTAL);

        QueueEntry 입장한_사람 = new QueueEntry(QueueState.ADMITTED, 0, 100, true, false, false, 3);
        assertThat(입장한_사람.behind()).as("줄을 떠난 사람의 뒤는 뜻이 없다")
                .isEqualTo(QueueEntry.UNKNOWN_TOTAL);

        // **음수를 안 낸다.** 기준이 어긋난 값이 들어와도 화면에 -1 명이 나가면 안 된다.
        QueueEntry 어긋난_값 = new QueueEntry(QueueState.WAITING, 5, 100, true, false, false, 2);
        assertThat(어긋난_값.behind()).as("어긋나면 0").isZero();
    }
}

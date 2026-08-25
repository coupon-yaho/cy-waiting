package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.queue.QueueEntry;
import com.kafkick.waiting.domain.queue.QueueState;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 픽스처가 <b>스크립트와 같은 계약을 읽는가</b>.
 *
 * <p>필터 시험은 전부 이 픽스처를 통해 상한을 밟는다. 픽스처가 실물보다
 * 관대해지면 게이트웨이가 잘못된 상한을 넘겨도 전부 초록으로 남는다 —
 * CY-508 이 실제로 그렇게 났다. 그래서 판정과 무관하게 계약만 따로 못 박는다.
 */
class FakeQueuePortTest {

    private static final String COUPON = "c1";

    private static final Instant 지금 = Instant.parse("2026-08-24T00:00:00Z");

    private final FakeQueuePort 줄 = FakeQueuePort.create();

    @Test
    @DisplayName("상한_0은_빈_줄에도_아무도_안_받는다")
    void 상한_0은_빈_줄에도_아무도_안_받는다() {
        // 0 을 상한 없음으로 읽으면 이 사람이 들어간다. enqueue.lua 도 같게 읽는다.
        QueueEntry entry = 줄.enqueue(COUPON, "누구", 0, 지금).block();

        assertThat(entry.accepted()).isFalse();
    }

    @Test
    @DisplayName("상한_없음은_계속_받는다")
    void 상한_없음은_계속_받는다() {
        for (int i = 0; i < 100; i++) {
            assertThat(줄.enqueue(COUPON, "대기자" + i, QueuePort.NO_LIMIT, 지금).block().accepted())
                    .isTrue();
        }
    }

    @Test
    @DisplayName("상한_없음보다_작은_값은_거절한다")
    void 상한_없음보다_작은_값은_거절한다() {
        // 실물은 스크립트가 오류를 낸다. 픽스처가 받아 주면 오염된 상한이
        // 게이트웨이 시험을 그냥 통과한다.
        assertThatThrownBy(() -> 줄.enqueue(COUPON, "누구", QueuePort.NO_LIMIT - 1, 지금).block())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("큐 길이 상한");
    }

    @Test
    @DisplayName("상한에_닿으면_그_다음부터_거절한다")
    void 상한에_닿으면_그_다음부터_거절한다() {
        assertThat(줄.enqueue(COUPON, "첫째", 2, 지금).block().accepted()).isTrue();
        assertThat(줄.enqueue(COUPON, "둘째", 2, 지금).block().accepted()).isTrue();

        assertThat(줄.enqueue(COUPON, "셋째", 2, 지금).block().accepted()).isFalse();
    }

    /**
     * 실물은 쿠폰마다 다른 ZSET 이다. 한 줄에 몰아넣으면 남의 쿠폰 상한이 나를
     * 막고, 남의 쿠폰 순번이 내 것으로 나온다.
     */
    @Test
    @DisplayName("다른_쿠폰의_줄은_따로_센다")
    void 다른_쿠폰의_줄은_따로_센다() {
        assertThat(줄.enqueue(COUPON, "누구", 1, 지금).block().accepted()).isTrue();

        // c1 의 상한이 찼어도 c2 는 빈 줄이다.
        assertThat(줄.enqueue("c2", "다른사람", 1, 지금).block().accepted()).isTrue();
        assertThat(줄.status("c2", "누구", 지금).block().state())
                .isEqualTo(QueueState.NOT_QUEUED);
    }

    @Test
    @DisplayName("이미_선_사람은_상한에_안_걸린다")
    void 이미_선_사람은_상한에_안_걸린다() {
        줄.enqueue(COUPON, "첫째", 1, 지금).block();

        // 새로고침 연타로 자기 자리를 잃으면 순번 역행이다 (불변식 3).
        QueueEntry 다시 = 줄.enqueue(COUPON, "첫째", 1, 지금).block();

        assertThat(다시.accepted()).isTrue();
        assertThat(다시.alreadyQueued()).isTrue();
    }
}

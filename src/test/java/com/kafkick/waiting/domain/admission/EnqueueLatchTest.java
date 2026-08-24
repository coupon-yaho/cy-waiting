package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 줄에 세운 직후의 한 구간을 메운다.
 *
 * <p>스냅샷은 한 틱 늦다. 그 사이 다음 초 창이 열리면 <b>방금 줄 선 사람을 신규
 * 유입이 넘어간다</b> — 스냅샷은 아직 한산하다고 말하기 때문이다 (불변식 4).
 */
class EnqueueLatchTest {

    private static final String COUPON = "c1";

    private final EnqueueLatch latch = EnqueueLatch.of(1_000, 3);

    @Test
    @DisplayName("세운_적_없으면_안_걸린다")
    void 세운_적_없으면_안_걸린다() {
        assertThat(latch.latched(COUPON, 100)).isFalse();
    }

    @Test
    @DisplayName("세우면_걸린다")
    void 세우면_걸린다() {
        latch.mark(COUPON, 100);

        assertThat(latch.latched(COUPON, 100)).isTrue();
    }

    /** 스냅샷이 따라잡을 때까지는 남아 있어야 한다. 한 틱만 살면 그 다음 창이 뚫린다. */
    @Test
    @DisplayName("스냅샷이_따라잡을_때까지_산다")
    void 스냅샷이_따라잡을_때까지_산다() {
        latch.mark(COUPON, 100);

        assertThat(latch.latched(COUPON, 102)).isTrue();
        assertThat(latch.latched(COUPON, 103)).isFalse();
    }

    /** 영원히 걸려 있으면 한 번 붐빈 쿠폰이 영영 안 풀린다. */
    @Test
    @DisplayName("지나면_풀린다")
    void 지나면_풀린다() {
        latch.mark(COUPON, 100);

        assertThat(latch.latched(COUPON, 200)).isFalse();
    }

    @Test
    @DisplayName("쿠폰마다_따로_건다")
    void 쿠폰마다_따로_건다() {
        latch.mark(COUPON, 100);

        assertThat(latch.latched("다른쿠폰", 100)).isFalse();
    }

    /** 시계가 뒤로 가도 미래의 표식이 영원히 살면 안 된다. */
    @Test
    @DisplayName("시계가_뒤로_가도_영원히_안_산다")
    void 시계가_뒤로_가도_영원히_안_산다() {
        latch.mark(COUPON, 1_000);

        assertThat(latch.latched(COUPON, 100)).isFalse();
    }

    /**
     * 인증이 없어 쿠폰 식별자로 아무 문자열이나 들어온다. 상한이 없으면 그것으로
     * 메모리를 밀어낼 수 있다.
     */
    @Test
    @DisplayName("키가_무제한으로_안_는다")
    void 키가_무제한으로_안_는다() {
        EnqueueLatch 좁은_것 = EnqueueLatch.of(10, 3);

        IntStream.range(0, 1_000).forEach(i -> 좁은_것.mark("c" + i, 100));

        assertThat(좁은_것.size()).isLessThanOrEqualTo(10);
    }

    @Test
    @DisplayName("수명이_양수가_아니면_안_만들어진다")
    void 수명이_양수가_아니면_안_만들어진다() {
        // 0 이면 세우자마자 풀려서 래치가 있으나 마나다.
        assertThatThrownBy(() -> EnqueueLatch.of(10, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

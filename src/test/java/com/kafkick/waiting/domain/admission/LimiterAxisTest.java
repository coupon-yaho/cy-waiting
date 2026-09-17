package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.SecondWindowLimiter.AcquireResult;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter.Axis;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 축마다 키 자리를 따로 준다 (CY-925).
 *
 * <p>한 축의 키가 클라이언트 입력에서 오면 그 축은 채워질 수 있다. 자리를 같이 쓰면 그때 다른 축의 새 키도
 * 못 만들어, 채운 쪽이 아니라 정상 쪽이 막힌다.
 */
class LimiterAxisTest {

    private static final long 지금 = 1_700_000_000L;

    @Test
    @DisplayName("한_축이_차도_다른_축은_새_키를_받는다")
    void 한_축이_차도_다른_축은_새_키를_받는다() {
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10);

        for (int i = 0; i < 50; i++) {
            limiter.tryAcquire(Axis.PRIMARY, "m" + i, 5, 지금);
        }

        assertThat(limiter.tryAcquire(Axis.SECONDARY, "10.0.0.1", 200, 지금))
                .as("다른 축의 새 키").isTrue();
    }

    @Test
    @DisplayName("축마다_자리를_따로_센다")
    void 축마다_자리를_따로_센다() {
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(3);

        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire(Axis.PRIMARY, "m" + i, 5, 지금);
            limiter.tryAcquire(Axis.SECONDARY, "i" + i, 5, 지금);
        }

        // 축이 둘이므로 자리도 축마다 셋이다. 합쳐서 세면 한 축이 다른 축의 자리를 먹는다.
        assertThat(limiter.size()).as("두 축의 합").isEqualTo(6);
    }

    /** 자리가 없는 축이 어느 쪽이든 자리 부족으로 답한다. 예산 고갈로 뭉뚱그리면 엉뚱한 데를 조인다. */
    @Test
    @DisplayName("어느_축의_자리가_없어도_자리_부족이다")
    void 어느_축의_자리가_없어도_자리_부족이다() {
        SecondWindowLimiter 앞이_찬_것 = SecondWindowLimiter.withMaxKeys(1);
        앞이_찬_것.tryAcquire(Axis.PRIMARY, "i1", 5, 지금);
        assertThat(앞이_찬_것.tryAcquireAll(Axis.SECONDARY, "m1", 5, Axis.PRIMARY, "i2", 5, 지금))
                .as("앞 축에 자리가 없다").isEqualTo(AcquireResult.KEY_SATURATED);

        SecondWindowLimiter 뒤가_찬_것 = SecondWindowLimiter.withMaxKeys(1);
        뒤가_찬_것.tryAcquire(Axis.SECONDARY, "m1", 5, 지금);
        assertThat(뒤가_찬_것.tryAcquireAll(Axis.SECONDARY, "m2", 5, Axis.PRIMARY, "i1", 5, 지금))
                .as("뒤 축에 자리가 없다").isEqualTo(AcquireResult.KEY_SATURATED);
    }

    /** 찬 축과 안 찬 축을 갈라 답한다. 한쪽만 보면 부르는 쪽이 늘 접거나 영영 안 접는다. */
    @Test
    @DisplayName("찬_축만_찼다고_답한다")
    void 찬_축만_찼다고_답한다() {
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(1);
        limiter.tryAcquire(Axis.SECONDARY, "m1", 5, 지금);

        assertThat(limiter.saturated(Axis.SECONDARY)).as("채운 축").isTrue();
        assertThat(limiter.saturated(Axis.PRIMARY)).as("안 채운 축").isFalse();
    }

    /** 축이 갈리면 키가 같아도 예산은 둘이다. 한 요청이 2 를 쓰는 것과 갈라야 한다. */
    @Test
    @DisplayName("축이_다르면_같은_키도_예산이_둘이다")
    void 축이_다르면_같은_키도_예산이_둘이다() {
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10);

        assertThat(limiter.tryAcquireAll(Axis.SECONDARY, "x", 1, Axis.PRIMARY, "x", 1, 지금))
                .isEqualTo(AcquireResult.ACQUIRED);
        assertThat(limiter.tryAcquireAll(Axis.SECONDARY, "x", 1, Axis.PRIMARY, "x", 1, 지금))
                .as("각 축이 제 상한을 따로 센다").isEqualTo(AcquireResult.COUPON_EXHAUSTED);
    }

    /** 축을 안 주면 앞 축이다. 기존 호출부가 그대로 돌아야 이 변경이 판정을 안 바꾼다. */
    @Test
    @DisplayName("축을_안_주면_앞_축이다")
    void 축을_안_주면_앞_축이다() {
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(1);

        assertThat(limiter.tryAcquire("c1", 5, 지금)).isTrue();
        assertThat(limiter.tryAcquire(Axis.PRIMARY, "c1", 5, 지금))
                .as("같은 자리를 쓴다").isTrue();
        assertThat(limiter.tryAcquire("c2", 5, 지금))
                .as("자리가 하나라 새 키는 못 받는다").isFalse();
    }
}

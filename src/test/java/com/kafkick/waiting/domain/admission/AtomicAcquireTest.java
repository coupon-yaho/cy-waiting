package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.SecondWindowLimiter.AcquireResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 두 예산을 전부-아니면-전무로 획득한다 (G2.12).
 *
 * <p>순서대로 치면 앞엣것을 소비한 뒤 뒤엣것이 거부할 때 <b>통과하지 않은 요청이
 * 예산을 깎는다.</b> 그 유실은 조용해서 부하 시험 전까지 안 보인다.
 */
class AtomicAcquireTest {

    private static final String COUPON = "coupon:1";
    private static final String GLOBAL = "node";

    @Test
    @DisplayName("둘_다_여유가_있으면_함께_차감한다")
    void 둘_다_여유가_있으면_함께_차감한다() {
        SecondWindowLimiter limiter = new SecondWindowLimiter(1000);

        assertThat(limiter.tryAcquireAll(COUPON, 10, GLOBAL, 100, 10))
                .isEqualTo(AcquireResult.ACQUIRED);
        assertThat(limiter.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("한쪽이_부족하면_다른_쪽도_차감하지_않는다")
    void 한쪽이_부족하면_다른_쪽도_차감하지_않는다() {
        SecondWindowLimiter limiter = new SecondWindowLimiter(1000);

        // 쿠폰 예산을 다 쓴다. 전역은 여유가 많다.
        assertThat(limiter.tryAcquireAll(COUPON, 1, GLOBAL, 100, 10))
                .isEqualTo(AcquireResult.ACQUIRED);

        // 쿠폰이 부족하므로 거부. 이때 전역 카운터가 늘면 안 된다.
        assertThat(limiter.tryAcquireAll(COUPON, 1, GLOBAL, 100, 10))
                .isEqualTo(AcquireResult.COUPON_EXHAUSTED);

        // 전역 예산이 1 만 쓰인 상태여야 한다 — 거부된 요청이 깎지 않았다.
        for (int i = 0; i < 99; i++) {
            assertThat(limiter.tryAcquireAll("other:" + i, 10, GLOBAL, 100, 10))
                    .isEqualTo(AcquireResult.ACQUIRED);
        }
        assertThat(limiter.tryAcquireAll("last", 10, GLOBAL, 100, 10))
                .isEqualTo(AcquireResult.GLOBAL_EXHAUSTED);
    }

    @Test
    @DisplayName("부족한_쪽에_따라_판정값이_갈린다")
    void 부족한_쪽에_따라_판정값이_갈린다() {
        // 대응이 다르다. 쿠폰이면 그 쿠폰만 조이면 되고, 전역이면 노드를 늘려야 한다.
        SecondWindowLimiter a = new SecondWindowLimiter(1000);
        assertThat(a.tryAcquireAll(COUPON, 0, GLOBAL, 100, 10))
                .isEqualTo(AcquireResult.COUPON_EXHAUSTED);

        SecondWindowLimiter b = new SecondWindowLimiter(1000);
        assertThat(b.tryAcquireAll(COUPON, 10, GLOBAL, 0, 10))
                .isEqualTo(AcquireResult.GLOBAL_EXHAUSTED);
    }

    @Test
    @DisplayName("초가_바뀌면_두_예산이_함께_리셋된다")
    void 초가_바뀌면_두_예산이_함께_리셋된다() {
        SecondWindowLimiter limiter = new SecondWindowLimiter(1000);

        assertThat(limiter.tryAcquireAll(COUPON, 1, GLOBAL, 1, 10))
                .isEqualTo(AcquireResult.ACQUIRED);
        assertThat(limiter.tryAcquireAll(COUPON, 1, GLOBAL, 1, 10))
                .isEqualTo(AcquireResult.COUPON_EXHAUSTED);
        assertThat(limiter.tryAcquireAll(COUPON, 1, GLOBAL, 1, 11))
                .isEqualTo(AcquireResult.ACQUIRED);
    }

    @Test
    @DisplayName("쿠폰키와_전역키가_같아도_슬롯을_이중으로_세지_않는다")
    void 쿠폰키와_전역키가_같아도_슬롯을_이중으로_세지_않는다() {
        // 같은 키면 신규 슬롯은 하나다. 둘로 세면 자리가 있는데도 거부한다.
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(1);

        assertThat(limiter.tryAcquireAll("same", 10, "same", 10, 10))
                .isEqualTo(AcquireResult.ACQUIRED);
    }

    @Test
    @DisplayName("같은_키를_두_번_차감하지_않는다")
    void 같은_키를_두_번_차감하지_않는다() {
        // 요청 하나가 2 를 소비하면 상한 2 에서 한 건만 통과한다.
        // 반환값만 보면 안 드러난다 — 몇 건이 통과하는지로 잰다.
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10);

        int passed = 0;
        for (int i = 0; i < 5; i++) {
            if (limiter.tryAcquireAll("same", 2, "same", 2, 10) == AcquireResult.ACQUIRED) {
                passed++;
            }
        }

        assertThat(passed).isEqualTo(2);
    }

    @Test
    @DisplayName("같은_키면_두_상한_중_작은_쪽을_쓴다")
    void 같은_키면_두_상한_중_작은_쪽을_쓴다() {
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10);

        assertThat(limiter.tryAcquireAll("same", 5, "same", 1, 10))
                .isEqualTo(AcquireResult.ACQUIRED);
        assertThat(limiter.tryAcquireAll("same", 5, "same", 1, 10))
                .isEqualTo(AcquireResult.GLOBAL_EXHAUSTED);
    }

    @Test
    @DisplayName("맵이_가득_차면_새_키를_받지_않는다")
    void 맵이_가득_차면_새_키를_받지_않는다() {
        SecondWindowLimiter limiter = new SecondWindowLimiter(2);

        assertThat(limiter.tryAcquireAll(COUPON, 10, GLOBAL, 100, 10))
                .isEqualTo(AcquireResult.ACQUIRED);
        // 자리가 없다. 통과시키면 상한이 무의미해진다.
        assertThat(limiter.tryAcquireAll("coupon:2", 10, GLOBAL, 100, 10))
                .isEqualTo(AcquireResult.COUPON_EXHAUSTED);
    }
}

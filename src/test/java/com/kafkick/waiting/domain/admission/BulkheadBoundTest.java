package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 쿠폰이 몇 개가 오든 <b>격벽이 메모리를 안 밀어낸다</b> (G6.4).
 *
 * <p>쿠폰 식별자는 밖에서 오는 값이라 가짓수에 상한이 없다. 안 막으면 맵 하나가
 * 힙을 차지하고, 그때 죽는 것은 격벽이 아니라 노드다.
 */
class BulkheadBoundTest {

    /** 운영에서 상정한 규모. 이만큼은 <b>막히지 않아야</b> 한다. */
    private static final int EXPECTED_COUPONS = 2_000;

    @Test
    @DisplayName("상정한_2천개는_전부_들어간다")
    void 상정한_2천개는_전부_들어간다() {
        Bulkhead bulkhead = Bulkhead.withMaxKeys(CouponKeys.MAX);

        for (int i = 0; i < EXPECTED_COUPONS; i++) {
            assertThat(bulkhead.tryEnter("c" + i, 1))
                    .describedAs("%d 번째 쿠폰", i)
                    .isTrue();
        }

        assertThat(bulkhead.size()).isEqualTo(EXPECTED_COUPONS);
        assertThat(bulkhead.inFlight()).isEqualTo(EXPECTED_COUPONS);
    }

    /**
     * 상한을 넘겨도 맵이 안 커집니다.
     *
     * <p>여기서 막는 대신 받아 주면 상한이 있으나 마나입니다 — 밖에서 오는 값이라
     * 얼마든지 밀어 넣을 수 있습니다.
     */
    @Test
    @DisplayName("상한을_넘겨도_맵이_안_커진다")
    void 상한을_넘겨도_맵이_안_커진다() {
        int max = 64;
        Bulkhead bulkhead = Bulkhead.withMaxKeys(max);

        for (int i = 0; i < max * 100; i++) {
            bulkhead.tryEnter("c" + i, 1);
        }

        assertThat(bulkhead.size()).isEqualTo(max);
        assertThat(bulkhead.inFlight()).isEqualTo(max);
    }

    /**
     * 드나듦을 반복해도 안 샙니다.
     *
     * <p>상한만 보면 "한 번 차고 안 커진다" 까지만 압니다. 캠페인이 바뀌며 쿠폰이
     * 계속 갈리는 것이 실제 모양이고, 그때 안 지우면 **새 쿠폰이 못 들어갑니다.**
     */
    @Test
    @DisplayName("쿠폰이_계속_갈려도_안_샌다")
    void 쿠폰이_계속_갈려도_안_샌다() {
        Bulkhead bulkhead = Bulkhead.withMaxKeys(CouponKeys.MAX);

        for (int i = 0; i < CouponKeys.MAX * 5; i++) {
            String coupon = "c" + i;
            assertThat(bulkhead.tryEnter(coupon, 1))
                    .describedAs("%d 번째 쿠폰이 자리를 못 잡았다 — 앞의 것이 안 지워졌다", i)
                    .isTrue();
            bulkhead.exit(coupon);
        }

        assertThat(bulkhead.size()).isZero();
        assertThat(bulkhead.inFlight()).isZero();
    }

    /** 상한은 밖에서 받지 않고 자기가 안다. 지표가 분모로 이 값을 읽는다 (6.3.6). */
    @Test
    @DisplayName("자기_상한을_말한다")
    void 자기_상한을_말한다() {
        assertThat(Bulkhead.withMaxKeys(7).maxKeys()).isEqualTo(7);
    }

    /**
     * 리미터와 격벽이 <b>같은 상한</b>을 씁니다 (6.3.5).
     *
     * <p>둘이 갈리면 한쪽만 찼을 때 판정이 어긋납니다 — 리미터는 아직 받는데
     * 격벽이 안 받거나, 그 반대입니다.
     */
    @Test
    @DisplayName("리미터와_격벽이_같은_상한을_쓴다")
    void 리미터와_격벽이_같은_상한을_쓴다() {
        assertThat(SecondWindowLimiter.withMaxKeys(CouponKeys.MAX).maxKeys())
                .isEqualTo(Bulkhead.withMaxKeys(CouponKeys.MAX).maxKeys())
                .isEqualTo(CouponKeys.MAX);
    }
}

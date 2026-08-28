package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 쿠폰이 몇 개가 오든 <b>격벽의 키가 상한에서 멈춘다</b> (G6.4).
 *
 * <p>쿠폰 식별자는 밖에서 오는 값이라 가짓수에 상한이 없다. 안 막으면 맵 하나가
 * 힙을 차지하고, 그때 죽는 것은 격벽이 아니라 노드다.
 */
// **여기서 재는 것은 키 수다.** 쿠폰당 값이 정수 하나라 키 수가 곧 메모리이지만,
// 그것은 이 시험이 증명하는 것이 아니라 Bulkhead 의 자료구조가 정하는 것이다.
// 같은 상한을 쓰는 나머지 두 자리(리미터·래치)는 각자의 시험이 본다.

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
     * 상한을 넘기면 <b>거절합니다.</b> 밀어내고 받는 것이 아닙니다.
     *
     * <p>크기만 보면 축출하는 구현이 그대로 통과합니다 — 맵은 상한에서 멈추지만
     * 밀려난 쿠폰의 동시 카운터가 0 으로 되돌아가, 그 쿠폰이 상한을 두 배로 씁니다.
     * 격벽이 있으나 마나가 됩니다.
     */
    @Test
    @DisplayName("상한을_넘기면_밀어내지_않고_거절한다")
    void 상한을_넘기면_밀어내지_않고_거절한다() {
        int max = 64;
        Bulkhead bulkhead = Bulkhead.withMaxKeys(max);

        int rejected = 0;
        for (int i = 0; i < max * 100; i++) {
            if (!bulkhead.tryEnter("c" + i, 1)) {
                rejected++;
            }
        }

        assertThat(rejected)
                .describedAs("상한 뒤로 온 것은 전부 거절한다")
                .isEqualTo(max * 99);
        assertThat(bulkhead.size()).isEqualTo(max);
        assertThat(bulkhead.inFlight()).isEqualTo(max);

        // 먼저 들어온 쿠폰이 자리를 지키고 있다. 축출하는 구현이면 여기서 깨진다.
        bulkhead.exit("c0");
        assertThat(bulkhead.size()).isEqualTo(max - 1);
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

    /**
     * 맵이 꽉 차도 <b>이미 자리를 쥔 쿠폰은 계속 씁니다.</b>
     *
     * <p>여기서 같이 막으면 캠페인이 만 개 도는 중에 <b>진행 중인 쿠폰이 자기 자리를
     * 못 쓰게</b> 됩니다. 맵은 어차피 안 커지므로 막을 이유도 없습니다.
     *
     * <p>앞의 시험들은 쿠폰마다 한 번씩만 들어가서 이 갈래를 안 지납니다 — 상한을
     * 보는 조건에서 "새 쿠폰만" 을 지워도 전부 통과합니다.
     */
    @Test
    @DisplayName("맵이_꽉_차도_쥔_쿠폰은_계속_쓴다")
    void 맵이_꽉_차도_쥔_쿠폰은_계속_쓴다() {
        int max = 4;
        Bulkhead bulkhead = Bulkhead.withMaxKeys(max);
        for (int i = 0; i < max; i++) {
            bulkhead.tryEnter("c" + i, 3);
        }

        assertThat(bulkhead.tryEnter("새 쿠폰", 3))
                .describedAs("맵이 찼으면 새 쿠폰은 안 받는다")
                .isFalse();
        assertThat(bulkhead.tryEnter("c0", 3))
                .describedAs("이미 자리를 쥔 쿠폰은 자기 몫을 계속 쓴다")
                .isTrue();
        assertThat(bulkhead.size()).isEqualTo(max);
        assertThat(bulkhead.inFlight()).isEqualTo(max + 1);
    }

    /**
     * 상한이 1 이어도 <b>하나는 담습니다.</b>
     *
     * <p>경계를 한 칸 잘못 잡으면 여기서 아무도 못 들어가고, 그 상태는 상한이 큰
     * 시험으로는 안 드러납니다.
     */
    @Test
    @DisplayName("상한이_1이면_쿠폰_하나는_담는다")
    void 상한이_1이면_쿠폰_하나는_담는다() {
        Bulkhead bulkhead = Bulkhead.withMaxKeys(1);

        assertThat(bulkhead.tryEnter("c1", 1)).isTrue();
        assertThat(bulkhead.tryEnter("c2", 1)).isFalse();
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

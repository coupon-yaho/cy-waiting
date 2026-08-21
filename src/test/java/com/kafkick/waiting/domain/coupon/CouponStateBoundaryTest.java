package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 파생값의 경계.
 *
 * <p>0 은 예외가 아니라 실제로 오는 값이다. 한산한 쿠폰의 credit 이 0 이고(I1),
 * 운영자가 비율을 0 으로 내리면 그 쿠폰만 통과가 멎는다 — 둘 다 정상 동작이라
 * "0 이하는 거부" 로 뭉뚱그리면 안 된다.
 */
class CouponStateBoundaryTest {

    private static final SnapshotMeta META = new SnapshotMeta(1000, 10);

    @Test
    @DisplayName("한산_비율_0은_유효하고_상한도_0이_된다")
    void 한산_비율_0은_유효하고_상한도_0이_된다() {
        assertThat(CouponStates.idle(500).idleCap(META, 0.0)).isZero();
    }

    @Test
    @DisplayName("한산_비율이_음수면_거부한다")
    void 한산_비율이_음수면_거부한다() {
        assertThatThrownBy(() -> CouponStates.idle(500).idleCap(META, -0.1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("최대_대기_시간이_0이면_큐_상한도_0이다")
    void 최대_대기_시간이_0이면_큐_상한도_0이다() {
        assertThat(CouponStates.queueing(100, 500, 1000).queueCapacity(0)).isZero();
    }

    @Test
    @DisplayName("최대_대기_시간이_1이면_credit_만큼_받는다")
    void 최대_대기_시간이_1이면_credit_만큼_받는다() {
        assertThat(CouponStates.queueing(100, 500, 1000).queueCapacity(1)).isEqualTo(100);
    }
}

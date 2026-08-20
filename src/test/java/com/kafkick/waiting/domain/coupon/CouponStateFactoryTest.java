package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 팩토리는 도달 가능한 상태 하나씩만 만든다. */
class CouponStateFactoryTest {

    @Test
    @DisplayName("idle_팩토리는_credit_0과_waiting_0을_만든다")
    void idle_팩토리는_credit_0과_waiting_0을_만든다() {
        CouponState s = CouponState.idle(500);

        assertThat(s.runtime()).isEqualTo(RuntimeState.IDLE);
        assertThat(s.credit()).isZero();
        assertThat(s.waiting()).isZero();
        assertThat(s.remainingStock()).isEqualTo(500);
    }

    @Test
    @DisplayName("queueing_팩토리는_대기자가_있는_상태를_만든다")
    void queueing_팩토리는_대기자가_있는_상태를_만든다() {
        CouponState s = CouponState.queueing(100, 500, 3000);

        assertThat(s.runtime()).isEqualTo(RuntimeState.QUEUEING);
        assertThat(s.waiting()).isEqualTo(3000);
    }

    @Test
    @DisplayName("draining_팩토리는_몫이_대기자_이상인_상태를_만든다")
    void draining_팩토리는_몫이_대기자_이상인_상태를_만든다() {
        CouponState s = CouponState.draining(3000, 500, 3000);

        assertThat(s.runtime()).isEqualTo(RuntimeState.DRAINING);
        assertThat(s.credit()).isGreaterThanOrEqualTo(s.waiting());
    }

    @Test
    @DisplayName("closed_팩토리는_재고가_0인_상태를_만든다")
    void closed_팩토리는_재고가_0인_상태를_만든다() {
        CouponState s = CouponState.closed(3000);

        assertThat(s.runtime()).isEqualTo(RuntimeState.CLOSED);
        assertThat(s.remainingStock()).isZero();
    }

    @Test
    @DisplayName("off_팩토리는_대기열이_꺼진_상태를_만든다")
    void off_팩토리는_대기열이_꺼진_상태를_만든다() {
        CouponState s = CouponState.off(500);

        assertThat(s.mode()).isEqualTo(QueueMode.OFF);
        assertThat(s.runtime()).isEqualTo(RuntimeState.IDLE);
    }

    @Test
    @DisplayName("unknown_팩토리는_스냅샷에_없는_쿠폰을_나타낸다")
    void unknown_팩토리는_스냅샷에_없는_쿠폰을_나타낸다() {
        // 미지 쿠폰은 404 로 끊는다. 상태를 만들어 두는 것은 판정이
        // null 을 다루지 않게 하려는 것이지 통과시키려는 게 아니다.
        CouponState s = CouponState.unknown();

        assertThat(s.runtime()).isEqualTo(RuntimeState.CLOSED);
        assertThat(s.remainingStock()).isZero();
        assertThat(s.credit()).isZero();
    }
}

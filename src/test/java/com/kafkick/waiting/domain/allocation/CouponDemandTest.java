package com.kafkick.waiting.domain.allocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 배분 요구량.
 *
 * <p>여기가 {@code IDLE ⟹ credit == 0}(I1)의 출처다. 대기자가 없으면 요구량이
 * 0 이고, 요구량이 0 이면 배분을 못 받는다.
 */
class CouponDemandTest {

    @Test
    @DisplayName("재고가_천장으로_작동한다")
    void 재고가_천장으로_작동한다() {
        // 재고 3 개에 100 명을 통과시키면 97 명이 헛걸음한다 (C-2).
        assertThat(new CouponDemand("c1", 100, 3).want()).isEqualTo(3);
    }

    @Test
    @DisplayName("재고가_넉넉하면_대기자_수가_그대로_요구량이다")
    void 재고가_넉넉하면_대기자_수가_그대로_요구량이다() {
        assertThat(new CouponDemand("c1", 40, 1000).want()).isEqualTo(40);
    }

    @Test
    @DisplayName("대기자가_없으면_배분_대상이_아니다")
    void 대기자가_없으면_배분_대상이_아니다() {
        assertThat(new CouponDemand("c1", 0, 1000).isActive()).isFalse();
    }

    @Test
    @DisplayName("재고가_없으면_대기자가_있어도_배분_대상이_아니다")
    void 재고가_없으면_대기자가_있어도_배분_대상이_아니다() {
        // 배분해 봐야 뒷단이 전부 거절한다. 크레딧만 버린다.
        assertThat(new CouponDemand("c1", 500, 0).isActive()).isFalse();
    }

    @Test
    @DisplayName("대기자와_재고가_모두_있으면_배분_대상이다")
    void 대기자와_재고가_모두_있으면_배분_대상이다() {
        assertThat(new CouponDemand("c1", 500, 10).isActive()).isTrue();
    }

    @Test
    @DisplayName("음수_대기자나_음수_재고는_거부한다")
    void 음수_대기자나_음수_재고는_거부한다() {
        assertThatThrownBy(() -> new CouponDemand("c1", -1, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponDemand("c1", 10, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("쿠폰_식별자가_없으면_거부한다")
    void 쿠폰_식별자가_없으면_거부한다() {
        assertThatThrownBy(() -> new CouponDemand(null, 10, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponDemand("  ", 10, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

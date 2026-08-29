package com.kafkick.waiting.domain.allocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.coupon.QueueMode;
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

    /** 미상을 뜻하는 한 값 말고는 음수를 안 받는다. 열어 두면 오타가 값이 된다. */
    @Test
    @DisplayName("음수_대기자나_뜻_없는_음수_재고는_거부한다")
    void 음수_대기자나_뜻_없는_음수_재고는_거부한다() {
        assertThatThrownBy(() -> new CouponDemand("c1", -1, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponDemand("c1", 10, -2))
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

    @Test
    @DisplayName("모드가_없으면_안_만들어진다")
    void 모드가_없으면_안_만들어진다() {
        // 정책을 못 읽은 것과 안 건 것은 다르다. 없는 것을 값으로 받으면
        // 발행이 무엇을 실을지 모르게 된다.
        assertThatThrownBy(() -> new CouponDemand("c1", 1, 1, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("모드를_안_주면_적응형이다")
    void 모드를_안_주면_적응형이다() {
        assertThat(new CouponDemand("c1", 1, 1).mode()).isEqualTo(QueueMode.ADAPTIVE);
    }

    /**
     * <b>재고를 못 읽은 것과 다 팔린 것은 다르다.</b>
     *
     * <p>둘을 같은 값으로 접으면 재고 키를 잃은 쿠폰이 매진으로 보이고, 줄에
     * 사람이 남아 있어도 종결된다. 재고가 실제로 돌아오는 것이 아니라서
     * 다음 스냅샷도 이것을 안 되돌린다 — 자동으로 안 낫는 유일한 오판이다.
     */
    @Test
    @DisplayName("재고_미상은_매진이_아니다")
    void 재고_미상은_매진이_아니다() {
        CouponDemand 미상 = CouponDemand.stockUnknown("c1", 100, QueueMode.ADAPTIVE);

        assertThat(미상.stockKnown()).as("모른다는 것을 값으로 안다").isFalse();
        assertThat(CouponDemand.stockUnknown("c1", 100, QueueMode.ADAPTIVE).stock())
                .as("0 이 아니다 — 0 이면 매진과 같아진다")
                .isNotEqualTo(0L);
    }

    /**
     * <b>미상이면 재고로 몫을 깎지 않는다.</b> 깎으면 재고를 못 읽는 동안 그 줄이
     * 통째로 굶는다. 진짜 상한은 뒷단이 원자적으로 지킨다 (불변식 2).
     */
    @Test
    @DisplayName("미상이면_대기만큼_요구한다")
    void 미상이면_대기만큼_요구한다() {
        CouponDemand 미상 = CouponDemand.stockUnknown("c1", 100, QueueMode.ADAPTIVE);

        assertThat(미상.want()).as("대기 그대로").isEqualTo(100);
        assertThat(미상.isActive()).as("굶기지 않는다").isTrue();
    }

    /** 아는 재고는 그대로 상한이다. 미상을 들이면서 이것이 흔들리면 안 된다. */
    @Test
    @DisplayName("아는_재고는_여전히_상한이다")
    void 아는_재고는_여전히_상한이다() {
        assertThat(new CouponDemand("c1", 100, 3).want()).isEqualTo(3);
        assertThat(new CouponDemand("c1", 100, 0).isActive()).isFalse();
    }
}

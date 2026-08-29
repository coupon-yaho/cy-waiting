package com.kafkick.waiting.domain.allocation;

import com.kafkick.waiting.domain.coupon.QueueMode;

/**
 * 이 쿠폰이 이번 틱에 받고 싶은 양.
 *
 * <p><b>재고가 천장이다</b>(C-2). 재고 3 개에 100 명을 통과시키면 97 명이
 * 헛걸음하고, 그만큼의 크레딧은 다른 쿠폰이 못 쓴 채 버려진다.
 *
 * @param couponId 예산을 나누는 단위
 * @param waiting  줄 선 사람 수
 * @param stock    남은 재고
 * @param mode     운영자가 정한 대기열 정책. <b>배분은 안 쓰고 발행이 쓴다</b>
 */
public record CouponDemand(String couponId, long waiting, long stock, QueueMode mode) {

    /**
     * 재고를 못 읽었다.
     *
     * <p><b>0 과 갈라야 한다.</b> 접으면 재고 키를 잃은 쿠폰이 매진으로 보이고,
     * 줄에 사람이 남아 있어도 종결된다 — 다음 스냅샷도 안 되돌린다.
     */
    // 상태 쪽에도 같은 뜻의 값이 따로 있다. 경계를 넘는 것은 값이 아니라
    // stockKnown() 이라, 둘이 같은 수일 필요는 없다.
    public static final long STOCK_UNKNOWN = -1;

    public CouponDemand {
        if (couponId == null || couponId.isBlank()) {
            throw new IllegalArgumentException("couponId 는 필수다");
        }
        if (mode == null) {
            throw new IllegalArgumentException("mode 는 필수다");
        }
        if (waiting < 0) {
            throw new IllegalArgumentException("waiting 은 0 이상이어야 한다: " + waiting);
        }
        if (stock < 0 && stock != STOCK_UNKNOWN) {
            throw new IllegalArgumentException(
                    "stock 은 0 이상이어야 한다. 못 읽었으면 stockUnknown 을 쓴다: " + stock);
        }
    }

    /** 재고 키가 안 온 쿠폰. 매진이 아니라 <b>모르는 것</b>이다. */
    public static CouponDemand stockUnknown(String couponId, long waiting, QueueMode mode) {
        return new CouponDemand(couponId, waiting, STOCK_UNKNOWN, mode);
    }

    /** 재고를 아는가. 모르면 매진 판정에 쓰면 안 된다. */
    public boolean stockKnown() {
        return stock != STOCK_UNKNOWN;
    }

    /** 정책을 안 적은 쿠폰. <b>정책이 없다는 것이 곧 적응형이다</b> — 기본값이다. */
    public CouponDemand(String couponId, long waiting, long stock) {
        this(couponId, waiting, stock, QueueMode.ADAPTIVE);
    }

    /**
     * 재고를 넘겨 주면 그 몫은 뒷단이 거절하고, 다른 쿠폰이 못 쓴 채 사라진다.
     *
     * <p><b>미상이면 안 깎는다.</b> 깎으면 재고를 못 읽는 동안 그 줄이 통째로
     * 굶는다. 진짜 상한은 뒷단이 원자적으로 지키므로(불변식 2) 여기서 모르는
     * 값을 0 으로 가정할 이유가 없다.
     */
    public long want() {
        return stockKnown() ? Math.min(waiting, stock) : waiting;
    }

    /** 여기가 {@code IDLE ⟹ credit == 0}(I1)의 출처다 — 요구량이 0 이면 못 받는다. */
    public boolean isActive() {
        return want() > 0;
    }
}

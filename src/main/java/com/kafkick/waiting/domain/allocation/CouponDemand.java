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
        if (stock < 0) {
            throw new IllegalArgumentException("stock 은 0 이상이어야 한다: " + stock);
        }
    }

    /** 정책을 안 적은 쿠폰. <b>정책이 없다는 것이 곧 적응형이다</b> — 기본값이다. */
    public CouponDemand(String couponId, long waiting, long stock) {
        this(couponId, waiting, stock, QueueMode.ADAPTIVE);
    }

    /** 재고를 넘겨 주면 그 몫은 뒷단이 거절하고, 다른 쿠폰이 못 쓴 채 사라진다. */
    public long want() {
        return Math.min(waiting, stock);
    }

    /** 여기가 {@code IDLE ⟹ credit == 0}(I1)의 출처다 — 요구량이 0 이면 못 받는다. */
    public boolean isActive() {
        return want() > 0;
    }
}

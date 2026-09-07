package com.kafkick.waiting.domain.allocation;

import com.kafkick.waiting.domain.coupon.QueueMode;

/**
 * 이 쿠폰이 이번 틱에 받고 싶은 양. <b>재고가 천장이다</b> — 재고 3 개에
 * 100 명을 통과시키면 97 명이 헛걸음하고, 그 크레딧은 다른 쿠폰이 못 쓴 채 버려진다.
 *
 * @param couponId 예산을 나누는 단위
 * @param waiting  줄 선 사람 수
 * @param stock    남은 재고
 * @param mode     운영자가 정한 대기열 정책. <b>배분은 안 쓰고 발행이 쓴다</b>
 */
public record CouponDemand(String couponId, long waiting, long stock, QueueMode mode) {

    /**
     * 재고를 못 읽었다. <b>0 과 갈라야 한다</b> — 접으면 재고 키를 잃은 쿠폰이 매진으로
     * 보이고, 줄에 사람이 남아 있어도 종결된다. 경계를 넘는 것은 값이 아니라
     * {@code stockKnown()} 이라, 상태 쪽의 같은 뜻 값과 수가 달라도 된다.
     */
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
     * <p><b>미상이면 안 깎는다.</b> 깎으면 그 줄이 굶는다. 초과 발급을 막는 진짜
     * 상한은 재고를 쥔 뒷단이 지킨다.
     */
    public long want() {
        return stockKnown() ? Math.min(waiting, stock) : waiting;
    }

    /** 유휴 쿠폰의 크레딧이 0 인 까닭이 여기다 — 요구량이 0 이면 몫을 안 받는다. */
    public boolean isActive() {
        return want() > 0;
    }
}

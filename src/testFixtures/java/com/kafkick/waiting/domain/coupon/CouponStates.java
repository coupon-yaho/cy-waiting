package com.kafkick.waiting.domain.coupon;

/**
 * 테스트용 쿠폰 상태 픽스처.
 *
 * <p><b>자유형 생성 메서드를 두지 않는다.</b> 이전 구현이 무너진 이유가 픽스처로
 * {@code (IDLE, credit=1000)} 을 찍어낼 수 있었던 것이고, 그 상태에서는 버그가
 * 드러나지 않았다. 여기 있는 것은 전부 도달 가능한 상태다.
 */
public final class CouponStates {

    /** 한산한 쿠폰. R1 의 주인공 — 줄 없이 통과해야 한다. */
    public static CouponState idle(long remainingStock) {
        return CouponState.idle(remainingStock);
    }

    /** 줄이 선 쿠폰. */
    public static CouponState queueing(long credit, long remainingStock, long waiting) {
        return CouponState.queueing(credit, remainingStock, waiting);
    }

    /** 배수 중인 쿠폰. 배분이 대기자를 따라잡아 이번 틱에 다 뺄 수 있는 상태다. */
    public static CouponState draining(long credit, long remainingStock, long waiting) {
        return CouponState.draining(credit, remainingStock, waiting);
    }

    /** 매진된 쿠폰. */
    public static CouponState closed(long waiting) {
        return CouponState.closed(waiting);
    }

    /** 운영자가 무조건 줄을 세우기로 했다. 한산해도 대기열을 태운다. */
    public static CouponState always(long remainingStock) {
        return CouponState.always(remainingStock);
    }

    /** 대기열이 꺼진 쿠폰. */
    public static CouponState off(long remainingStock) {
        return CouponState.off(remainingStock);
    }

    /** 스냅샷에 없는 쿠폰. */
    public static CouponState unknown() {
        return CouponState.unknown();
    }

    private CouponStates() {
    }
}

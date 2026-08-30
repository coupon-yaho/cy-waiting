package com.kafkick.waiting.domain.coupon;

/**
 * 테스트용 쿠폰 상태 픽스처.
 *
 * <p><b>자유형 생성 메서드를 두지 않는다.</b> 이전 구현이 무너진 이유가 픽스처로
 * {@code (IDLE, credit=1000)} 을 찍어낼 수 있었던 것이고, 그 상태에서는 버그가
 * 드러나지 않았다. 여기 있는 것은 전부 도달 가능한 상태다.
 */
public final class CouponStates {

    /**
     * 한산한 쿠폰. R1 의 주인공 — 줄 없이 통과해야 한다.
     *
     * <p><b>재고 0 짜리도 발행 경로가 만든다.</b> {@code AllocationRound.stateOf}
     * 는 {@code stock<=0 && waiting==0} 에서 {@code CLOSED} 가 아니라 이것을
     * 낸다. 세 가지가 같은 자리로 접힌다 — 시작 전, 완판 뒤 큐 정리가 끝난 뒤,
     * 그리고 재고 키 유실(CY-702). 가운데가 매진 쿠폰의 정상 종착점이다.
     */
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

    /**
     * 재고 키를 못 읽은 쿠폰 (CY-702). <b>매진도 아니고 재고를 아는 것도 아니다.</b>
     *
     * <p>발행 경로가 실제로 내는 상태다 — {@code AllocationRound.stateOf} 가
     * 재고 미상인 수요를 이것으로 만든다.
     */
    public static CouponState stockUnknown(long credit, long waiting) {
        return CouponState.stockUnknown(QueueMode.ADAPTIVE, credit, waiting);
    }


    /** 운영자가 껐는데 줄이 아직 남아 있다 — 붐비는 쿠폰을 끄면 생긴다. */
    public static CouponState offWithQueue(long credit, long remainingStock, long waiting) {
        return CouponState.offWithQueue(credit, remainingStock, waiting);
    }

    /**
     * 운영자가 항상 대기를 걸었고 줄이 이미 섰다.
     *
     * <p>발행 경로가 실제로 만드는 상태다 — {@code AllocationRound.stateOf} 가
     * {@code demand.mode()} 를 그대로 싣고 그 값이 {@code ALWAYS} 일 수 있다.
     */
    public static CouponState alwaysWithQueue(long credit, long remainingStock, long waiting) {
        return CouponState.withQueue(QueueMode.ALWAYS, credit, remainingStock, waiting);
    }

    private CouponStates() {
    }
}

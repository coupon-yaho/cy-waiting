package com.kafkick.waiting.domain.coupon;

/**
 * 운영자가 정한 대기열 정책. 사람이 고르는 값이다.
 *
 * <p>지금 붐비는지({@link RuntimeState})와 무관하게 적용된다.
 */
public enum QueueMode {

    /**
     * 대기열을 쓰지 않는다.
     *
     * <p><b>이미 줄이 있으면 그 줄이 빠질 때까지는 예외다.</b> 우회시키면
     * 신규 유입이 줄 선 사람을 통째로 추월한다 (불변식 4).
     */
    OFF,

    /** 몰릴 때만 줄을 세운다. 상한을 넘은 초과분만 큐로 간다. */
    ADAPTIVE,

    /** 한산해도 무조건 줄을 세운다. */
    ALWAYS
}

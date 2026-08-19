package com.kafkick.waiting.domain.coupon;

/**
 * 운영자가 정한 대기열 정책. 사람이 고르는 값이다.
 *
 * <p>지금 붐비는지({@link RuntimeState})와 무관하게 적용된다.
 */
public enum QueueMode {

    /** 대기열을 쓰지 않는다. 붐벼도 줄을 세우지 않는다. */
    OFF,

    /** 몰릴 때만 줄을 세운다. 상한을 넘은 초과분만 큐로 간다. */
    ADAPTIVE,

    /** 한산해도 무조건 줄을 세운다. */
    ALWAYS
}

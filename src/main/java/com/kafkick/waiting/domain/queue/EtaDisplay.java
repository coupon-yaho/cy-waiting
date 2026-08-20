package com.kafkick.waiting.domain.queue;

/**
 * 사용자에게 보여 줄 대기 시간 구간.
 *
 * <p>초 단위로 보여 주면 1초씩 줄다 멈추는 것이 보이고, 그때마다 신뢰를 잃는다.
 */
public enum EtaDisplay {

    /** 아직 배수율을 모른다. "10분 이상" 과 뭉치면 떠날지 판단할 수 없다. */
    CALCULATING,

    /** 30초 미만. */
    ALMOST_THERE,

    /** 30초 이상 90초 미만. */
    ABOUT_A_MINUTE,

    /** 90초 이상 450초 미만. */
    ABOUT_FIVE_MINUTES,

    /** 450초 이상. */
    OVER_TEN_MINUTES
}

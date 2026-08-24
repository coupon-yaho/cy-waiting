package com.kafkick.waiting.domain.queue;

/**
 * 사용자에게 보여 줄 대기 시간 구간.
 *
 * <p>초 단위로 보여 주면 1초씩 줄다 멈추는 것이 보이고, 그때마다 신뢰를 잃는다.
 *
 * <p><b>"모름" 이 없다.</b> 배수율을 몰라도 값을 준다 — 계산 중이라는 표시는
 * 떠날지 기다릴지 판단할 근거를 안 준다. 모르면 가장 넓은 구간으로 접는다.
 */
public enum EtaDisplay {

    /** 30초 미만. */
    ALMOST_THERE,

    /** 30초 이상 90초 미만. */
    ABOUT_A_MINUTE,

    /** 90초 이상 450초 미만. */
    ABOUT_FIVE_MINUTES,

    /** 450초 이상. */
    OVER_TEN_MINUTES
}

package com.kafkick.waiting.control;

/**
 * 이 노드가 뒷단으로 넘긴 초당 수를 아는 것. 하트비트가 이 값을 실어 리더가
 * 합산한다. 회복 봉우리가 정상의 1.2배를 넘는지 볼 재료이고, 그 수는 노드마다 제 것만 안다.
 */
public interface PassRateSource {

    /** <b>시계를 안 받는다.</b> 세는 쪽이 제 시계로 재야 창이 안 어긋난다. */
    long passRatePerSec();
}

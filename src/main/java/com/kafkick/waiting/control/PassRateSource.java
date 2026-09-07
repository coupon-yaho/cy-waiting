package com.kafkick.waiting.control;

/**
 * 이 노드가 뒷단으로 보낸 초당 수를 아는 것.
 *
 * <p>하트비트가 이 값을 실어 리더가 합산한다. 회복 봉우리를 정상과 견줄
 * 재료이고, 그 수는 노드마다 제 것만 안다 (RC4).
 */
public interface PassRateSource {

    /** <b>시계를 안 받는다.</b> 세는 쪽이 제 시계로 재야 창이 안 어긋난다. */
    long passRatePerSec();
}

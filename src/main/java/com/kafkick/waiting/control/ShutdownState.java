package com.kafkick.waiting.control;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 종료 신호를 받았나. <b>두 판정이 함께 본다.</b>
 *
 * <p>받는 쪽만 알면 부하 분산기는 뺐는데 살아 있음 판정이 루프 정지를 보고
 * 파드를 죽인다 — 진행 중인 요청을 든 채로 끊긴다. 한쪽에 갇힌 플래그로는
 * 그 요구를 표현할 수 없다.
 */
public final class ShutdownState {

    private static final Logger log = LoggerFactory.getLogger(ShutdownState.class);

    private final AtomicBoolean draining = new AtomicBoolean();

    public static ShutdownState create() {
        return new ShutdownState();
    }

    /** 되돌릴 수 없는 모드 전환이라 한 번 남긴다. */
    public void draining() {
        if (draining.compareAndSet(false, true)) {
            log.info("드레이닝 시작 — 받는 것을 끊고 진행 중인 요청만 마친다");
        }
    }

    public boolean isDraining() {
        return draining.get();
    }
}

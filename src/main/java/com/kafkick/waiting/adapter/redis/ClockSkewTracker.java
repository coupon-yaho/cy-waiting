package com.kafkick.waiting.adapter.redis;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 시계가 뒤로 간 사실을 남긴다.
 *
 * <p>바닥값이 조용히 보정하면 순서는 맞지만 <b>"왜 다 같은 순번인가" 를 영영
 * 못 밝힌다.</b> 보정한 횟수와 폭을 들고 있다가 지표로 내보낸다.
 *
 * <p>여러 요청이 동시에 들어오므로 원자 갱신이 필요하다 — 세는 값이 틀리면
 * 있었는지 없었는지도 못 믿는다.
 */
public class ClockSkewTracker {

    private final AtomicLong appliedCount = new AtomicLong();
    private final AtomicLong maxSkewMicros = new AtomicLong();

    /**
     * 등록 결과를 기록한다.
     *
     * @param floorApplied 바닥값이 적용됐는가
     * @param skewMicros   바닥값이 실제 시각을 앞선 폭. 음수면 정상이다
     */
    public void record(boolean floorApplied, long skewMicros) {
        if (!floorApplied) {
            return;
        }
        appliedCount.incrementAndGet();
        maxSkewMicros.accumulateAndGet(Math.max(0, skewMicros), Math::max);
    }

    /** 바닥값이 적용된 횟수. 0 이 아니면 시계가 뒤로 간 적이 있다는 뜻이다. */
    public long appliedCount() {
        return appliedCount.get();
    }

    /**
     * 관측된 최대 역행 폭(마이크로초).
     *
     * <p>이 값이 크면 `slew` 가 아니라 `step` 보정이 걸렸다는 신호다 (2.4절).
     */
    public long maxSkewMicros() {
        return maxSkewMicros.get();
    }
}

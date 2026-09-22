package com.kafkick.waiting.control;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

/**
 * 회차 한 번에 걸린 시간을 타이머에 싣는다. 스케줄러가 재서 넘기는 값이다.
 *
 * <p>분위수를 낸다 — 틱 게이트는 p99 를 보고, 평균만 나오면 꼬리를 못 본다.
 */
public final class TickLatency {

    static final String METRIC = "waiting.allocation.tick";

    private TickLatency() {
    }

    public static LongConsumer recorder(MeterRegistry meters) {
        Timer timer = Timer.builder(METRIC)
                .description("배분 회차 한 번이 시작부터 끝까지 걸린 시간. 리더에서만 돈다")
                .publishPercentiles(0.5, 0.95, 0.99)
                .distributionStatisticExpiry(Duration.ofMinutes(10))
                .distributionStatisticBufferLength(1)
                .register(meters);
        // **음수는 버린다.** 단조 시계가 아니면 뒤로 갈 수 있고, 그 값이 분위수를 망친다.
        return nanos -> {
            if (nanos >= 0) {
                timer.record(nanos, TimeUnit.NANOSECONDS);
            }
        };
    }
}

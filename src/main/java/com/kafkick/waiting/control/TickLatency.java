package com.kafkick.waiting.control;

import com.kafkick.waiting.control.AllocationScheduler.Outcome;
import com.kafkick.waiting.control.AllocationScheduler.RoundObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 회차 한 번에 걸린 시간을 <b>결과별로</b> 싣는다. 틱 게이트는 성공한 회차만으로 잰다.
 *
 * <p>섞으면 시한에 걸린 회차가 시한 값으로 잘려 적히고, 빨리 실패한 회차가 꼬리를 끌어내린다.
 */
public final class TickLatency {

    static final String METRIC = "waiting.allocation.tick";

    private TickLatency() {
    }

    public static RoundObserver recorder(MeterRegistry meters) {
        // 셋을 다 미리 단다. 한 번도 안 난 결과가 스크레이프에 없으면 "0 건" 과 "안 쟀다" 가 갈린다.
        Map<Outcome, Timer> timers = new EnumMap<>(Outcome.class);
        for (Outcome outcome : Outcome.values()) {
            timers.put(outcome, Timer.builder(METRIC)
                    .description("배분 회차 한 번이 시작부터 끝까지 걸린 시간. 리더에서만 돈다")
                    .tag("outcome", outcome.name().toLowerCase(Locale.ROOT))
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .distributionStatisticExpiry(Duration.ofMinutes(10))
                    .distributionStatisticBufferLength(1)
                    .register(meters));
        }
        // **음수는 버린다.** 단조 시계가 아니면 뒤로 갈 수 있고, 그 값이 분위수를 망친다.
        return (nanos, outcome) -> {
            if (nanos >= 0) {
                timers.get(outcome).record(nanos, TimeUnit.NANOSECONDS);
            }
        };
    }
}

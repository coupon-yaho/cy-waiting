package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.AllocationScheduler.Outcome;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 회차 시간을 <b>결과별로</b> 싣는가.
 *
 * <p>한 통에 섞으면 시한에 걸린 회차는 시한 값으로 잘려 적히고, 빨리 실패한 회차는 꼬리를 끌어내려
 * 레디스가 끊긴 동안 틱이 오히려 건강해 보인다.
 */
class TickLatencyTest {

    private static double 개수(SimpleMeterRegistry 계측, String 결과) {
        return 계측.get("waiting.allocation.tick").tag("outcome", 결과).timer().count();
    }

    @Test
    @DisplayName("결과별로 나눠 적는다")
    void 결과별로_적는다() {
        SimpleMeterRegistry 계측 = new SimpleMeterRegistry();
        AllocationScheduler.RoundObserver 관찰 = TickLatency.recorder(계측);

        관찰.observe(TimeUnit.MILLISECONDS.toNanos(42), Outcome.OK);
        관찰.observe(TimeUnit.MILLISECONDS.toNanos(1_000), Outcome.TIMEOUT);
        관찰.observe(TimeUnit.MILLISECONDS.toNanos(3), Outcome.ERROR);

        assertThat(개수(계측, "ok")).isEqualTo(1);
        assertThat(개수(계측, "timeout"))
                .as("시한에 걸린 회차가 성공과 섞이면 게이트가 시한 값을 잰다").isEqualTo(1);
        assertThat(개수(계측, "error"))
                .as("빨리 실패한 회차가 섞이면 장애 중에 꼬리가 내려간다").isEqualTo(1);
        assertThat(계측.get("waiting.allocation.tick").tag("outcome", "ok").timer()
                .totalTime(TimeUnit.MILLISECONDS)).isEqualTo(42.0);
    }

    @Test
    @DisplayName("음수는 안 적는다 — 시계가 뒤로 가면 나온다")
    void 음수는_버린다() {
        SimpleMeterRegistry 계측 = new SimpleMeterRegistry();

        TickLatency.recorder(계측).observe(-1, Outcome.OK);

        assertThat(개수(계측, "ok")).as("음수를 적으면 분위수가 망가진다").isZero();
    }
}

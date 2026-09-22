package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 회차 시간을 타이머에 싣는가. */
class TickLatencyTest {

    @Test
    @DisplayName("넘겨받은 나노초를 그대로 적는다")
    void 나노초를_적는다() {
        SimpleMeterRegistry 계측 = new SimpleMeterRegistry();

        TickLatency.recorder(계측).accept(TimeUnit.MILLISECONDS.toNanos(42));

        Timer 틱 = 계측.get("waiting.allocation.tick").timer();
        assertThat(틱.count()).isEqualTo(1);
        assertThat(틱.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(42.0);
    }

    @Test
    @DisplayName("음수는 안 적는다 — 시계가 뒤로 가면 나온다")
    void 음수는_버린다() {
        SimpleMeterRegistry 계측 = new SimpleMeterRegistry();

        TickLatency.recorder(계측).accept(-1);

        assertThat(계측.get("waiting.allocation.tick").timer().count())
                .as("음수를 적으면 분위수가 망가진다").isZero();
    }
}

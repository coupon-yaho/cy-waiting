package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 재연결 지연 계산의 경계. <b>재연결 예약 안에서 던지면 해제 조건이 없다</b> — 영영 다시 안 붙는다. */
class CappedJitterDelayTest {

    /** 바탕이 크면 두 배로 늘리다 넘친다. 넘친 음수가 난수 범위로 들어가면 던진다. */
    @Test
    @DisplayName("바탕이_커도_오래_실패하면_넘치지_않고_상한_안이다")
    void 바탕이_커도_오래_실패하면_넘치지_않고_상한_안이다() {
        Duration 상한 = Duration.ofSeconds(10);
        CappedJitterDelay 지연 = CappedJitterDelay.of(Duration.ofSeconds(9), 상한);

        for (long 시도 = 1; 시도 <= 64; 시도++) {
            assertThat(지연.createDelay(시도)).as("%d 번째 시도", 시도)
                    .isBetween(Duration.ofMillis(4_500), 상한);
        }
    }

    @Test
    @DisplayName("시도_번호가_0_이하여도_바탕_범위다")
    void 시도_번호가_0_이하여도_바탕_범위다() {
        CappedJitterDelay 지연 = CappedJitterDelay.of(Duration.ofMillis(100), Duration.ofSeconds(1));

        assertThat(지연.createDelay(0)).isBetween(Duration.ofMillis(50), Duration.ofMillis(100));
        assertThat(지연.createDelay(-3)).isBetween(Duration.ofMillis(50), Duration.ofMillis(100));
        assertThat(지연.createDelay(Long.MIN_VALUE)).as("빼기가 넘쳐 상한으로 가면 안 된다")
                .isBetween(Duration.ofMillis(50), Duration.ofMillis(100));
    }
}

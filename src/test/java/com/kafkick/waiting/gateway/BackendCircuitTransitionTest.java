package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.MutableClock;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 5xx 로 연 서킷의 전이를 <b>시계를 쥐고</b> 잰다 (CY-926).
 *
 * <p>C9b 시나리오는 실시간 대기에 기대 전이 시점을 못 박지 못한다. 여기서는 같은 설정에
 * 시계만 바꿔 끼우고 자동 전환을 꺼, 시각을 옮긴 만큼만 상태가 바뀌는지 본다.
 */
class BackendCircuitTransitionTest {

    /** C9b 와 같은 값. 표본 하한 3, 창 2초, 열린 대기 1초, half-open 허가 2. */
    private static final BackendCircuitProperties 설정 = new BackendCircuitProperties(
            Duration.ofSeconds(2), 3, 50f, Duration.ofMillis(1500), 50f,
            Duration.ofSeconds(1), Duration.ofSeconds(30), 2);

    private static final Duration 열린_대기 = Duration.ofSeconds(1);

    private final MutableClock 시계 = MutableClock.at(Instant.parse("2026-09-14T00:00:00Z"));

    private final CircuitBreaker 서킷 = CircuitBreaker.of("backend", CircuitBreakerConfig
            .from(BackendCircuit.registry(설정).getDefaultConfig())
            .clock(시계)
            // 자동 전환은 실시간 스케줄러로 돈다. 끄면 허가를 청하는 순간 시계로 판단한다.
            .automaticTransitionFromOpenToHalfOpenEnabled(false)
            .build());

    private void 오백을_받는다(int 건수) {
        for (int i = 0; i < 건수; i++) {
            서킷.onError(0, TimeUnit.NANOSECONDS, new IllegalStateException("500"));
        }
    }

    private void 성공한다(int 건수) {
        for (int i = 0; i < 건수; i++) {
            서킷.onSuccess(0, TimeUnit.NANOSECONDS);
        }
    }

    private void 열어_둔다() {
        오백을_받는다(3);
        assertThat(서킷.getState()).as("표본 하한을 5xx 로 채우면 열린다")
                .isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    @DisplayName("표본_하한을_못_채우면_안_열린다")
    void 표본_하한을_못_채우면_안_열린다() {
        오백을_받는다(2);

        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    /** <b>대기를 다 채울 때까지 막는다.</b> 라이브러리는 대기가 끝난 시각을 넘어야 푼다. */
    @Test
    @DisplayName("열린_대기가_지나야_반쯤_열린다")
    void 열린_대기가_지나야_반쯤_열린다() {
        열어_둔다();

        시계.앞으로(열린_대기);
        assertThat(서킷.tryAcquirePermission()).as("대기가 막 끝난 순간").isFalse();
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        시계.앞으로(Duration.ofMillis(1));
        assertThat(서킷.tryAcquirePermission()).isTrue();
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
    }

    /** 허가 수만큼 성공하면 닫힌다. 그 전에는 반쯤 열린 채다. */
    @Test
    @DisplayName("반쯤_열린_시도가_성공하면_닫힌다")
    void 반쯤_열린_시도가_성공하면_닫힌다() {
        열어_둔다();
        시계.앞으로(열린_대기.plusMillis(1));
        assertThat(서킷.tryAcquirePermission()).isTrue();

        성공한다(1);
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
        성공한다(1);
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    /** <b>뒷단이 아직 5xx 면 다시 연다.</b> 닫히면 전량이 아픈 뒷단에 꽂힌다. */
    @Test
    @DisplayName("반쯤_열린_시도가_5xx_면_다시_열린다")
    void 반쯤_열린_시도가_5xx_면_다시_열린다() {
        열어_둔다();
        시계.앞으로(열린_대기.plusMillis(1));
        assertThat(서킷.tryAcquirePermission()).isTrue();

        오백을_받는다(2);

        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(서킷.tryAcquirePermission()).as("다시 연 뒤 대기를 새로 잡는다").isFalse();
    }
}

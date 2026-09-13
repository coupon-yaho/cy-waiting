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
 * 5xx 로 연 서킷의 전이를 <b>시계를 쥐고</b> 잰다 (CY-926). C9b 시나리오는 실시간 대기라 시점을
 * 못 박는다. 자동 전환을 꺼 호출이 올 때 시계로 판단하는 경로만 본다 — 자동 전환과 half-open
 * 상한은 실시간 스케줄러로 돌아 시나리오에 남긴다.
 */
class BackendCircuitTransitionTest {

    /** 두 문턱을 다르게 둔다. 같으면 서로 바꿔 묶어도 안 드러난다. */
    private static final BackendCircuitProperties 설정 = new BackendCircuitProperties(
            Duration.ofSeconds(2), 3, 50f, Duration.ofMillis(1500), 80f,
            Duration.ofSeconds(1), Duration.ofSeconds(30), 2);

    private static final Duration 열린_대기 = 설정.waitDurationInOpenState();

    private final MutableClock 시계 = MutableClock.at(Instant.parse("2026-09-14T00:00:00Z"));

    private final CircuitBreaker 서킷 = CircuitBreaker.of("backend", CircuitBreakerConfig
            .from(BackendCircuit.registry(설정).getDefaultConfig())
            .clock(시계)
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
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    /** 대기가 끝난 시각을 넘겨 첫 허가를 받는다. 그 허가가 half-open 의 첫 자리다. */
    private void 반쯤_연다() {
        열어_둔다();
        시계.앞으로(열린_대기.plusMillis(1));
        assertThat(서킷.tryAcquirePermission()).isTrue();
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
    }

    @Test
    @DisplayName("표본_하한을_못_채우면_안_열린다")
    void 표본_하한을_못_채우면_안_열린다() {
        오백을_받는다(2);

        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    /** 문턱은 실패율이다. 절반이 5xx 면 열리고, 그 아래면 안 열린다. */
    @Test
    @DisplayName("실패율이_문턱에_닿아야_열린다")
    void 실패율이_문턱에_닿아야_열린다() {
        성공한다(2);
        오백을_받는다(1);
        assertThat(서킷.getState()).as("3건 중 1건").isEqualTo(CircuitBreaker.State.CLOSED);

        오백을_받는다(1);
        assertThat(서킷.getState()).as("4건 중 2건").isEqualTo(CircuitBreaker.State.OPEN);
    }

    /** 창이 시간 단위다. 창을 벗어난 5xx 는 표본에서 빠진다. */
    @Test
    @DisplayName("창을_벗어난_5xx_는_안_센다")
    void 창을_벗어난_5xx_는_안_센다() {
        오백을_받는다(2);
        시계.앞으로(설정.slidingWindowSize());

        오백을_받는다(1);

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

    /** <b>허가 수만큼만 보낸다.</b> 더 주면 약한 뒷단에 전량이 꽂힌다. 그만큼 성공하면 닫힌다. */
    @Test
    @DisplayName("반쯤_열린_시도가_성공하면_닫힌다")
    void 반쯤_열린_시도가_성공하면_닫힌다() {
        반쯤_연다();
        assertThat(서킷.tryAcquirePermission()).isTrue();
        assertThat(서킷.tryAcquirePermission()).as("허가 2건을 넘는 시도").isFalse();

        성공한다(1);
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
        성공한다(1);
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    /** <b>뒷단이 아직 5xx 면 다시 열고 대기를 새로 잡는다.</b> 닫히면 아픈 뒷단에 전량이 간다. */
    @Test
    @DisplayName("반쯤_열린_시도가_문턱에_닿으면_다시_열린다")
    void 반쯤_열린_시도가_문턱에_닿으면_다시_열린다() {
        반쯤_연다();
        assertThat(서킷.tryAcquirePermission()).isTrue();

        성공한다(1);
        오백을_받는다(1);
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        시계.앞으로(열린_대기);
        assertThat(서킷.tryAcquirePermission()).isFalse();
        시계.앞으로(Duration.ofMillis(1));
        assertThat(서킷.tryAcquirePermission()).isTrue();
    }
}

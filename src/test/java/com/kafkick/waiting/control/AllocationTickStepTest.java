package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 배분 한 틱의 조립 (CY-927).
 *
 * <p>틱은 가용량과 운영값을 읽고 회차를 돈다. <b>읽기를 회차 앞에 차례로 두면</b> 레디스가 느린 날 각자 시한까지
 * 기다려 틱을 먹고, 회차가 틱 시한 안에 못 끝나 발행이 잘린다. 지연 200ms 에서 8초에 발행 넷이었다.
 */
class AllocationTickStepTest {

    private static final Duration 읽기 = Duration.ofMillis(250);

    private final ControlPlaneConfig 배선 = new ControlPlaneConfig();

    @Test
    @DisplayName("두_읽기를_동시에_돌려_회차에_넘기고_회차는_기다리지_않고_시작한다")
    void 두_읽기를_동시에_돌려_회차에_넘기고_회차는_기다리지_않고_시작한다() {
        VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
        AtomicLong 회차_시작 = new AtomicLong(-1);
        AtomicLong 읽기_끝 = new AtomicLong(-1);
        Mono<Void> 한_틱 = 배선.allocationTickStep(
                () -> Mono.delay(읽기, 시계).then(),
                () -> Mono.delay(읽기, 시계).then(),
                읽기들 -> Mono.fromRunnable(() -> 회차_시작.set(시계.now(TimeUnit.MILLISECONDS)))
                        .then(읽기들)
                        .then(Mono.fromRunnable(() -> 읽기_끝.set(시계.now(TimeUnit.MILLISECONDS)))))
                .get();

        한_틱.subscribe();
        시계.advanceTime();
        assertThat(회차_시작.get()).as("회차는 읽기를 안 기다리고 시작한다").isZero();

        시계.advanceTimeBy(읽기);
        assertThat(읽기_끝.get()).as("두 읽기가 동시에 끝난다 — 차례면 %s 뒤다", 읽기.multipliedBy(2))
                .isEqualTo(읽기.toMillis());
    }
}

package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 프로브를 주기적으로 돌리는 루프.
 *
 * <p><b>회차가 간격을 넘기는 것이 정상이다.</b> 프로브가 의미를 갖는 유일한 상황이
 * 뒷단이 느린 상황이기 때문이다. 그때 루프가 죽으면 이 장치가 통째로 사라지고,
 * 회복은 다시 폴링 간격에 묶인다.
 */
@Tag("unit")
class BackendProbeLoopTest {

    private static final Duration 짧은_간격 = Duration.ofMillis(20);

    private static final Duration 기다림 = Duration.ofSeconds(5);

    /**
     * <b>회차가 간격을 넘겨도 계속 돈다.</b> {@code Flux.interval} 은 이 자리에서
     * 기다리지 않고 넘침으로 스트림을 끝낸다 — 그러면 프로세스가 사는 동안 프로브가
     * 영영 안 돌고, 밖에서는 켜 놓은 것으로 보인다.
     */
    @Test
    @DisplayName("회차가_간격을_넘겨도_계속_돈다")
    void 회차가_간격을_넘겨도_계속_돈다() {
        AtomicInteger 회차 = new AtomicInteger();
        BackendProbeLoop 루프 = BackendProbeLoop.of(
                () -> Mono.delay(짧은_간격.multipliedBy(5))
                        .doOnNext(tick -> 회차.incrementAndGet())
                        .then(),
                짧은_간격);

        루프.start();
        try {
            await().atMost(기다림).until(() -> 회차.get() >= 3);
        } finally {
            루프.stop();
        }

        assertThat(루프.isRunning()).isFalse();
    }

    /** 회차가 터져도 다음이 나간다. 안 그러면 한 번의 실패가 장치를 끝낸다. */
    @Test
    @DisplayName("회차가_터져도_다음이_나간다")
    void 회차가_터져도_다음이_나간다() {
        AtomicInteger 회차 = new AtomicInteger();
        BackendProbeLoop 루프 = BackendProbeLoop.of(
                () -> Mono.error(new IllegalStateException("터졌다 " + 회차.incrementAndGet())),
                짧은_간격);

        루프.start();
        try {
            await().atMost(기다림).until(() -> 회차.get() >= 3);
        } finally {
            루프.stop();
        }
    }

    /** 두 번 켜도 하나만 돈다. 겹치면 반쯤 열린 자리를 두 배로 먹는다. */
    @Test
    @DisplayName("두_번_켜도_하나만_돈다")
    void 두_번_켜도_하나만_돈다() {
        AtomicInteger 회차 = new AtomicInteger();
        BackendProbeLoop 루프 = BackendProbeLoop.of(
                () -> Mono.fromRunnable(회차::incrementAndGet), 짧은_간격);

        루프.start();
        루프.start();
        try {
            await().atMost(기다림).until(() -> 회차.get() >= 3);
            int 잰_값 = 회차.get();
            루프.stop();

            await().during(짧은_간격.multipliedBy(5)).atMost(기다림)
                    .until(() -> 회차.get() >= 잰_값);
            assertThat(루프.isRunning()).isFalse();
        } finally {
            루프.stop();
        }
    }
}

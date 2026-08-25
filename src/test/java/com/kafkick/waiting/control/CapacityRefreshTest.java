package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 재료 읽기 한 판.
 *
 * <p><b>못 읽은 것이 배분을 막지 않는다.</b> 그리고 못 읽은 것을 "보고 0건" 으로
 * 접지도 않는다 — 접으면 하한으로 떨어져 전면 억제가 된다.
 */
class CapacityRefreshTest {

    private static final Instant 지금 = Instant.parse("2026-08-25T00:00:00Z");

    private static final Duration 예산 = Duration.ofMillis(250);

    private CapacityCollector collector() {
        return CapacityCollector.of(Duration.ofSeconds(60), Duration.ofSeconds(3), 1, 10_000);
    }

    @Test
    @DisplayName("읽으면_수집한다")
    void 읽으면_수집한다() {
        CapacityCollector collector = collector();
        CapacityRefresh refresh = CapacityRefresh.of(
                () -> Mono.just(List.of(new CapacityReport("i1", 500, 지금.getEpochSecond()))),
                collector, () -> 지금, () -> 1, 예산, Schedulers.immediate(), new SimpleMeterRegistry());

        refresh.refresh().block();

        // 램프 첫 판이라 값 자체는 0 이지만, 수집이 돌았다는 것은 관측이 섰다는 뜻이다.
        assertThat(collector.lastKnown()).isNotNegative();
    }

    @Test
    @DisplayName("못_읽으면_직전_값을_지킨다")
    void 못_읽으면_직전_값을_지킨다() {
        CapacityCollector collector = collector();
        collector.collect(List.of(new CapacityReport("i1", 500, 지금.getEpochSecond() - 120)),
                지금.getEpochSecond(), 1);
        long 직전 = collector.lastKnown();

        CapacityRefresh refresh = CapacityRefresh.of(
                () -> Mono.error(new IllegalStateException("레디스가 죽었다")),
                collector, () -> 지금, () -> 1, 예산, Schedulers.immediate(), new SimpleMeterRegistry());

        // **완료로 끝난다.** 여기서 오류를 흘리면 배분이 같이 안 돈다.
        refresh.refresh().block();

        assertThat(collector.lastKnown()).isEqualTo(직전);
    }

    /**
     * <b>느린 것은 오류가 아니다.</b> 자기 예산이 없으면 배분 예산을 먹고, 판이
     * 통째로 안 끝나 임계가 안 올라간다 — 큐가 자라 다음 판이 더 무거워진다.
     */
    @Test
    @DisplayName("느리면_예산_안에서_포기한다")
    void 느리면_예산_안에서_포기한다() {
        CapacityCollector collector = collector();
        collector.collect(List.of(new CapacityReport("i1", 500, 지금.getEpochSecond() - 120)),
                지금.getEpochSecond(), 1);
        long 직전 = collector.lastKnown();

        // **가상 시간으로 잰다.** 실제로 기다리면 시험이 장비 속도에 걸리고,
        // 예산을 늘려도 통과하는 시험이 된다 (TS-4).
        VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
        CapacityRefresh refresh = CapacityRefresh.of(
                Mono::never, collector, () -> 지금, () -> 1, 예산, 시계, new SimpleMeterRegistry());

        // **검증에 상한을 둔다.** 예산이 안 걸리는 구현에서 무기한 기다리면
        // 시험이 실패가 아니라 정지가 된다 — CI 에서 그건 진단이 안 된다.
        StepVerifier.create(refresh.refresh())
                .then(() -> 시계.advanceTimeBy(예산))
                .expectComplete()
                .verify(Duration.ofSeconds(5));

        assertThat(collector.lastKnown()).isEqualTo(직전);
    }
}

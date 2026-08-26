package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * 배분 재료를 잰다.
 *
 * <p><b>못 재는 값은 회복했는지도 못 본다.</b> 전역 크레딧과 노드 수가 판정의 분자와
 * 분모인데 제어 평면에 미터가 없었다. 장애가 걷힌 뒤 지표가 돌아왔는지를 판정
 * 카운터로 역산할 수는 없다.
 */
class ControlPlaneMetricsTest {

    private static final Instant 지금 = Instant.parse("2026-08-25T00:00:00Z");

    private static final Duration 예산 = Duration.ofMillis(250);

    @Test
    @DisplayName("전역_크레딧과_노드_수를_잰다")
    void 전역_크레딧과_노드_수를_잰다() {
        MeterRegistry meters = new SimpleMeterRegistry();
        CapacityCollector collector =
                CapacityCollector.of(Duration.ofSeconds(60), Duration.ofSeconds(3), 5, 10_000);
        CapacityRefresh refresh = CapacityRefresh.of(
                () -> Mono.just(new CapacitySample(List.of(new CapacityReport("i1", 300, 지금.getEpochSecond())), 지금.getEpochSecond())),
                collector, () -> 3, 예산, Schedulers.immediate(), meters);

        refresh.refresh().block();

        // **수집기가 낸 값을 그대로 비춘다.** 숫자를 따로 적으면 램프 규칙이 바뀔 때
        // 지표가 아니라 시험이 틀린 것이 된다.
        assertThat(meters.get("waiting.capacity.credit").gauge().value())
                .isEqualTo(collector.lastKnown());
        assertThat(meters.get("waiting.capacity.nodes").gauge().value()).isEqualTo(3);
    }

    @Test
    @DisplayName("못_읽은_판을_센다")
    void 못_읽은_판을_센다() {
        MeterRegistry meters = new SimpleMeterRegistry();
        CapacityCollector collector =
                CapacityCollector.of(Duration.ofSeconds(60), Duration.ofSeconds(3), 5, 10_000);
        CapacityRefresh refresh = CapacityRefresh.of(
                () -> Mono.error(new IllegalStateException("레디스가 죽었다")),
                collector, () -> 1, 예산, Schedulers.immediate(), meters);

        refresh.refresh().block();

        // 못 읽은 것과 보고 0건은 다르다. 그 구간의 길이를 지표로만 잴 수 있다.
        assertThat(meters.counter("waiting.capacity.read.failed").count()).isEqualTo(1);
    }
}

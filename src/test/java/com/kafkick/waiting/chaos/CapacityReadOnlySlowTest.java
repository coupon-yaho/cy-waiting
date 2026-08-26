package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.CapacityCollector;
import com.kafkick.waiting.control.CapacityRefresh;
import com.kafkick.waiting.control.CapacityReport;
import com.kafkick.waiting.control.CapacitySample;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * <b>가용량 읽기만 예산을 넘는다.</b> 수요 읽기와 발행은 멀쩡하다.
 *
 * <p>읽기 예산이 틱의 1/4 이라 이것이 가장 흔한 부분 장애다. 레디스가 통째로
 * 죽으면 판 자체가 실패해 감쇠값이 배분에 닿지도 않는다 — 감쇠가 실제로 도는
 * 구간은 여기뿐이다.
 */
@Tag("chaos")
class CapacityReadOnlySlowTest {

    private static final long NOW = 1_800_000_000L;
    private static final Duration 램프 = Duration.ofSeconds(60);
    private static final Duration 신선도 = Duration.ofSeconds(3);
    private static final long 하한 = 10;
    private static final int 노드 = 4;

    private final AtomicBoolean 느리다 = new AtomicBoolean();

    private CapacityCollector collector() {
        return CapacityCollector.of(램프, 신선도, 하한, 100_000);
    }

    private CapacityRefresh refresh(CapacityCollector collector, long credits) {
        return CapacityRefresh.of(
                () -> 느리다.get()
                        // 예산을 넘긴다. 수요 읽기는 이 경로와 무관하다.
                        ? Mono.<CapacitySample>never()
                        : Mono.just(new CapacitySample(
                                List.of(new CapacityReport("i1", credits, NOW)), NOW)),
                collector, () -> 노드, Duration.ofMillis(50),
                Schedulers.immediate(), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

    /** 진입 — 유예 안에서는 직전 값 그대로다. 한 판 느렸다고 조이면 순단마다 흔들린다. */
    @Test
    @DisplayName("진입_유예_안에서는_크레딧이_그대로다")
    void 진입_유예_안에서는_크레딧이_그대로다() {
        CapacityCollector collector = collector();
        CapacityRefresh refresh = refresh(collector, 10_000);
        refresh.refresh().block();
        long 정상 = collector.lastKnown();

        느리다.set(true);
        for (int i = 0; i < CapacityCollector.HOLD_ROUNDS; i++) {
            refresh.refresh().block();
        }

        assertThat(collector.lastKnown()).isEqualTo(정상);
    }

    /**
     * 유지 — 줄어들되 <b>노드당 몫이 유휴 역수 아래로 안 내려간다</b>. 그 아래면
     * 한산한 쿠폰이 전 노드에서 막힌다 (R1).
     */
    @Test
    @DisplayName("유지_바닥이_노드를_받친다")
    void 유지_바닥이_노드를_받친다() {
        CapacityCollector collector = collector();
        CapacityRefresh refresh = refresh(collector, 10_000);
        refresh.refresh().block();

        느리다.set(true);
        for (int i = 0; i < 100; i++) {
            refresh.refresh().block();
        }

        long 바닥 = (long) 노드 * CapacityCollector.IDLE_DIVISOR;
        assertThat(collector.lastKnown()).isEqualTo(Math.max(하한, 바닥));
        assertThat(collector.lastKnown() / 노드)
                .as("노드당 몫이 유휴 역수 이상")
                .isGreaterThanOrEqualTo(CapacityCollector.IDLE_DIVISOR);
    }

    /**
     * 유지 — 뒷단이 스스로 "여유 0" 이라고 말한 뒤라면 <b>감쇠가 그것을 안 올린다</b>.
     * 죽었다고 말한 뒷단에 바닥만큼 다시 밀어넣으면 서킷이 half-open 으로 갈 때
     * 시험 트래픽이 아니라 상시 유입이 도달해 있다.
     */
    @Test
    @DisplayName("유지_보고한_0은_안_올라간다")
    void 유지_보고한_0은_안_올라간다() {
        CapacityCollector collector = collector();
        CapacityRefresh refresh = refresh(collector, 0);
        refresh.refresh().block();
        assertThat(collector.lastKnown()).isZero();

        느리다.set(true);
        for (int i = 0; i < 100; i++) {
            refresh.refresh().block();
        }

        assertThat(collector.lastKnown()).isZero();
    }

    /** 회복 — 첫 성공 판에 실측으로 돌아온다. 유예도 다시 찬다. */
    @Test
    @DisplayName("회복_첫_판에_실측으로_돌아온다")
    void 회복_첫_판에_실측으로_돌아온다() {
        CapacityCollector collector = collector();
        CapacityRefresh refresh = refresh(collector, 10_000);
        refresh.refresh().block();
        long 정상 = collector.lastKnown();
        느리다.set(true);
        for (int i = 0; i < 20; i++) {
            refresh.refresh().block();
        }
        assertThat(collector.lastKnown()).isLessThan(정상);

        느리다.set(false);
        refresh.refresh().block();

        assertThat(collector.lastKnown()).isEqualTo(정상);
        // 유예가 다시 찼다 — 바로 다음 실패에 안 깎인다.
        느리다.set(true);
        for (int i = 0; i < CapacityCollector.HOLD_ROUNDS; i++) {
            refresh.refresh().block();
        }
        assertThat(collector.lastKnown()).isEqualTo(정상);
    }
}

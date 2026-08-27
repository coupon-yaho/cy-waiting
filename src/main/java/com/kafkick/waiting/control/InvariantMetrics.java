package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.ClockSkewTracker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

/**
 * 불변식이 깨지기 <b>전에</b> 오르는 값들 (6.9.1).
 *
 * <p>초과 발급 자체는 재고를 가진 발급 계층만 압니다. 게이트웨이는 <b>스스로
 * 계산한 값</b>으로 대신 봅니다 — 여기가 오르면 원인이 이쪽에 있습니다.
 */
public final class InvariantMetrics {

    private final AllocationRound round;

    private final ClockSkewTracker skew;

    private InvariantMetrics(AllocationRound round, ClockSkewTracker skew) {
        this.round = Objects.requireNonNull(round, "round 는 필수다");
        this.skew = Objects.requireNonNull(skew, "skew 는 필수다");
    }

    /**
     * 지표에 겁니다.
     *
     * <p><b>강한 참조로 등록합니다.</b> 약한 참조면 첫 GC 에 수거되어 영원히
     * {@code NaN} 을 내는데, 스크레이프에는 줄이 그대로 나갑니다.
     */
    public static InvariantMetrics bind(AllocationRound round, ClockSkewTracker skew,
            MeterRegistry meters) {
        Objects.requireNonNull(meters, "meters 는 필수다");
        InvariantMetrics metrics = new InvariantMetrics(round, skew);

        metrics.gauge(meters, "waiting.allocation.over", InvariantMetrics::overAllocated,
                "마지막 틱에 가진 것보다 더 나눠 준 양. 초과 발급의 선행 지표다");
        metrics.gauge(meters, "waiting.queue.floor.applied", InvariantMetrics::floorApplied,
                "시계가 뒤로 가 바닥값이 걸린 횟수. 순번 역행의 선행 지표다");
        metrics.gauge(meters, "waiting.queue.floor.skew.micros", InvariantMetrics::maxSkew,
                "관측된 최대 역행 폭(마이크로초)");
        // 강한 참조로 걸었으므로 이 인스턴스가 살아 있어야 값이 안 굳는다.
        return metrics;
    }

    /** <b>태그를 안 붙입니다.</b> 쿠폰 식별자는 가짓수에 상한이 없습니다 (LG-4). */
    private void gauge(MeterRegistry meters, String name,
            ToDoubleFunction<InvariantMetrics> read, String why) {
        Gauge.builder(name, this, read)
                .description(why)
                .strongReference(true)
                .register(meters);
    }

    /** 0 이 정상이고, 그 밖은 전부 사고입니다. */
    private double overAllocated() {
        return round.lastOverAllocated();
    }

    /** 오르면 시계가 뒤로 갔다는 뜻입니다. 가드가 잡았어도 원인은 남아 있습니다. */
    private double floorApplied() {
        return skew.appliedCount();
    }

    private double maxSkew() {
        return skew.maxSkewMicros();
    }
}

package com.kafkick.waiting.routing;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.ToDoubleFunction;

/**
 * 인스턴스로 나간 요청이 지금 얼마나 물려 있는지 (9.2.6).
 *
 * <p><b>누수는 값이 안 내려가는 것으로만 보인다.</b> 부하가 끝났는데 0 이 아니면
 * 감소를 어디선가 놓친 것이고, 그 인스턴스는 고르개에서 조용히 배제된다 (G9.3).
 */
public final class InFlightMetrics {

    private final InFlightRegistry registry;

    private final InstanceOutliers outliers;

    private final LongSupplier nowMillis;

    private InFlightMetrics(InFlightRegistry registry, InstanceOutliers outliers,
            LongSupplier nowMillis) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.outliers = Objects.requireNonNull(outliers, "outliers");
        this.nowMillis = Objects.requireNonNull(nowMillis, "nowMillis");
    }

    /**
     * 레지스트리를 지표에 건다.
     *
     * <p><b>강한 참조로 등록한다.</b> 약한 참조면 첫 GC 에 수거되어 영원히
     * {@code NaN} 을 내는데, 스크레이프에는 줄이 그대로 나간다.
     */
    public static void bind(InFlightRegistry registry, InstanceOutliers outliers,
            LongSupplier nowMillis, MeterRegistry meters) {
        Objects.requireNonNull(meters, "meters");
        InFlightMetrics metrics = new InFlightMetrics(registry, outliers, nowMillis);

        metrics.gauge(meters, "waiting.routing.inflight", InFlightMetrics::total,
                "지금 뒷단에 물려 있는 요청 수. 부하가 끝나면 0 이어야 한다");
        metrics.gauge(meters, "waiting.routing.inflight.busiest", InFlightMetrics::busiest,
                "가장 바쁜 인스턴스의 수. 합만으로는 쏠림이 안 보인다");
        metrics.gauge(meters, "waiting.routing.instances", InFlightMetrics::instances,
                "카운터를 들고 있는 인스턴스 수. 안 줄면 사라진 대가 남은 것이다");
        metrics.gauge(meters, "waiting.routing.ejected", InFlightMetrics::ejected,
                "연속 실패로 표시된 인스턴스 수. 전체 대수와 같아지면 뒷단 전체가 "
                        + "앓는 것이고, 그때는 배제가 안 걸린 채 그대로 나간다");
    }

    /**
     * <b>인스턴스 식별자를 라벨에 안 붙인다.</b> 재기동마다 새로 오므로(R-3)
     * 시계열이 무한히 늘고, 하나 붙는 순간 지표가 메모리를 밀어낸다 (LG-4).
     */
    private void gauge(MeterRegistry meters, String name,
            ToDoubleFunction<InFlightMetrics> read, String why) {
        Gauge.builder(name, this, read)
                .description(why)
                .strongReference(true)
                .register(meters);
    }

    private double total() {
        return registry.total(nowMillis.getAsLong());
    }

    private double busiest() {
        return registry.busiest(nowMillis.getAsLong());
    }

    private double instances() {
        return registry.instances().size();
    }

    private double ejected() {
        return outliers.ejectedCount(nowMillis.getAsLong());
    }

    /** 걸었다는 표시. 빈으로 두어야 스프링이 이 배선을 실제로 돌린다. */
    public record Binding() {
    }
}

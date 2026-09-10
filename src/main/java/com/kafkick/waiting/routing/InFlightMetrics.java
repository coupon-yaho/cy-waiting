package com.kafkick.waiting.routing;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.ToDoubleFunction;

/**
 * 인스턴스로 나간 요청이 지금 얼마나 물려 있는지.
 *
 * <p><b>누수는 값이 안 내려가는 것으로만 보인다.</b> 부하가 끝났는데 0 이 아니면
 * 감소를 어디선가 놓친 것이고, 그 인스턴스는 고르개에서 조용히 배제된다.
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
     * <p><b>게이지는 강한 참조로 건다.</b> 약한 참조면 첫 GC 에 수거되어 영원히
     * {@code NaN} 을 내고, 카운터는 그 게이지와 같은 객체를 써서 함께 산다.
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
                "연속 실패로 표시된 인스턴스 수. waiting.routing.seen 과 같아지면 "
                        + "뒷단 전체가 앓는 것이고, 그때는 배제가 안 걸린 채 그대로 나간다");
        metrics.gauge(meters, "waiting.routing.seen", InFlightMetrics::seen,
                "마지막으로 본 인스턴스 수. 위 값을 여기에 견준다 — 물린 건수 쪽 "
                        + "게이지는 배제된 대가 빠져서 견줄 대상이 못 된다");
        // **되돌리는 중은 배제도 정상도 아니다.** 위 게이지들이 그 구간을 못 잡아,
        // 그 대가 회복을 마쳤는지를 운영에서 물을 수단이 없었다.
        metrics.gauge(meters, "waiting.routing.ramping", InFlightMetrics::ramping,
                "되돌리는 중인 인스턴스 수. 배제 게이지가 안 세는 구간이라 "
                        + "이 값이 안 내려가면 회복이 안 끝나고 있는 것이다");
        metrics.gauge(meters, "waiting.routing.ramp.suppressed", InFlightMetrics::rampSuppressed,
                "되돌리는 중이라 깎기로 한 몫의 합. 고르개가 대마다 최소 하나는 "
                        + "남기므로 실제로 막히는 양은 이보다 작다");
        // **진입만 세면 해제를 못 본다.** 되돌리다 다시 빠지는 것과 끝까지 마치는 것을
        // 갈라야 회복이 도는지 맴도는지가 갈린다.
        metrics.counter(meters, "waiting.routing.ejections.started",
                InFlightMetrics::ejectionsStarted,
                "정상 구간에서 뺀 횟수. 배제 국면 하나가 여기서 열린다");
        metrics.counter(meters, "waiting.routing.ejections.repeated",
                InFlightMetrics::reEjections,
                "되돌리는 중에 다시 뺀 횟수. 이것만 늘고 완주가 안 늘면 회복이 맴돈다");
        metrics.counter(meters, "waiting.routing.ejections.overridden",
                InFlightMetrics::ejectionsOverridden,
                "배제가 무시된 국면 수. 배제 게이지가 든 수가 실제로 걸렸는지를 "
                        + "여기에 견준다 — 그 구간의 그 대는 몫이 안 깎인 채 받는다");
        metrics.counter(meters, "waiting.routing.ramp.completed",
                InFlightMetrics::rampsCompleted,
                "되돌리기를 끝까지 마친 횟수. 라우팅이 한 건도 안 돌면 같이 멎는다");
    }

    /**
     * 누적을 그대로 읽는다. <b>게이지로 두면 안 된다</b> — 되돌아가지 않는 값이라
     * 스크레이프 사이의 증가분을 셈하는 쪽이 맞다. 여기는 약한 참조라, 위 게이지들과
     * <b>같은 객체를 넘겨</b> 그쪽 강한 참조에 얹는다.
     */
    private void counter(MeterRegistry meters, String name,
            ToDoubleFunction<InFlightMetrics> read, String why) {
        FunctionCounter.builder(name, this, read)
                .description(why)
                .register(meters);
    }

    private double ramping() {
        return outliers.rampingCount(nowMillis.getAsLong());
    }

    private double rampSuppressed() {
        return outliers.rampSuppressed(nowMillis.getAsLong());
    }

    private double ejectionsStarted() {
        return outliers.ejectionsStarted();
    }

    private double reEjections() {
        return outliers.reEjections();
    }

    private double ejectionsOverridden() {
        return outliers.ejectionsOverridden();
    }

    private double rampsCompleted() {
        return outliers.rampsCompleted();
    }

    /**
     * <b>인스턴스 식별자를 라벨에 안 붙인다.</b> 재기동마다 새로 오므로 시계열이
     * 무한히 늘고, 하나 붙는 순간 지표가 메모리를 밀어낸다. 아래 카운터들이 이
     * 강한 참조에 얹혀 있으므로, 게이지를 다 걷으면 그쪽이 첫 GC 에 죽는다.
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
        return outliers.markedCount(nowMillis.getAsLong());
    }

    private double seen() {
        return outliers.seenCount();
    }

    /** 걸었다는 표시. 빈으로 두어야 스프링이 이 배선을 실제로 돌린다. */
    public record Binding() {
    }
}

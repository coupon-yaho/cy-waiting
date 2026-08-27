package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.Bulkhead;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

/**
 * 격벽이 지금 얼마나 차 있는지를 냅니다.
 *
 * <p><b>막은 횟수만으로는 부족합니다.</b> 그 값은 이미 막힌 뒤에야 오릅니다.
 * 차오르는 중인지 비어 있는지를 봐야 상한을 올릴지 뒷단을 늘릴지 판단합니다.
 */
public final class BulkheadMetrics {

    private final Bulkhead bulkhead;

    private BulkheadMetrics(Bulkhead bulkhead) {
        this.bulkhead = Objects.requireNonNull(bulkhead, "bulkhead 는 필수다");
    }

    /**
     * 격벽을 지표에 겁니다.
     *
     * <p><b>강한 참조로 등록합니다.</b> 약한 참조면 첫 GC 에 수거되어 영원히
     * {@code NaN} 을 내는데, 스크레이프에는 줄이 그대로 나갑니다. 쿠폰별 라벨은
     * 안 붙입니다 — 식별자는 가짓수에 상한이 없습니다 (LG-4).
     */
    public static void bind(Bulkhead bulkhead, MeterRegistry meters) {
        Objects.requireNonNull(meters, "meters 는 필수다");
        BulkheadMetrics metrics = new BulkheadMetrics(bulkhead);

        gauge(meters, "waiting.bulkhead.in.flight", metrics, BulkheadMetrics::inFlight,
                "지금 뒷단에 걸려 있는 요청 수");
        gauge(meters, "waiting.bulkhead.coupons", metrics, BulkheadMetrics::coupons,
                "자리를 쥐고 있는 쿠폰 수. 상한에 붙으면 새 쿠폰이 못 들어간다");
    }

    // RULE-EXCEPTION(JS-13): 등록 시점에만 쓰이는 배선이다. 인스턴스 메서드로 두면
    // 게이지 대상과 등록 주체가 같은 객체가 되어 참조 관계가 흐려진다.
    private static void gauge(MeterRegistry meters, String name, BulkheadMetrics target,
            ToDoubleFunction<BulkheadMetrics> read, String why) {
        Gauge.builder(name, target, read)
                .description(why)
                .strongReference(true)
                .register(meters);
    }

    /** 이 값이 상한에 붙으면 곧 막히기 시작합니다. */
    private double inFlight() {
        return bulkhead.inFlight();
    }

    /** 맵이 상한에 붙으면 새 쿠폰이 아예 못 들어갑니다. */
    private double coupons() {
        return bulkhead.size();
    }
}

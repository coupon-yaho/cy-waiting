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
 */
public final class BulkheadMetrics {

    private final Bulkhead bulkhead;

    /**
     * 담을 수 있는 쿠폰 수.
     *
     * <p><b>분자만 내면 판단이 안 됩니다.</b> 800 이라는 값만 보고는 여유인지
     * 임박인지 모릅니다 — 분모가 코드 상수로만 있으면 알람이 그 숫자를 베껴
     * 적고, 상수를 바꾸는 날 조용히 갈라집니다.
     */
    private final int maxCoupons;

    private BulkheadMetrics(Bulkhead bulkhead, int maxCoupons) {
        this.bulkhead = Objects.requireNonNull(bulkhead, "bulkhead 는 필수다");
        this.maxCoupons = maxCoupons;
    }

    /**
     * 격벽을 지표에 겁니다.
     *
     * <p><b>강한 참조로 등록합니다.</b> 약한 참조면 첫 GC 에 수거되어 영원히
     * {@code NaN} 을 내는데, 스크레이프에는 줄이 그대로 나갑니다.
     */
    public static void bind(Bulkhead bulkhead, int maxCoupons, MeterRegistry meters) {
        Objects.requireNonNull(meters, "meters 는 필수다");
        BulkheadMetrics metrics = new BulkheadMetrics(bulkhead, maxCoupons);

        metrics.gauge(meters, "waiting.bulkhead.inflight", BulkheadMetrics::inFlight,
                "지금 뒷단에 걸려 있는 요청 수");
        metrics.gauge(meters, "waiting.bulkhead.coupons", BulkheadMetrics::coupons,
                "자리를 쥐고 있는 쿠폰 수");
        metrics.gauge(meters, "waiting.bulkhead.max.coupons", BulkheadMetrics::maxCoupons,
                "담을 수 있는 쿠폰 수. 분모가 없으면 포화를 못 잰다");
    }

    /**
     * <b>태그를 안 붙입니다.</b> 쿠폰 식별자는 밖에서 오는 값이라 가짓수에 상한이
     * 없고, 하나 붙는 순간 지표 하나가 메모리를 밀어냅니다 (LG-4).
     */
    private void gauge(MeterRegistry meters, String name,
            ToDoubleFunction<BulkheadMetrics> read, String why) {
        Gauge.builder(name, this, read)
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

    private double maxCoupons() {
        return maxCoupons;
    }
}

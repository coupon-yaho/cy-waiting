package com.kafkick.waiting.control;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

/**
 * 판정이 지금 무엇을 보고 있는지를 지표로 냅니다.
 *
 * <p>안 내면 사고가 났을 때 게이트웨이가 어떤 재료로 판정했는지를 사후에
 * 재구성할 수 없습니다. 요청당 로그를 남기지 않기 때문입니다 (LG-1).
 */
public final class SnapshotMetrics {

    /** 아직 값을 모른다는 뜻입니다. 헬스 지시자와 같은 값을 씁니다. */
    private static final double UNKNOWN = -1;

    private final SnapshotHolder holder;

    private SnapshotMetrics(SnapshotHolder holder) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
    }

    /**
     * 홀더를 지표에 겁니다.
     *
     * <p><b>강한 참조로 등록합니다.</b> {@code MeterRegistry.gauge(name, obj, fn)} 는
     * 대상을 약한 참조로 잡으므로, 여기서 만든 객체가 첫 GC 에 수거되고 그 뒤로는
     * 세 지표가 영원히 {@code NaN} 을 냅니다. 스크레이프에는 줄이 그대로 나가기
     * 때문에 이름만 확인하는 시험으로는 드러나지 않습니다.
     */
    public static void bind(SnapshotHolder holder, MeterRegistry meters) {
        Objects.requireNonNull(meters, "meters 는 필수다");
        SnapshotMetrics metrics = new SnapshotMetrics(holder);

        metrics.gauge(meters, "waiting.queue.waiting", SnapshotMetrics::waitingTotal,
                "전 쿠폰의 대기 인원 합");
        metrics.gauge(meters, "waiting.snapshot.age", SnapshotMetrics::ageSeconds,
                "판정 재료의 나이(초). 아직 못 받았으면 -1");
        metrics.gauge(meters, "waiting.snapshot.coupons", SnapshotMetrics::couponCount,
                "지금 보고 있는 쿠폰 수");
    }

    private void gauge(MeterRegistry meters, String name,
            ToDoubleFunction<SnapshotMetrics> read, String why) {
        Gauge.builder(name, this, read)
                .description(why)
                .strongReference(true)
                .register(meters);
    }

    /** 전 쿠폰의 대기 인원 합입니다. 이 값이 곧 "지금 얼마나 밀렸는가" 입니다. */
    private double waitingTotal() {
        return holder.current().coupons().values().stream()
                .mapToLong(state -> state.waiting())
                .sum();
    }

    /**
     * 재료가 얼마나 낡았는지를 냅니다. 판정이 낡음으로 넘어가는 순간을 짚는 값입니다.
     *
     * <p><b>못 받았으면 -1 입니다.</b> 기준선이 {@code Instant.EPOCH} 라 그대로 내면
     * 17억이 나갑니다. 가르는 기준은 루프가 돌았는지가 아니라 <b>재료를 받았는지</b>
     * 입니다 — 실패해도 루프는 돌아서, 첫 틱으로 가르면 레디스가 죽은 채 뜬 노드가
     * 곧바로 17억을 냅니다. 헬스 지시자가 같은 기준을 씁니다.
     */
    private double ageSeconds() {
        SnapshotHolder.View view = holder.view();
        return view.snapshot().isPublished() ? view.dataAge().toMillis() / 1000.0 : UNKNOWN;
    }

    /** 보고 있는 쿠폰 수입니다. 0 이면 배분이 멎었거나 활성 목록이 빈 것입니다. */
    private double couponCount() {
        return holder.current().coupons().size();
    }
}

package com.kafkick.waiting.control;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import com.kafkick.waiting.domain.coupon.Tunables;
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
        // **받아 오는 것과 배분이 도는 것은 다른 고장입니다** (8.4.4). 배분이
        // 멎어도 재료는 계속 받아 오므로 응답은 정상으로 보이고, 그동안 줄은
        // 안 빠집니다. 나이를 합쳐 두면 그 구간이 지표에서 안 보입니다.
        metrics.gauge(meters, "waiting.snapshot.fetch.age", SnapshotMetrics::fetchAgeSeconds,
                "재료를 마지막으로 받아 온 지 지난 시간(초). 아직 못 받았으면 -1");
        metrics.gauge(meters, "waiting.snapshot.tick.age", SnapshotMetrics::tickAgeSeconds,
                "배분이 마지막으로 돈 지 지난 시간(초). 아직 안 돌았으면 -1");
        // **지금 무엇이 걸려 있는지가 보여야 합니다** (6.8.4). 안 보이면 값을
        // 바꾸고도 그것이 닿았는지를 못 확인하고, 장애 중에 그 확인이 필요합니다.
        metrics.gauge(meters, "waiting.tunable.idle.ratio", SnapshotMetrics::idleRatio,
                "적용 중인 한산 몫. 안 실려 왔으면 -1");
        metrics.gauge(meters, "waiting.tunable.inflight.seconds",
                SnapshotMetrics::inFlightSeconds,
                "적용 중인 걸림 시간(초). 안 실려 왔으면 -1");
    }

    private void gauge(MeterRegistry meters, String name,
            ToDoubleFunction<SnapshotMetrics> read, String why) {
        Gauge.builder(name, this, read)
                .description(why)
                .strongReference(true)
                .register(meters);
    }

    /** 적용 중인 한산 몫. <b>{@code -1} 은 "안 실려 왔다" 는 뜻입니다.</b> */
    // 그때는 각 노드가 자기 기동 설정으로 돈다. 운영 값으로는 0 을 못 넣으므로
    // 이 게이지가 0 을 낼 일은 없다 — 음수만이 미전파를 뜻한다.
    private double idleRatio() {
        Tunables applied = holder.current().meta().tunables();
        return applied == null ? UNKNOWN : applied.idleCreditRatio();
    }

    /** 적용 중인 걸림 시간. 안 실려 왔으면 -1 입니다. */
    private double inFlightSeconds() {
        Tunables applied = holder.current().meta().tunables();
        return applied == null ? UNKNOWN : applied.inFlightSeconds();
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
    private double fetchAgeSeconds() {
        SnapshotHolder.View view = holder.view();
        return view.snapshot().isPublished() ? view.fetchAge().toMillis() / 1000.0 : UNKNOWN;
    }

    /** 배분 루프가 마지막으로 돈 뒤 지난 시간. <b>이것만 크면 배분이 멎은 것입니다.</b> */
    private double tickAgeSeconds() {
        SnapshotHolder.View view = holder.view();
        return view.snapshot().isPublished() ? view.tickAge().toMillis() / 1000.0 : UNKNOWN;
    }

    private double ageSeconds() {
        SnapshotHolder.View view = holder.view();
        return view.snapshot().isPublished() ? view.dataAge().toMillis() / 1000.0 : UNKNOWN;
    }

    /** 보고 있는 쿠폰 수입니다. 0 이면 배분이 멎었거나 활성 목록이 빈 것입니다. */
    private double couponCount() {
        return holder.current().coupons().size();
    }
}

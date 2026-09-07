package com.kafkick.waiting.control;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import com.kafkick.waiting.domain.coupon.Tunables;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

/**
 * 판정이 지금 무엇을 보고 있는지를 지표로 낸다. 안 내면 사고가 났을 때 어떤 재료로
 * 판정했는지를 사후에 재구성할 수 없다 — 요청당 로그를 안 남기기 때문이다.
 */
public final class SnapshotMetrics {

    /** 아직 값을 모른다는 뜻. 헬스 지시자와 같은 값을 쓴다. */
    private static final double UNKNOWN = -1;

    private final SnapshotHolder holder;

    private SnapshotMetrics(SnapshotHolder holder) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
    }

    /**
     * 홀더를 지표에 건다. <b>강한 참조로 등록한다</b> — {@code MeterRegistry.gauge} 는
     * 대상을 약한 참조로 잡으므로 여기서 만든 객체가 첫 GC 에 수거되면 그 뒤로는 지표가
     * 영원히 {@code NaN} 이다. 스크레이프에 줄은 그대로 나가 이름만 보는 시험은 못 잡는다.
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
        // **받아 오는 것과 배분이 도는 것은 다른 고장이다.** 배분이 멎어도 재료는 계속
        // 받아 오므로 응답은 정상으로 보이고, 그동안 줄은 안 빠진다. 나이를 합쳐 두면
        // 그 구간이 지표에서 안 보인다.
        metrics.gauge(meters, "waiting.snapshot.fetch.age", SnapshotMetrics::fetchAgeSeconds,
                "재료를 마지막으로 받아 온 지 지난 시간(초). 아직 못 받았으면 -1");
        metrics.gauge(meters, "waiting.snapshot.tick.age", SnapshotMetrics::tickAgeSeconds,
                "배분이 마지막으로 돈 지 지난 시간(초). 아직 안 돌았으면 -1");
        // **지금 무엇이 걸려 있는지가 보여야 한다.** 안 보이면 값을 바꾸고도
        // 그것이 닿았는지를 못 확인하고, 장애 중에 그 확인이 필요하다.
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

    /**
     * 적용 중인 한산 몫. <b>{@code -1} 은 "안 실려 왔다" 는 뜻이다.</b> 운영 값으로 0 을
     * 못 넣으므로 이 게이지가 0 을 낼 일은 없다 — 음수만이 미전파를 뜻한다.
     */
    private double idleRatio() {
        Tunables applied = holder.current().meta().tunables();
        return applied == null ? UNKNOWN : applied.idleCreditRatio();
    }

    /** 적용 중인 걸림 시간. 안 실려 왔으면 -1 이다. */
    private double inFlightSeconds() {
        Tunables applied = holder.current().meta().tunables();
        return applied == null ? UNKNOWN : applied.inFlightSeconds();
    }

    /** 전 쿠폰의 대기 인원 합. 이 값이 곧 "지금 얼마나 밀렸는가" 다. */
    private double waitingTotal() {
        return holder.current().coupons().values().stream()
                .mapToLong(state -> state.waiting())
                .sum();
    }

    /**
     * 판정이 낡음으로 넘어가는 순간을 짚는 값. <b>못 받았으면 -1 이다</b> — 기준선이
     * {@code Instant.EPOCH} 라 그대로 내면 17억이 나간다. 루프가 돌았는지가 아니라
     * <b>재료를 받았는지</b>로 가른다. 실패해도 루프는 돌기 때문이다.
     */
    private double fetchAgeSeconds() {
        SnapshotHolder.View view = holder.view();
        return view.snapshot().isPublished() ? view.fetchAge().toMillis() / 1000.0 : UNKNOWN;
    }

    /**
     * 조회 루프가 마지막으로 돈 뒤 지난 시간. <b>널 검사를 안 겹친다</b> — 틱 시각이 없는
     * 것은 초기값 하나뿐이고 그것은 {@code EPOCH} 라 발행으로 안 읽힌다.
     */
    private double tickAgeSeconds() {
        SnapshotHolder.View view = holder.view();
        return view.snapshot().isPublished() ? view.tickAge().toMillis() / 1000.0 : UNKNOWN;
    }

    private double ageSeconds() {
        SnapshotHolder.View view = holder.view();
        return view.snapshot().isPublished() ? view.dataAge().toMillis() / 1000.0 : UNKNOWN;
    }

    /** 보고 있는 쿠폰 수. 0 이면 배분이 멎었거나 활성 목록이 빈 것이다. */
    private double couponCount() {
        return holder.current().coupons().size();
    }
}

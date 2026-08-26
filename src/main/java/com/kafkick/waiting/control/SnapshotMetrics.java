package com.kafkick.waiting.control;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;

/**
 * 판정이 지금 무엇을 보고 있는지를 지표로 낸다.
 *
 * <p>안 내면 사고가 났을 때 게이트웨이가 어떤 재료로 판정했는지를 사후에
 * 재구성할 수 없다. 로그로는 못 한다 — 요청당 로그를 안 남기기 때문이다 (LG-1).
 */
public final class SnapshotMetrics {

    private final SnapshotHolder holder;

    private SnapshotMetrics(SnapshotHolder holder) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
    }

    /**
     * 홀더를 지표에 건다.
     *
     * <p><b>게이지다.</b> 지금 값이 궁금하지 누적이 아니다. 홀더가 값을 갈아
     * 끼우므로 여기서 다시 읽기만 한다. 쿠폰별 라벨은 안 붙인다 — 식별자는
     * 인증이 없어 아무 문자열이나 들어오고, 시계열이 그만큼 는다 (LG-4).
     */
    public static void bind(SnapshotHolder holder, MeterRegistry meters) {
        Objects.requireNonNull(meters, "meters 는 필수다");
        SnapshotMetrics metrics = new SnapshotMetrics(holder);

        meters.gauge("waiting.queue.waiting", metrics, SnapshotMetrics::waitingTotal);
        meters.gauge("waiting.snapshot.age", metrics, SnapshotMetrics::ageSeconds);
        meters.gauge("waiting.snapshot.coupons", metrics, SnapshotMetrics::couponCount);
    }

    /** 전 쿠폰의 대기 인원 합. 이 값이 곧 "지금 얼마나 밀렸나" 다. */
    private double waitingTotal() {
        return holder.current().coupons().values().stream()
                .mapToLong(state -> state.waiting())
                .sum();
    }

    /**
     * 재료가 얼마나 낡았는가.
     *
     * <p>판정이 낡음으로 넘어가는 순간을 사후에 짚는 값이다. 임계는
     * {@code dataStaleAfter} 이고, 넘으면 사다리가 다른 줄을 탄다.
     */
    private double ageSeconds() {
        return holder.dataAge().toMillis() / 1000.0;
    }

    /** 보고 있는 쿠폰 수. 0 이면 배분이 멎었거나 활성 목록이 빈 것이다. */
    private double couponCount() {
        return holder.current().coupons().size();
    }
}

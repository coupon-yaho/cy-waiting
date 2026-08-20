package com.kafkick.waiting;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

/**
 * 시험이 앞으로 감는 시계 (TS-4).
 *
 * <p>{@link Clock#fixed} 는 못 움직여서 시각을 옮기려면 프로덕션 코드에
 * <b>시험용 구멍</b>을 내게 된다. 그 구멍은 운영에서 아무도 안 쓰지만 지워지지도
 * 않는다 — 여기 두면 프로덕션은 {@link Clock} 만 알면 된다.
 */
public final class MutableClock extends Clock {

    private final ZoneId zone;
    private Instant now;

    private MutableClock(Instant now, ZoneId zone) {
        // null 이 들어오면 instant()·getZone() 이 null 을 돌려주는 시계가 된다.
        // 그건 Clock 이 아니고, 쓰는 쪽에서 엉뚱한 NPE 로 드러난다.
        this.now = Objects.requireNonNull(now, "now");
        this.zone = Objects.requireNonNull(zone, "zone");
    }

    /** 주어진 시각에 멈춰 있는 UTC 시계. {@link #앞으로} 로만 움직인다. */
    public static MutableClock at(Instant now) {
        return new MutableClock(now, ZoneId.of("UTC"));
    }

    public void 앞으로(Duration 만큼) {
        now = now.plus(만큼);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MutableClock(now, zone);
    }

    @Override
    public Instant instant() {
        return now;
    }
}

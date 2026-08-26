package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 지금 몇 명이 서 있고 얼마나 낡았는지.
 *
 * <p><b>판정이 무엇을 보고 있는지가 지표로 나와야 한다.</b> 안 그러면 사고가 났을
 * 때 게이트웨이가 어떤 재료로 판정했는지를 사후에 재구성할 수 없다.
 */
class SnapshotMetricsTest {

    private static final Instant 지금 = Instant.parse("2026-08-26T00:00:00Z");

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(5), Clock.fixed(지금, ZoneOffset.UTC));

    private void 지표를_건다() {
        SnapshotMetrics.bind(holder, meters);
    }

    private void 스냅샷을_심는다(long waiting) {
        holder.replace(new GatewaySnapshot(
                Map.of("c1", CouponStates.queueing(10, 1_000, waiting),
                        "c2", CouponStates.queueing(5, 500, waiting * 2)),
                new SnapshotMeta(1_000, 4), 지금));
    }

    /**
     * <b>대기 인원은 합으로 본다.</b> 쿠폰별로 라벨을 붙이면 시계열이 쿠폰 수만큼
     * 늘고, 쿠폰 식별자는 인증이 없어 아무 문자열이나 들어온다 (LG-4).
     */
    @Test
    @DisplayName("대기_인원의_합을_낸다")
    void 대기_인원의_합을_낸다() {
        지표를_건다();

        스냅샷을_심는다(100);

        assertThat(meters.get("waiting.queue.waiting").gauge().value()).isEqualTo(300);
    }

    /** 재료가 얼마나 낡았는지. 판정이 낡음으로 넘어가는 순간을 사후에 짚는 값이다. */
    @Test
    @DisplayName("재료의_나이를_낸다")
    void 재료의_나이를_낸다() {
        지표를_건다();
        스냅샷을_심는다(100);

        assertThat(meters.get("waiting.snapshot.age").gauge().value()).isZero();
    }

    /** 몇 개 쿠폰을 보고 있는지. 활성 목록이 비면 배분이 통째로 멎는다. */
    @Test
    @DisplayName("보고_있는_쿠폰_수를_낸다")
    void 보고_있는_쿠폰_수를_낸다() {
        지표를_건다();

        스냅샷을_심는다(100);

        assertThat(meters.get("waiting.snapshot.coupons").gauge().value()).isEqualTo(2);
    }

    /**
     * <b>재료를 한 번도 못 받은 구간이 보여야 한다.</b> 기동 직후에는 판정을
     * 미루고 그냥 흘려보내므로, 그 구간이 길면 상한 없이 뒷단으로 간다.
     */
    @Test
    @DisplayName("첫_틱_전에는_쿠폰이_0이다")
    void 첫_틱_전에는_쿠폰이_0이다() {
        지표를_건다();

        assertThat(meters.get("waiting.snapshot.coupons").gauge().value()).isZero();
    }
}

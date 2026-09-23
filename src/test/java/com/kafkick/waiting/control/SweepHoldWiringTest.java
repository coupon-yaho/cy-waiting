package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.MutableClock;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 청소를 멈추는 조건. 리더의 재료가 낡았거나, <b>어느 노드든 조회를 상한으로 거절하는 중</b>이다. 거절당한 사람은
 * 생존 신호를 못 갱신해, 그동안 걷으면 서버가 시킨 시각에 성실히 온 사람이 줄을 잃는다.
 */
class SweepHoldWiringTest {

    private final ControlPlaneConfig 배선 = new ControlPlaneConfig();

    private final MutableClock 시계 = MutableClock.at(Instant.ofEpochSecond(1_000));

    private final SnapshotHolder holder = SnapshotHolder.of(Duration.ofSeconds(3),
            Duration.ofSeconds(5), 시계);

    private final GatewayRegistry 등록부 = GatewayRegistry.of(3, 1);

    @Test
    @DisplayName("재료가_신선해도_거절_중이면_청소를_멈춘다")
    void 재료가_신선해도_거절_중이면_청소를_멈춘다() {
        holder.replace(new GatewaySnapshot(Map.of(), new SnapshotMeta(10, 1), 시계.instant()));
        등록부.pollRejectionObserved(1);

        assertThat(배선.sweepHeld(holder, 등록부).getAsBoolean()).isTrue();
    }

    @Test
    @DisplayName("재료가_신선하고_거절이_없으면_돈다")
    void 재료가_신선하고_거절이_없으면_돈다() {
        holder.replace(new GatewaySnapshot(Map.of(), new SnapshotMeta(10, 1), 시계.instant()));

        assertThat(배선.sweepHeld(holder, 등록부).getAsBoolean()).isFalse();
    }

    /** 원래 조건을 잃지 않는다. 회차마다 새로 읽는지도 본다 — 한 번 떠 두면 기동 때 값에 굳는다. */
    @Test
    @DisplayName("재료가_낡으면_거절이_없어도_멈춘다")
    void 재료가_낡으면_거절이_없어도_멈춘다() {
        holder.replace(new GatewaySnapshot(Map.of(), new SnapshotMeta(10, 1), 시계.instant()));
        var 멈추나 = 배선.sweepHeld(holder, 등록부);
        assertThat(멈추나.getAsBoolean()).isFalse();

        시계.앞으로(Duration.ofSeconds(6));

        assertThat(멈추나.getAsBoolean()).isTrue();
    }
}

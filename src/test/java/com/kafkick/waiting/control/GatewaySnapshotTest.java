package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.routing.InstanceAddress;
import com.kafkick.waiting.domain.routing.InstanceRouting;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 분모를 올린 사본은 분모 말고는 아무것도 안 잃는다. 돌아온 노드가 그 틱에 라우팅 목록을 잃으면 안 된다. */
class GatewaySnapshotTest {

    private static final Instant 발행_시각 = Instant.parse("2026-09-24T00:00:00Z");

    @Test
    @DisplayName("분모를_올려도_쿠폰과_라우팅_목록을_지킨다")
    void 분모를_올려도_쿠폰과_라우팅_목록을_지킨다() {
        List<InstanceRouting> 목록 = List.of(new InstanceRouting("be-1",
                InstanceAddress.parse("be-1.internal:9000").orElseThrow(), 30));
        GatewaySnapshot 받은_것 = new GatewaySnapshot(Map.of("c1", CouponStates.idle(500)),
                new SnapshotMeta(1000, 2), 발행_시각, 목록);

        assertThat(받은_것.withGatewayCountAtLeast(3)).isEqualTo(new GatewaySnapshot(
                Map.of("c1", CouponStates.idle(500)), new SnapshotMeta(1000, 3), 발행_시각, 목록));
        assertThat(받은_것.withGatewayCountAtLeast(2)).isSameAs(받은_것);
    }
}

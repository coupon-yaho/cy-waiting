package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.coupon.Tunables;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 운영자가 고친 값이 <b>배포 없이</b> 전 노드에 닿는가 (P-1 · 6.8.2).
 *
 * <p>장애 중에 되돌릴 수 있어야 롤백 전략이 성립합니다. 스냅샷은 이미 매 틱 전
 * 노드에 닿으므로, 설정 서버를 새로 붙이지 않고 그 경로에 실어 보냅니다.
 */
class TunablesTransportTest {

    private static final Instant 지금 = Instant.parse("2026-08-27T00:00:00Z");

    private final SnapshotCodec codec = SnapshotCodec.create();

    private Map<String, String> 실어_보낸다(Tunables 값) {
        return codec.encode(
                new GatewaySnapshot(Map.of("c1", CouponState.idle(100)),
                        SnapshotMeta.withoutPollScale(1_000, 1, 값), 지금),
                CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
    }

    /** 실은 값이 그대로 돌아와야 한다. 갈리면 리더가 정한 값과 노드가 보는 값이 다르다. */
    @Test
    @DisplayName("실은_값이_그대로_돌아온다")
    void 실은_값이_그대로_돌아온다() {
        Tunables 바꾼_값 = new Tunables(0.4, 7);

        GatewaySnapshot 읽은_것 = codec.decode(실어_보낸다(바꾼_값));

        assertThat(읽은_것.meta().tunables()).isEqualTo(바꾼_값);
    }

    /**
     * <b>안 실린 것과 기본값을 실은 것은 다릅니다.</b> 기본값으로 채워 보내면
     * 읽는 쪽이 그것을 "운영자가 정한 값" 으로 읽고 각 노드의 기동 설정을
     * 덮어씁니다 — 아무도 안 바꿨는데 값이 바뀝니다.
     */
    @Test
    @DisplayName("안_실으면_안_실린_채로_읽힌다")
    void 안_실으면_안_실린_채로_읽힌다() {
        Map<String, String> 실은_것 = codec.encode(
                new GatewaySnapshot(Map.of("c1", CouponState.idle(100)),
                        new SnapshotMeta(1_000, 1), 지금),
                CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());

        assertThat(실은_것).doesNotContainKey("#tunables");
        assertThat(codec.decode(실은_것).meta().tunables()).isNull();
    }

    /**
     * <b>오타 하나가 판정을 멈추면 안 됩니다.</b> 장애 중에 손으로 넣는 값이라,
     * 그때 기동이 막히면 되돌릴 수단 자체가 사라집니다.
     */
    @Test
    @DisplayName("깨진_값이_실려_와도_판정이_안_멈춘다")
    void 깨진_값이_실려_와도_판정이_안_멈춘다() {
        Map<String, String> 깨진_것 = 실어_보낸다(new Tunables(0.4, 7));
        깨진_것.put("#tunables", "{이건 JSON 이 아니다");

        Tunables 읽은_것 = codec.decode(깨진_것).meta().tunables();

        assertThat(읽은_것).isEqualTo(Tunables.defaults());
    }

    /** 한 값이 깨져도 나머지는 살아야 한다. 오타 하나가 방금 고친 값을 되돌린다. */
    @Test
    @DisplayName("한_값만_깨지면_나머지는_산다")
    void 한_값만_깨지면_나머지는_산다() {
        Map<String, String> 반쯤_깨진_것 = 실어_보낸다(new Tunables(0.4, 7));
        반쯤_깨진_것.put("#tunables", "{\"idleCreditRatio\":0.4,\"inFlightSeconds\":8oops}");

        Tunables 읽은_것 = codec.decode(반쯤_깨진_것).meta().tunables();

        assertThat(읽은_것.idleCreditRatio()).as("멀쩡한 값").isEqualTo(0.4);
        assertThat(읽은_것.inFlightSeconds()).as("깨진 값")
                .isEqualTo(Tunables.defaults().inFlightSeconds());
    }
}

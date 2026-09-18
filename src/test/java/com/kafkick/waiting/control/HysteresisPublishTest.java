package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.QueueMode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 발행이 싣는 히스테리시스 (CY-963).
 *
 * <p>제품이 아직 히스테리시스를 안 돌려 발행은 빈 값을 싣는다. 돌리기 시작하면 여기가 매 틱 이월을
 * 지우는 자리라, 지금 무엇을 싣는지를 못 박아 그날 이 시험이 먼저 빨개지게 한다.
 */
class HysteresisPublishTest {

    private static final Instant 지금 = Instant.parse("2026-09-18T00:00:00Z");

    private final AtomicReference<Map<String, String>> 발행된_것 = new AtomicReference<>();

    private final SnapshotCodec codec = SnapshotCodec.create();

    private AllocationRound 회차(List<CouponDemand> 수요) {
        return AllocationRound.of(() -> true,
                () -> Mono.just(new TimedDemands(수요, 지금.getEpochSecond())),
                () -> 1_000, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행된_것.set(hash);
                    return Mono.empty();
                },
                () -> 지금,
                () -> Mono.just(CreditSmoother.restore(0.3, CreditSmoother.Snapshot.empty())),
                codec, () -> 0);
    }

    /**
     * <b>지금은 빈 값이다.</b> 히스테리시스를 배선하는 날 발행 쪽을 같이 안 고치면 스냅샷이 매 틱 이월을
     * 덮어써, 승계마다 큐 켜짐꺼짐이 다시 처음부터 선다.
     */
    @Test
    @DisplayName("발행은_아직_빈_히스테리시스를_싣는다")
    void 발행은_아직_빈_히스테리시스를_싣는다() {
        AllocationRound round = 회차(List.of(
                new CouponDemand("c1", 100, 1_000_000, QueueMode.ADAPTIVE)));

        round.run().block();

        assertThat(codec.hysteresis(발행된_것.get()))
                .as("제품이 히스테리시스를 돌리기 시작하면 이 자리를 같이 고친다")
                .isEqualTo(QueueingHysteresis.Snapshot.empty());
    }
}

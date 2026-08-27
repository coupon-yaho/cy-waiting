package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 나눠 준 합이 가진 것을 넘었는가 (6.9.1).
 *
 * <p><b>초과 발급의 선행 지표입니다</b> (불변식 2). 재고를 가진 것은 발급 계층이라
 * 게이트웨이는 초과 발급 자체를 못 봅니다 — 스스로 계산한 이 값이 오르면 원인이
 * 배분에 있습니다.
 */
class OverAllocationTest {

    private static final Instant 지금 = Instant.parse("2026-08-27T00:00:00Z");

    private final AtomicReference<Map<String, String>> 발행된_것 = new AtomicReference<>();

    private AllocationRound 판(long 크레딧, List<CouponDemand> 수요) {
        return AllocationRound.of(() -> true,
                () -> Mono.just(new TimedDemands(수요, 지금.getEpochSecond())),
                () -> 크레딧, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행된_것.set(hash);
                    return Mono.empty();
                },
                () -> 지금,
                () -> Mono.just(CreditSmoother.restore(1.0, CreditSmoother.Snapshot.empty())),
                SnapshotCodec.create(), () -> 0);
    }

    /** 정상 판에서는 0 이다. 0 이 아닌 것을 잡으려면 0 이 정상임을 먼저 못 박는다. */
    @Test
    @DisplayName("정상_배분에서는_초과가_0_이다")
    void 정상_배분에서는_초과가_0_이다() {
        AllocationRound round = 판(1_000, List.of(
                new CouponDemand("c1", 100, 1_000_000, QueueMode.ADAPTIVE),
                new CouponDemand("c2", 100, 1_000_000, QueueMode.ADAPTIVE)));

        round.run().block();

        assertThat(round.lastOverAllocated()).isZero();
    }

    /**
     * <b>수요가 없으면 나눠 줄 것도 없습니다.</b> 이 경우에도 0 이어야 하고,
     * 안 그러면 빈 판마다 알람이 울립니다.
     */
    @Test
    @DisplayName("수요가_없어도_초과가_0_이다")
    void 수요가_없어도_초과가_0_이다() {
        AllocationRound round = 판(1_000, List.of());

        round.run().block();

        assertThat(round.lastOverAllocated()).isZero();
    }

    /**
     * <b>크레딧이 0 이면 아무에게도 못 줍니다.</b> 여기서 뭐라도 나가면 그것이
     * 곧 초과입니다.
     */
    @Test
    @DisplayName("크레딧이_0_이면_아무도_못_받는다")
    void 크레딧이_0_이면_아무도_못_받는다() {
        AllocationRound round = 판(0, List.of(
                new CouponDemand("c1", 0, 1_000_000, QueueMode.ADAPTIVE)));

        round.run().block();

        assertThat(round.lastOverAllocated()).isZero();
    }
}

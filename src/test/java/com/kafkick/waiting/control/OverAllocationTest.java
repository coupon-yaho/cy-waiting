package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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

    /** 뒷단이 받는다고 한 양. 평활과 하한이 이 값과 벌어지는 것이 선행 지표다. */
    private final AtomicLong 관측 = new AtomicLong();

    /** 판 사이에 이월되는 평활 상태. 매번 새로 시작하면 지연이 안 쌓인다. */
    private final AtomicReference<CreditSmoother.Snapshot> 평활 =
            new AtomicReference<>(CreditSmoother.Snapshot.empty());

    private AllocationRound 판(long 크레딧, List<CouponDemand> 수요) {
        관측.set(크레딧);
        return AllocationRound.of(() -> true,
                () -> Mono.just(new TimedDemands(수요, 지금.getEpochSecond())),
                관측::get, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행된_것.set(hash);
                    평활.set(SnapshotCodec.create().smoothing(hash));
                    return Mono.empty();
                },
                () -> 지금,
                // **운영과 같은 평활 계수다.** 1.0 이면 평활이 없어서 이 지표가
                // 재려는 지연 자체가 안 생긴다.
                () -> Mono.just(CreditSmoother.restore(0.3, 평활.get())),
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

        assertThat(round.budgetOvershoot()).isZero();
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

        assertThat(round.budgetOvershoot()).isZero();
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

        assertThat(round.budgetOvershoot()).isZero();
    }

    /**
     * <b>평활이 만드는 초과가 이 지표의 본론입니다.</b> 뒷단이 못 받는다고 해도
     * 평활은 열 틱 넘게 옛 값을 나눠 줍니다 — 그 구간이 초과 발급 직전입니다.
     */
    @Test
    @DisplayName("뒷단이_줄었는데_평활이_옛_값을_나눠_주면_센다")
    void 뒷단이_줄었는데_평활이_옛_값을_나눠_주면_센다() {
        AllocationRound round = 판(10_000, List.of(
                new CouponDemand("c1", 100_000, 1_000_000, QueueMode.ADAPTIVE)));
        round.run().block();
        assertThat(round.budgetOvershoot()).as("첫 판은 관측과 같다").isZero();

        // 뒷단 여덟 대가 죽었다. 평활은 한 틱에 다 안 따라온다.
        관측.set(1_000);
        round.run().block();

        assertThat(round.budgetOvershoot())
                .as("나눠 준 예산이 관측을 넘은 양")
                .isPositive();
    }

    /** 관측이 안 줄면 초과도 없다. 늘 오르는 값이면 알람이 아무것도 안 말한다. */
    @Test
    @DisplayName("관측이_그대로면_초과가_안_오른다")
    void 관측이_그대로면_초과가_안_오른다() {
        AllocationRound round = 판(1_000, List.of(
                new CouponDemand("c1", 100, 1_000_000, QueueMode.ADAPTIVE)));

        round.run().block();
        round.run().block();

        assertThat(round.budgetOvershoot()).isZero();
    }

    /**
     * <b>누적입니다.</b> 마지막 틱의 값을 내면 리더십을 잃는 순간 그 값이 굳고,
     * 15초 스크레이프가 1초짜리 사건을 대부분 놓칩니다.
     */
    @Test
    @DisplayName("초과가_누적된다")
    void 초과가_누적된다() {
        AllocationRound round = 판(10_000, List.of(
                new CouponDemand("c1", 100_000, 1_000_000, QueueMode.ADAPTIVE)));
        round.run().block();

        관측.set(1_000);
        round.run().block();
        double 한_번 = round.budgetOvershoot();
        round.run().block();

        assertThat(round.budgetOvershoot()).isGreaterThan(한_번);
    }
}

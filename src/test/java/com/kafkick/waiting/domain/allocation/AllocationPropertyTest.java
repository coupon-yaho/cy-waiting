package com.kafkick.waiting.domain.allocation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 배분 총합은 전역 크레딧을 넘지 않는다 — 타협 불가 기준이다.
 *
 * <p>예시 몇 개로는 못 잡는다. 두 패스에 걸친 정수 나눗셈은 특정
 * {@code (credit, 쿠폰 수, 요구량 분포)} 조합에서만 어긋난다.
 */
class AllocationPropertyTest {

    private static final long SEED = 20260819L;
    private static final int TRIALS = 100_000;

    @Test
    @DisplayName("무작위_10만회에서_배분_총합이_전역_크레딧을_넘지_않는다")
    void 무작위_10만회에서_배분_총합이_전역_크레딧을_넘지_않는다() {
        Random rnd = new Random(SEED);
        int violations = 0;

        for (int t = 0; t < TRIALS; t++) {
            long credit = rnd.nextLong(0, 200_000);
            List<CouponDemand> demands = randomDemands(rnd);

            long total = FairShareAllocator.allocate(credit, demands).stream()
                    .mapToLong(Grant::credit).sum();

            if (total > credit) {
                violations++;
            }
        }

        assertThat(violations)
                .withFailMessage("초과 배분 %d 건 (시드 %d)", violations, SEED)
                .isZero();
    }

    @Test
    @DisplayName("무작위_10만회에서_아무도_요구량보다_많이_받지_않는다")
    void 무작위_10만회에서_아무도_요구량보다_많이_받지_않는다() {
        // 넘지 않는 것만 보면 부족하다. 한 쿠폰에 몰아줘도 총합은 안 넘는다.
        Random rnd = new Random(SEED);
        int violations = 0;

        for (int t = 0; t < TRIALS; t++) {
            long credit = rnd.nextLong(0, 200_000);
            List<CouponDemand> demands = randomDemands(rnd);

            List<Grant> grants = FairShareAllocator.allocate(credit, demands);
            for (Grant g : grants) {
                long want = demands.stream()
                        .filter(d -> d.couponId().equals(g.couponId()))
                        .mapToLong(CouponDemand::want).findFirst().orElse(0);
                if (g.credit() > want) {
                    violations++;
                }
            }
        }

        assertThat(violations)
                .withFailMessage("요구량 초과 %d 건 (시드 %d)", violations, SEED)
                .isZero();
    }

    @Test
    @DisplayName("무작위_10만회에서_줄_수_있는_몫을_남기지_않는다")
    void 무작위_10만회에서_줄_수_있는_몫을_남기지_않는다() {
        // 유휴 낭비 0. 남은 크레딧이 굶주린 쿠폰 수보다 많으면 더 줄 수 있었다는
        // 뜻이다 — 그만큼은 대기자가 이유 없이 기다린 시간이다.
        Random rnd = new Random(SEED);
        int wasted = 0;

        for (int t = 0; t < TRIALS; t++) {
            long credit = rnd.nextLong(0, 200_000);
            List<CouponDemand> demands = randomDemands(rnd);

            List<Grant> grants = FairShareAllocator.allocate(credit, demands);
            long total = grants.stream().mapToLong(Grant::credit).sum();
            long stillHungry = grants.stream()
                    .filter(g -> g.credit() < wantOf(demands, g.couponId()))
                    .count();

            // 굶주린 쿠폰 수만큼 남았다면 각자 1 씩 더 줄 수 있었다는 뜻이다.
            if (stillHungry > 0 && credit - total >= stillHungry) {
                wasted++;
            }
        }

        assertThat(wasted)
                .withFailMessage("줄 수 있었는데 남긴 경우 %d 건 (시드 %d)", wasted, SEED)
                .isZero();
    }

    private static long wantOf(List<CouponDemand> demands, String couponId) {
        return demands.stream()
                .filter(d -> d.couponId().equals(couponId))
                .mapToLong(CouponDemand::want).findFirst().orElse(0);
    }

    private static List<CouponDemand> randomDemands(Random rnd) {
        int n = rnd.nextInt(1, 40);
        List<CouponDemand> demands = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            demands.add(new CouponDemand(
                    "c" + i, rnd.nextLong(0, 200_000), rnd.nextLong(0, 200_000)));
        }
        return demands;
    }
}

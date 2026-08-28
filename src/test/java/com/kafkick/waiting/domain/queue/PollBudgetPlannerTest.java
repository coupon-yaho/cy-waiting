package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 폴링 부하를 큐를 훑지 않고 닫힌 식으로 구한다.
 *
 * <p>20,000명을 세면 그 계산 자체가 부하다. 배수율을 알면 각 밴드에 몇 명이
 * 있는지는 곱셈 몇 번으로 나온다.
 */
class PollBudgetPlannerTest {

    @Test
    @DisplayName("큐를_훑지_않고_닫힌_식으로_예상_폴링을_구한다")
    void 큐를_훑지_않고_닫힌_식으로_예상_폴링을_구한다() {
        // 배수 10/s · 1000명. 앞 50명은 5초 안(1s 간격), 다음 250명은
        // 30초 안(3s), 다음 900명 중 남은 700명은 120초 안(10s).
        // 50/1 + 250/3 + 700/10 = 50 + 83.33 + 70 = 203.33
        assertThat(PollBudgetPlanner.expectedPollRps(1000, 10))
                .isCloseTo(203.33, within(0.1));
    }

    @Test
    @DisplayName("배수율이_0이면_전원이_가장_먼_밴드다")
    void 배수율이_0이면_전원이_가장_먼_밴드다() {
        // 안 빠지는 줄은 전원의 ETA 가 무한이다.
        assertThat(PollBudgetPlanner.expectedPollRps(100_000, 0))
                .isCloseTo(100_000 / 30.0, within(0.01));
    }

    @Test
    @DisplayName("대기자가_없으면_폴링도_없다")
    void 대기자가_없으면_폴링도_없다() {
        assertThat(PollBudgetPlanner.expectedPollRps(0, 10)).isZero();
    }

    @Test
    @DisplayName("배수가_빨라_전원이_첫_밴드면_인원만큼_폴링한다")
    void 배수가_빨라_전원이_첫_밴드면_인원만큼_폴링한다() {
        // 100명 · 배수 1000/s → 전원 0.1초 안. 1초 간격이 하한이다.
        assertThat(PollBudgetPlanner.expectedPollRps(100, 1000))
                .isCloseTo(100, within(0.01));
    }

    @Test
    @DisplayName("예산이_남아도_배수는_1_미만으로_내려가지_않는다")
    void 예산이_남아도_배수는_1_미만으로_내려가지_않는다() {
        // 한산할 때 오히려 부하를 만들지 않는다.
        assertThat(PollBudgetPlanner.pollScale(10, 4000)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("예산을_넘으면_넘은_비율만큼_간격을_늘린다")
    void 예산을_넘으면_넘은_비율만큼_간격을_늘린다() {
        assertThat(PollBudgetPlanner.pollScale(8000, 4000)).isEqualTo(2.0);
    }

    @Test
    @DisplayName("예산이_0이하면_배수를_1로_둔다")
    void 예산이_0이하면_배수를_1로_둔다() {
        // 0 으로 나누면 무한이 되어 아무도 폴링을 못 한다.
        assertThat(PollBudgetPlanner.pollScale(8000, 0)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("매진_쿠폰의_대기자는_전역_폴링_예산에_들어가지_않는다")
    void 매진_쿠폰의_대기자는_전역_폴링_예산에_들어가지_않는다() {
        // 죽은 큐가 살아 있는 쿠폰의 폴링 간격을 늘리면, 배분에서 막아 둔
        // 기아가 폴링 경로로 되살아난다.
        List<CouponDemand> demands = List.of(
                new CouponDemand("soldout", 100_000, 0),
                new CouponDemand("live", 1000, 10_000));

        double withDead = PollBudgetPlanner.expectedPollRps(demands, id -> 10);

        assertThat(withDead).isCloseTo(PollBudgetPlanner.expectedPollRps(1000, 10), within(0.1));
    }

    /**
     * <b>설계점에서 예산이 실제로 지켜진다.</b>
     *
     * <p>배수를 값으로 못 박기만 하면 그 배수가 간격에 닿아 부하를 실제로
     * 줄이는지는 안 재진다. 상한 60초에 잘려 예산의 여덟 배가 나가는 구간이
     * 있는데, 그 구간에서만 재면 기구가 도는 것처럼 보인다.
     */
    @Test
    @DisplayName("설계_규모에서_배수를_지키면_예산_안에_든다")
    void 설계_규모에서_배수를_지키면_예산_안에_든다() {
        // R4 의 설계점 — 노드 20 대, 동시 대기 20,000, 초당 배수 4,000.
        long 대기 = 20_000;
        double 배수율 = 4_000;
        double 예산 = PollBudgetPlanner.budgetRps(20);

        double 예상 = PollBudgetPlanner.expectedPollRps(대기, 배수율);
        double 배수 = PollBudgetPlanner.pollScale(예상, 예산);
        // 전원이 5초 안이라 1초 밴드다. 배수만큼 곧장 늘어난다. 줄 한가운데
        // 사람으로 잰다 — 맨 끝은 ETA 가 정확히 경계라 한 밴드 위로 넘어가고,
        // 그 사람은 더 느리게 물으므로 아래 부등식을 느슨하게 만든다.
        PollIntervalPolicy 정책 = PollIntervalPolicy.of(0);
        long 간격 = 정책.intervalSec(대기 / 2 / 배수율, () -> 0.5, 배수);

        assertThat(배수).as("예산 초과 배수").isEqualTo(5.0);
        assertThat(간격).as("그 사람이 받는 간격").isEqualTo(5);
        // **여기가 요점이다.** 배수가 간격에 닿아 실제 부하가 예산 안으로 든다.
        assertThat((double) 대기 / 간격).as("실측 폴링").isLessThanOrEqualTo(예산);
    }

    /**
     * <b>상한 60초가 배수의 실효 천장이다.</b>
     *
     * <p>가장 먼 밴드에서는 배수 2 를 넘는 순간 전부 같은 값이 된다. 배수가
     * 17 이어도 실제 간격은 60초고, 그 위는 증설로만 해결된다 — 계획서에
     * 이 경계를 안 적으면 다음 사람이 배수를 올려 해결하려 든다.
     */
    @Test
    @DisplayName("가장_먼_밴드에서는_배수가_상한에_잘린다")
    void 가장_먼_밴드에서는_배수가_상한에_잘린다() {
        PollIntervalPolicy 정책 = PollIntervalPolicy.of(0);
        long 먼_밴드_eta = 1_000;

        assertThat(정책.intervalSec(먼_밴드_eta, () -> 0.5, 2.0)).isEqualTo(60);
        assertThat(정책.intervalSec(먼_밴드_eta, () -> 0.5, 17.58)).isEqualTo(60);
    }

    @Test
    @DisplayName("살아_있는_쿠폰이_여럿이면_합산한다")
    void 살아_있는_쿠폰이_여럿이면_합산한다() {
        List<CouponDemand> demands = List.of(
                new CouponDemand("a", 1000, 10_000),
                new CouponDemand("b", 1000, 10_000));

        assertThat(PollBudgetPlanner.expectedPollRps(demands, id -> 10))
                .isCloseTo(2 * PollBudgetPlanner.expectedPollRps(1000, 10), within(0.1));
    }

    @Test
    @DisplayName("줄이_길고_배수가_느리면_네_밴드가_모두_찬다")
    void 줄이_길고_배수가_느리면_네_밴드가_모두_찬다() {
        // 배수 1/s · 1000명. 앞 5명 1초 밴드, 25명 3초, 90명 10초,
        // 남은 880명이 30초 밴드다. 5 + 8.33 + 9 + 29.33 = 51.67
        // 이 경우가 없으면 마지막 밴드로 넘어가는 경로가 한 번도 안 돈다.
        assertThat(PollBudgetPlanner.expectedPollRps(1000, 1))
                .isCloseTo(51.67, within(0.1));
    }

    @Test
    @DisplayName("배수가_아주_느려도_맨_앞사람은_첫_밴드다")
    void 배수가_아주_느려도_맨_앞사람은_첫_밴드다() {
        // 반올림하면 이 사람이 먼 밴드로 밀려 예산을 과소 추정하고,
        // pollScale 이 안 올라 실제 부하가 예산을 넘는다.
        assertThat(PollBudgetPlanner.expectedPollRps(1, 0.02)).isCloseTo(1.0, within(0.001));
    }
}

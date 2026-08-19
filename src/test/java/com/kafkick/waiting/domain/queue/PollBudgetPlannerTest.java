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
}

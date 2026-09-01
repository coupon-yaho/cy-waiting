package com.kafkick.waiting.chaos;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.queue.PollBudgetPlanner;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.DoubleSupplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * C19 — 전역 폴링 배수가 한 틱에 떨어진다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C19 절이 든다. 여기는
 * 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class PollScaleDropScenarioTest {

    /** 노드 하나. 노드당 예산이 200 이라 이 값이 곧 전역 예산이다. */
    private static final int 노드 = 1;

    private static final double 예산 = 200;

    /** 배수를 올리는 큰 쿠폰. 2만 명이 초당 50 씩 빠진다. */
    private static final String A = "c19-big";

    private static final long A_대기 = 20_000;

    private static final double A_배분 = 50;

    /** 배수에 같이 휩쓸리는 작은 쿠폰. 이쪽 대기자가 무엇을 받는지가 관심사다. */
    private static final String B = "c19-small";

    private static final long B_대기 = 200;

    private static final double B_배분 = 5;

    /** 지터 폭. 천장이 60/(1+폭) 이라 이 값이 먼 밴드의 상한을 정한다. */
    private static final double 지터 = 0.2;

    /** 흔들림을 죽인다. 배수의 효과만 보려면 지터가 상수여야 한다. */
    private static final DoubleSupplier 흔들지_않는다 = () -> 0.5;

    /**
     * 손으로 잰 기대값이다 — 프로덕션 식을 베끼면 상수를 바꿔도 같이 움직여
     * 아무것도 안 잰다.
     *
     * <p>둘을 합치면 초당 1,655 건이고 예산이 200 이라 배수가 8.275 다.
     */
    private static final double 배수 = 8.275;

    /** 배수가 걸린 뒤 가장 가까운 밴드(1초)가 받는 간격. 1 × 8.275 를 반올림한다. */
    private static final long 가까운_밴드 = 8;

    /**
     * 가장 먼 밴드(30초)가 받는 간격. <b>30 × 8.275 = 248 이 아니다</b> —
     * 천장 50 에 막힌다.
     */
    private static final long 먼_밴드 = 50;

    private final PollIntervalPolicy 폴링 = PollIntervalPolicy.of(지터);

    private double 배수를_낸다(List<CouponDemand> 수요) {
        Map<String, Double> 배분 = Map.of(A, A_배분, B, B_배분);
        double 기대 = PollBudgetPlanner.expectedPollRps(
                수요, couponId -> 배분.getOrDefault(couponId, 0.0));
        return PollBudgetPlanner.pollScale(기대, PollBudgetPlanner.budgetRps(노드));
    }

    private long 간격(double etaSec, double scale) {
        return 폴링.intervalSec(etaSec, 흔들지_않는다, scale);
    }

    /**
     * 세 구간을 한 판정으로 잇는다.
     *
     * <p>배수는 매 틱 처음부터 다시 계산된다. 그래서 진입도 회복도 한 틱이고,
     * 이 시나리오가 재는 것은 <b>그 한 틱이 대기자에게 무엇을 하는가</b>다.
     */
    @Test
    @DisplayName("C19_큰_쿠폰이_매진되면_폴링_배수가_한_틱에_풀린다")
    void C19_큰_쿠폰이_매진되면_폴링_배수가_한_틱에_풀린다() {
        double[] 정상 = new double[1];
        double[] 걸린_배수 = new double[1];
        long[] 정상_간격 = new long[2];
        long[] 걸린_간격 = new long[2];
        double[] 회복_배수 = new double[1];
        long[] 회복_간격 = new long[2];

        ChaosScenario.named("C19 전역 폴링 배수 하강")
                .baseline(() -> {
                    // 작은 쿠폰만 있으면 예산 안이라 배수가 안 걸린다.
                    정상[0] = 배수를_낸다(List.of(new CouponDemand(B, B_대기, B_대기)));
                    정상_간격[0] = 간격(1, 정상[0]);
                    정상_간격[1] = 간격(300, 정상[0]);
                })
                .inject(() -> 걸린_배수[0] = 배수를_낸다(List.of(
                        new CouponDemand(A, A_대기, A_대기),
                        new CouponDemand(B, B_대기, B_대기))))
                .duringFault(() -> {
                    걸린_간격[0] = 간격(1, 걸린_배수[0]);
                    걸린_간격[1] = 간격(300, 걸린_배수[0]);
                })
                // 재고가 0 이면 요구량이 0 이라 예산 계산에서 빠진다. 한 틱이다.
                .recover(() -> 회복_배수[0] = 배수를_낸다(List.of(
                        new CouponDemand(A, A_대기, 0),
                        new CouponDemand(B, B_대기, B_대기))))
                .afterRecovery(() -> {
                    회복_간격[0] = 간격(1, 회복_배수[0]);
                    회복_간격[1] = 간격(300, 회복_배수[0]);
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 전제 — 평시에 배수가 안 걸려 있었나. 걸려 있었으면 아래가
                        // 무엇 때문에 오른 것인지 모른다.
                        평시에_배수가_안_걸렸다(정상[0]),
                        평시_간격이_밴드_그대로다(정상_간격[0], 정상_간격[1]),
                        // 계획서가 요구하는 진입 조건이다. 5 미만이면 이 시나리오가
                        // 재려는 구간에 아예 못 들어간다.
                        배수가_다섯을_넘는다(걸린_배수[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 가까운 밴드는 배수를 그대로 받는다.
                        가까운_밴드가_배수를_받는다(걸린_간격[0]),
                        // **먼 밴드는 천장에 막힌다.** 배수를 그대로 받으면 248초인데
                        // 50 에서 잘린다 — 의도된 동작이고, 그 대가는 계획서에 있다.
                        먼_밴드가_천장에_막힌다(걸린_간격[1])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        // 매진된 쿠폰의 줄은 예산에서 빠진다. 안 빠지면 죽은 큐가
                        // 예산을 먹고 산 쿠폰의 폴링이 영영 뜸해진다.
                        매진되면_배수가_풀린다(회복_배수[0]),
                        // 간격이 밴드로 돌아온다.
                        평시_간격이_밴드_그대로다(회복_간격[0], 회복_간격[1])))
                // **RC1~RC6 은 여기서 안 잰다.** 줄도 뒷단 유입도 안 만든다. 하강이
                // 만드는 회복 버스트는 열린 루프가 있어야 재진다 (CY-817).
                .run();
    }

    private Optional<String> 평시에_배수가_안_걸렸다(double 정상) {
        return 정상 == 1.0 ? Optional.empty()
                : Optional.of("전제 — 평시 배수가 %.3f 다. 1.0 이 아니면 작은 쿠폰만으로도 "
                        .formatted(정상) + "예산을 넘긴 것이라 비교 기준이 없다");
    }

    private Optional<String> 평시_간격이_밴드_그대로다(long 가까운, long 먼) {
        if (가까운 != 1) {
            return Optional.of("가장 가까운 밴드가 %d초다 — 1초여야 한다".formatted(가까운));
        }
        return 먼 == 30 ? Optional.empty()
                : Optional.of("가장 먼 밴드가 %d초다 — 30초여야 한다".formatted(먼));
    }

    private Optional<String> 배수가_다섯을_넘는다(double 걸린) {
        if (Math.abs(걸린 - 배수) > 0.001) {
            return Optional.of("배수가 %.3f 다 — %.3f 여야 한다".formatted(걸린, 배수));
        }
        return 걸린 >= 5 ? Optional.empty()
                : Optional.of("배수가 %.3f 라 계획서의 진입 조건(5 이상)에 못 든다"
                        .formatted(걸린));
    }

    private Optional<String> 가까운_밴드가_배수를_받는다(long 간격) {
        return 간격 == 가까운_밴드 ? Optional.empty()
                : Optional.of("가까운 밴드가 %d초다 — 1초 × 배수 = %d초여야 한다"
                        .formatted(간격, 가까운_밴드));
    }

    /**
     * 먼 밴드는 배수를 다 못 받는다. 천장이 그 위에 있기 때문이다.
     *
     * <p>범위가 아니라 값으로 본다. "늘었다" 만 보면 천장을 걷어내도 통과한다.
     */
    private Optional<String> 먼_밴드가_천장에_막힌다(long 간격) {
        return 간격 == 먼_밴드 ? Optional.empty()
                : Optional.of("먼 밴드가 %d초다 — 천장 %d초에 막혀야 한다"
                        .formatted(간격, 먼_밴드));
    }

    private Optional<String> 매진되면_배수가_풀린다(double 회복) {
        return 회복 == 1.0 ? Optional.empty()
                : Optional.of("매진 뒤 배수가 %.3f 다 — 죽은 큐가 예산을 먹고 있다"
                        .formatted(회복));
    }
}

package com.kafkick.waiting.chaos;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollBudgetPlanner;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.DoubleSupplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * C19 — 전역 폴링 배수가 걸리고 한 틱에 풀린다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C19 절이 든다. 여기는
 * 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class PollScaleDropScenarioTest {

    /**
     * 노드 하나. 예산이 200 이고 그것이 곧 전역 예산이다.
     *
     * <p><b>진입 조건을 만드는 것이 부하가 아니라 이 값이다.</b> 같은 줄을 설계
     * 규모(20대)에 두면 예산이 4,000 이라 배수가 아예 안 걸린다 — 이 시나리오가
     * 걷는 것은 축소·열화 배포다.
     */
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

    /** <b>배선값을 쓴다.</b> 리터럴로 두면 배선이 바뀌어도 이 시나리오가 안 문다. */
    private static final double 지터 = PollIntervalPolicy.NORMAL_JITTER_RATIO;

    /**
     * 손으로 잰 기대값이다 — 프로덕션 식을 베끼면 상수를 바꿔도 같이 움직여
     * 아무것도 안 잰다. 둘을 합치면 초당 1,655 건이고 예산이 200 이다.
     */
    private static final double 배수 = 8.275;

    /**
     * 폴링 간격의 천장. <b>지터 전 값이다</b> — 60/(1+0.2) 이라 흔들림의 위쪽 끝이
     * 60 에 닿는다. 개인이 받는 값은 이 값이 아니라 그 둘레로 흩어진다.
     */
    private static final long 천장 = 50;

    /**
     * 노드 하나가 예산 안에 담을 수 있는 대기자 수의 <b>상한</b>.
     *
     * <p>실제 용량은 이보다 작다 — 지터가 흔든 뒤의 평균 부하가 1/천장보다 크다.
     */
    private static final long 담을_수_있는_대기자 = (long) (예산 * 천장);

    /** 배수를 올려도 부하가 더 안 내려가는 것을 보려고 넣는 값. 실제의 1,200 배다. */
    private static final double 터무니없는_배수 = 10_000;

    private static final DoubleSupplier 흔들지_않는다 = () -> 0.5;

    private final PollIntervalPolicy 폴링 = PollIntervalPolicy.of(지터);

    private double 배수를_낸다(List<CouponDemand> 수요) {
        Map<String, Double> 배분 = Map.of(A, A_배분, B, B_배분);
        double 기대 = PollBudgetPlanner.expectedPollRps(
                수요, couponId -> 배분.getOrDefault(couponId, 0.0));
        return PollBudgetPlanner.pollScale(기대, PollBudgetPlanner.budgetRps(노드));
    }

    /** 순번으로 간격을 낸다. ETA 를 손으로 주면 그 줄에 없는 사람을 재게 된다. */
    private long 간격(long 순번, double 배분율, double scale) {
        return 폴링.intervalSec(EtaPolicy.etaSec(순번, 배분율), 흔들지_않는다, scale);
    }

    /** 줄 전체의 실측 부하. 대표 한 명으로는 지터가 부하에 주는 영향이 안 보인다. */
    private double 실측_부하(double scale) {
        Random 고정_씨앗 = new Random(42);
        double 합 = 0;
        for (long i = 0; i < A_대기; i++) {
            합 += 1.0 / 폴링.intervalSec(EtaPolicy.etaSec(i, A_배분), 고정_씨앗::nextDouble, scale);
        }
        for (long i = 0; i < B_대기; i++) {
            합 += 1.0 / 폴링.intervalSec(EtaPolicy.etaSec(i, B_배분), 고정_씨앗::nextDouble, scale);
        }
        return 합;
    }

    private long 먼_밴드_간격(double 난수, double scale) {
        return 폴링.intervalSec(EtaPolicy.etaSec(B_대기 - 1, B_배분), () -> 난수, scale);
    }

    /**
     * 세 구간을 한 판정으로 잇는다.
     *
     * <p>배수는 매 틱 처음부터 다시 계산된다. 그래서 유지 구간에서도 <b>다시
     * 계산해서</b> 같은 값이 나오는지 본다 — 진입의 값을 다시 포맷하면 그 구간이
     * 새로 재는 것이 없다.
     */
    @Test
    @DisplayName("C19_큰_쿠폰이_매진되면_폴링_배수가_한_틱에_풀린다")
    void C19_큰_쿠폰이_매진되면_폴링_배수가_한_틱에_풀린다() {
        double[] 정상 = new double[1];
        long[] 정상_간격 = new long[3];
        double[] 걸린_배수 = new double[2];
        long[] 걸린_간격 = new long[3];
        long[] 흔들림 = new long[3];
        double[] 부하 = new double[2];
        double[] 회복_배수 = new double[1];
        long[] 회복_간격 = new long[3];

        List<CouponDemand> 둘_다 = List.of(
                new CouponDemand(A, A_대기, A_대기), new CouponDemand(B, B_대기, B_대기));

        ChaosScenario.named("C19 전역 폴링 배수 하강")
                .baseline(() -> {
                    정상[0] = 배수를_낸다(List.of(new CouponDemand(B, B_대기, B_대기)));
                    B_의_앞뒤를_잰다(정상_간격, 정상[0]);
                })
                .inject(() -> 걸린_배수[0] = 배수를_낸다(둘_다))
                .duringFault(() -> {
                    // **다시 계산한다.** 배수는 상태가 없어 매 틱 새로 나온다.
                    걸린_배수[1] = 배수를_낸다(둘_다);
                    B_의_앞뒤를_잰다(걸린_간격, 걸린_배수[1]);
                    흔들림[0] = 먼_밴드_간격(0, 걸린_배수[1]);
                    흔들림[1] = 먼_밴드_간격(0.5, 걸린_배수[1]);
                    흔들림[2] = 먼_밴드_간격(1, 걸린_배수[1]);
                    부하[0] = 실측_부하(걸린_배수[1]);
                    부하[1] = 실측_부하(터무니없는_배수);
                })
                .recover(() -> 회복_배수[0] = 배수를_낸다(List.of(
                        new CouponDemand(A, A_대기, 0), new CouponDemand(B, B_대기, B_대기))))
                .afterRecovery(() -> B_의_앞뒤를_잰다(회복_간격, 회복_배수[0]))
                .assertEntry(() -> RecoveryCriteria.violations(
                        평시에_배수가_안_걸렸다(정상[0]),
                        평시_간격이_밴드_그대로다(정상_간격),
                        배수가_예산_초과분만큼_오른다(걸린_배수[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        배수가_틱마다_같다(걸린_배수[0], 걸린_배수[1]),
                        // B 의 앞은 배수를 그대로 받고 꼬리는 천장에 붙는다.
                        걸린_간격이_밴드에_배수를_곱한_값이다(걸린_간격),
                        // 천장에 붙어도 흔들림이 죽지 않는다 — 천장이 있는 이유다.
                        천장에_붙어도_흔들린다(흔들림),
                        부하가_예산을_넘긴다(부하[0]),
                        부하가_천장이_정한_바닥에_멎는다(부하[0], 부하[1]),
                        노드_하나가_담을_수_있는_수를_넘었다(부하[1])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        매진되면_배수가_풀린다(회복_배수[0]),
                        평시_간격이_밴드_그대로다(회복_간격)))
                // **RC1~RC6 은 여기서 안 잰다.** 이 시나리오는 예산 계산과 폴링 정책만
                // 걷는다 — 줄도 뒷단 유입도 세우지 않는다.
                .run();
    }

    /** B 의 맨 앞·한가운데·맨 뒤. 셋이 서로 다른 밴드에 있어야 밴드표가 재진다. */
    private void B_의_앞뒤를_잰다(long[] 담을_곳, double scale) {
        담을_곳[0] = 간격(0, B_배분, scale);
        담을_곳[1] = 간격(100, B_배분, scale);
        담을_곳[2] = 간격(B_대기 - 1, B_배분, scale);
    }

    private Optional<String> 평시에_배수가_안_걸렸다(double 정상) {
        return 정상 == 1.0 ? Optional.empty()
                : Optional.of(("전제 — 평시 배수가 %.3f 다. 1.0 이 아니면 작은 쿠폰만으로도 "
                        + "예산을 넘긴 것이라 비교 기준이 없다").formatted(정상));
    }

    /** 순번 0·100·199 는 ETA 0·20·39.8 이라 밴드 1·3·10 초에 하나씩 걸린다. */
    private Optional<String> 평시_간격이_밴드_그대로다(long[] 간격) {
        long[] 기대 = {1, 3, 10};
        for (int i = 0; i < 기대.length; i++) {
            if (간격[i] != 기대[i]) {
                return Optional.of("배수 없는 간격이 %d초다 — %d초여야 한다"
                        .formatted(간격[i], 기대[i]));
            }
        }
        return Optional.empty();
    }

    private Optional<String> 배수가_예산_초과분만큼_오른다(double 걸린) {
        return Math.abs(걸린 - 배수) <= 0.001 ? Optional.empty()
                : Optional.of("배수가 %.3f 다 — %.3f 여야 한다".formatted(걸린, 배수));
    }

    private Optional<String> 배수가_틱마다_같다(double 진입, double 유지) {
        return 진입 == 유지 ? Optional.empty()
                : Optional.of("전제 — 같은 수요에서 배수가 %.3f 에서 %.3f 로 갈렸다"
                        .formatted(진입, 유지));
    }

    /** 앞의 둘은 밴드×배수를 그대로 받고, 꼬리는 82.75 가 아니라 천장이다. */
    private Optional<String> 걸린_간격이_밴드에_배수를_곱한_값이다(long[] 간격) {
        long[] 기대 = {8, 25, 천장};
        for (int i = 0; i < 기대.length; i++) {
            if (간격[i] != 기대[i]) {
                return Optional.of("배수 걸린 간격이 %d초다 — %d초여야 한다"
                        .formatted(간격[i], 기대[i]));
            }
        }
        return Optional.empty();
    }

    /** 천장에 붙은 밴드도 지터 폭만큼 흩어져야 한다. 40·50·60 이다. */
    private Optional<String> 천장에_붙어도_흔들린다(long[] 흔들림) {
        long[] 기대 = {40, 천장, 60};
        for (int i = 0; i < 기대.length; i++) {
            if (흔들림[i] != 기대[i]) {
                return Optional.of("천장에서 흔들림이 %d초다 — %d초여야 한다"
                        .formatted(흔들림[i], 기대[i]));
            }
        }
        return Optional.empty();
    }

    /**
     * 배수를 아무리 올려도 부하가 `대기자 / 천장` 언저리에서 멎는다.
     *
     * <p><b>정확히 그 값이 아니라 조금 위다.</b> 1/x 가 볼록해서 흔들린 간격의 평균
     * 부하가 1/천장보다 크다 — 그 폭이 5% 안이다.
     */
    private Optional<String> 부하가_천장이_정한_바닥에_멎는다(double 걸린, double 무한대) {
        double 바닥 = (double) (A_대기 + B_대기) / 천장;
        if (무한대 > 걸린) {
            return Optional.of("배수를 올렸는데 부하가 %.0f 에서 %.0f 로 늘었다"
                    .formatted(걸린, 무한대));
        }
        return Math.abs(무한대 - 바닥) / 바닥 <= 0.05 ? Optional.empty()
                : Optional.of("무한대 배수의 부하가 %.0f 다 — 대기자/천장 = %.0f 여야 한다"
                        .formatted(무한대, 바닥));
    }

    /** 이 줄은 노드 하나로 예산 안에 못 담는다. <b>배수가 아니라 규모의 문제다.</b> */
    private Optional<String> 노드_하나가_담을_수_있는_수를_넘었다(double 무한대) {
        if (A_대기 + B_대기 <= 담을_수_있는_대기자) {
            return Optional.of("전제 — 대기자 %d 명이 상한 %d 이하다. 이 픽스처로는 "
                    .formatted(A_대기 + B_대기, 담을_수_있는_대기자) + "규모 한계가 안 드러난다");
        }
        return 무한대 > 예산 ? Optional.empty()
                : Optional.of("대기자 %d 명인데 부하가 %.0f 로 예산 %.0f 안에 들어왔다"
                        .formatted(A_대기 + B_대기, 무한대, 예산));
    }

    /**
     * 배수가 걸린 뒤에도 부하가 예산 밖이다. 계획서가 값으로 적은 자리다.
     *
     * <p>밴드로 본다. 정확 일치는 지터 씨앗에 묶여 쓸모없이 깨진다.
     */
    private Optional<String> 부하가_예산을_넘긴다(double 걸린) {
        double 비 = 걸린 / 예산;
        return 비 >= 2.2 && 비 <= 2.4 ? Optional.empty()
                : Optional.of("배수 걸린 부하가 예산의 %.2f 배다 — 2.2~2.4 배여야 한다"
                        .formatted(비));
    }

    private Optional<String> 매진되면_배수가_풀린다(double 회복) {
        return 회복 == 1.0 ? Optional.empty()
                : Optional.of("매진 뒤 배수가 %.3f 다 — 죽은 큐가 예산을 먹고 있다"
                        .formatted(회복));
    }
}

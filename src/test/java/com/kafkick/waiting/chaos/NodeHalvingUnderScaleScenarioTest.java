package com.kafkick.waiting.chaos;

import com.kafkick.waiting.control.GatewayRegistry;
import com.kafkick.waiting.domain.queue.PollBudgetPlanner;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * C21 — 배수가 걸린 채 노드 절반이 사라진다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C21 절이 든다. 여기는
 * 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class NodeHalvingUnderScaleScenarioTest {

    /** 감소를 확정하기까지의 연속 관측 수. 배선 기본값이다. */
    private static final int 하강_지연_틱 = 3;

    private static final int 처음_노드 = 20;

    private static final int 남는_노드 = 10;

    /** 설계 규모의 줄. 전원이 첫 밴드에 들어와 배수가 온전히 걸린다. */
    private static final long 대기 = 20_000;

    private static final double 배분율 = 4_000;

    /** 노드 하나가 감당할 폴링. 이 값을 넘으면 그 노드가 설계 밖이다. */
    private static final double 노드당_예산 = 200;

    /** 손으로 잰 값이다. 2만 명이 전원 첫 밴드라 초당 2만 건을 만든다. */
    private static final double 기대_부하 = 20_000;

    private final GatewayRegistry 분모 = GatewayRegistry.of(하강_지연_틱, 처음_노드);

    /**
     * 이 분모로 배수를 걸었을 때 <b>살아 있는 노드 한 대</b>가 받는 폴링.
     *
     * <p>전원이 첫 밴드라 간격이 곧 배수다. 밴드가 하나뿐인 판을 고른 것은
     * 천장이 안 물게 하기 위해서다 — 천장의 몫은 C19 가 잰다.
     */
    private double 노드당_폴링(int 분모값, int 살아있는) {
        double 예산 = PollBudgetPlanner.budgetRps(분모값);
        double 배수 = PollBudgetPlanner.pollScale(기대_부하, 예산);
        return 기대_부하 / 배수 / 살아있는;
    }

    /**
     * 세 구간을 한 판정으로 잇는다.
     *
     * <p>분모의 비대칭이 <b>발급과 폴링에서 서로 반대</b>라는 것이 이 시나리오의
     * 주제다. 발급은 감소를 늦추는 것이 안전하고, 폴링은 그 반대다.
     */
    @Test
    @DisplayName("C21_배수가_걸린_채_노드_절반이_사라지면_남은_노드가_설계값을_넘게_받는다")
    void C21_배수가_걸린_채_노드_절반이_사라지면_남은_노드가_설계값을_넘게_받는다() {
        int[] 정상_분모 = new int[1];
        double[] 정상_노드당 = new double[1];
        int[] 지연중_분모 = new int[1];
        double[] 지연중_노드당 = new double[1];
        int[] 지연_끝_분모 = new int[1];
        double[] 지연_끝_노드당 = new double[1];
        int[] 걸린_틱 = new int[1];
        int[] 회복_분모 = new int[1];

        ChaosScenario.named("C21 배수가 걸린 채 노드 절반 소실")
                .baseline(() -> {
                    분모.observed(처음_노드);
                    정상_분모[0] = 분모.count();
                    정상_노드당[0] = 노드당_폴링(정상_분모[0], 처음_노드);
                })
                .inject(() -> {
                    // 절반이 사라진 첫 관측. 분모는 아직 안 내려간다 (F5).
                    분모.observed(남는_노드);
                    지연중_분모[0] = 분모.count();
                    지연중_노드당[0] = 노드당_폴링(지연중_분모[0], 남는_노드);
                })
                .duringFault(() -> {
                    // 같은 관측을 이어 준다. 몇 틱 만에 내려가는지를 센다.
                    int 틱 = 1;
                    while (분모.count() > 남는_노드 && 틱 < 100) {
                        분모.observed(남는_노드);
                        틱++;
                    }
                    걸린_틱[0] = 틱;
                    지연_끝_분모[0] = 분모.count();
                    지연_끝_노드당[0] = 노드당_폴링(지연_끝_분모[0], 남는_노드);
                })
                .recover(() -> {
                    분모.observed(처음_노드);
                    회복_분모[0] = 분모.count();
                })
                .afterRecovery(() -> { })
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 전제 — 평시에 노드당 몫이 설계값이어야 뒤의 배가 뜻을 갖는다.
                        평시에_설계값을_받는다(정상_분모[0], 정상_노드당[0]),
                        // 감소는 즉시 반영 안 된다. 발급 상한에는 그것이 안전한
                        // 방향이고, 여기서는 그 지연이 그대로 실린다 (F5).
                        분모가_한_틱에_안_내려간다(지연중_분모[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 지연이 설정한 틱 수만큼이고 그 뒤에는 내려간다.
                        지연이_설정만큼이다(걸린_틱[0]),
                        분모가_지연_뒤에_내려간다(지연_끝_분모[0]),
                        // 내려간 뒤에는 노드당 몫이 설계값으로 돌아온다.
                        지연_뒤에_설계값을_받는다(지연_끝_노드당[0]),
                        // **지연 구간의 노드당 몫은 설계값의 두 배다.** 판정이
                        // 아니라 기록이다 — 그것이 CY-732 이고 게이트 미충족이다.
                        지연_구간이_설계값을_넘는다(정상_노드당[0], 지연중_노드당[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        // 증가는 즉시다. 늦추면 기존 노드가 작은 분모로 나눈다.
                        분모가_즉시_올라간다(회복_분모[0])))
                // **RC1~RC6 은 여기서 안 잰다.** 이 판은 분모와 예산 계산만 걷는다.
                .run();
    }

    private Optional<String> 평시에_설계값을_받는다(int 분모값, double 노드당) {
        if (분모값 != 처음_노드) {
            return Optional.of("전제 — 평시 분모가 %d 다. %d 여야 한다"
                    .formatted(분모값, 처음_노드));
        }
        return 노드당 == 노드당_예산 ? Optional.empty()
                : Optional.of("전제 — 평시 노드당 폴링이 %.0f 다. 설계값 %.0f 여야 한다"
                        .formatted(노드당, 노드당_예산));
    }

    private Optional<String> 분모가_한_틱에_안_내려간다(int 분모값) {
        return 분모값 == 처음_노드 ? Optional.empty()
                : Optional.of("절반이 사라진 첫 틱에 분모가 %d 로 내려갔다 — %d 여야 한다"
                        .formatted(분모값, 처음_노드));
    }

    private Optional<String> 지연이_설정만큼이다(int 틱) {
        return 틱 == 하강_지연_틱 ? Optional.empty()
                : Optional.of("분모가 %d 틱 만에 내려갔다 — %d 틱이어야 한다"
                        .formatted(틱, 하강_지연_틱));
    }

    private Optional<String> 분모가_지연_뒤에_내려간다(int 분모값) {
        return 분모값 == 남는_노드 ? Optional.empty()
                : Optional.of("지연이 끝났는데 분모가 %d 다 — %d 여야 한다"
                        .formatted(분모값, 남는_노드));
    }

    private Optional<String> 지연_뒤에_설계값을_받는다(double 노드당) {
        return 노드당 == 노드당_예산 ? Optional.empty()
                : Optional.of("분모가 내려간 뒤 노드당 폴링이 %.0f 다 — %.0f 여야 한다"
                        .formatted(노드당, 노드당_예산));
    }

    /**
     * 지연 구간의 초과분을 값으로 남긴다. <b>통과 조건이 아니다</b> — 이 배가
     * 1.0 이 되는 것이 CY-732 가 고치려는 것이고, 지금은 게이트 미충족이다.
     */
    private Optional<String> 지연_구간이_설계값을_넘는다(double 평시, double 지연중) {
        double 배 = 지연중 / 평시;
        return 배 == (double) 처음_노드 / 남는_노드 ? Optional.empty()
                : Optional.of("지연 구간의 노드당 몫이 평시의 %.2f 배다 — 분모가 안 내려간 "
                        .formatted(배) + "동안 살아남은 노드 비율 %.2f 배여야 한다"
                        .formatted((double) 처음_노드 / 남는_노드));
    }

    private Optional<String> 분모가_즉시_올라간다(int 분모값) {
        return 분모값 == 처음_노드 ? Optional.empty()
                : Optional.of("노드가 돌아왔는데 분모가 %d 다 — 증가는 즉시여야 한다"
                        .formatted(분모값));
    }
}

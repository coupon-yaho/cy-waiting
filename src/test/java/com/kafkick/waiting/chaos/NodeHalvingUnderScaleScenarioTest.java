package com.kafkick.waiting.chaos;

import com.kafkick.waiting.control.ControlPlaneProperties;
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
 * 그것을 어떻게 판정하는가만 든다. <b>"노드당 폴링" 은 프로덕션에 없는 값이다</b> —
 * 부하가 살아 있는 노드에 고루 나뉜다는 이 시험 안의 가정으로 나눈 것이다.
 */
@Tag("chaos")
class NodeHalvingUnderScaleScenarioTest {

    /**
     * 노드 20 대가 실제로 설 수 있는 배선.
     *
     * <p>레코드를 거쳐 만든다 — {@code GatewayRegistry.of} 를 바로 부르면 검증을
     * 건너뛰어, 어느 배포에서도 안 생기는 조합을 재게 된다 (TS-3). <b>노드 수도
     * 하강 지연도 전부 여기서 읽는다</b> — 리터럴을 따로 두면 한 시나리오 안에
     * 서로 배타적인 두 배포가 섞인다.
     */
    private static final ControlPlaneProperties.Capacity 배선 = 스무_대_배포();

    private static final int 처음_노드 = 배선.expectedNodes();

    private static final int 남는_노드 = 처음_노드 / 2;

    /** 감소를 확정하기까지의 연속 관측 수. <b>배선에서 읽는다</b> — 설정이 바뀌면 따라간다. */
    private static final int 하강_지연_틱 = 배선.rampDownTicks();

    /** 설계 규모의 줄. 전원이 첫 밴드에 들어와 배수가 온전히 걸린다. */
    private static final long 대기 = 20_000;

    private static final double 배분율 = 4_000;

    /** 노드 하나가 감당할 폴링. 이 값을 넘으면 그 노드가 설계 밖이다. */
    private static final double 노드당_예산 = 200;

    /**
     * 이 줄이 만드는 초당 폴링.
     *
     * <p><b>리터럴로 두면 밴드표가 바뀌어도 시험이 안 따라간다.</b> 손으로 잰
     * 2만은 아래 전제 판정이 이 함수의 출력에 대해 따로 확인한다.
     */
    private static final double 기대_부하 = PollBudgetPlanner.expectedPollRps(대기, 배분율);

    /** 손으로 잰 값. 2만 명이 전원 첫 밴드라 초당 2만 건이다. */
    private static final double 손으로_잰_부하 = 20_000;

    /** 나눗셈 결과를 견주므로 정확 비교를 안 쓴다 (TS-11). */
    private static final double 오차 = 1e-9;

    /**
     * <b>배선이 띄우는 대로 띄운다.</b> 프로덕션은 {@code expectedNodes} 로 시작한다.
     *
     * <p>그래서 평시 관측(20)은 값을 안 움직인다 — {@code observed()} 를 통째로
     * 무력화해도 평시 전제만은 통과한다. 그 뮤턴트는 주입·유지 판정이 문다.
     * 다른 값에서 띄우면 평시에서도 죽지만, 그 배포는 존재하지 않는다.
     */
    private final GatewayRegistry 분모 =
            GatewayRegistry.of(배선.rampDownTicks(), 배선.expectedNodes());

    private static ControlPlaneProperties.Capacity 스무_대_배포() {
        ControlPlaneProperties.Capacity 기본 = ControlPlaneProperties.defaults().capacity();
        // 하한을 같이 올려야 레코드가 받는다 — 노드 20 은 하한을 올린 배포에서만
        // 존재한다. 기본 하한 5 그대로면 여기서 터진다.
        int 노드 = 20;
        return new ControlPlaneProperties.Capacity(기본.rampUp(), 기본.freshness(),
                노드 * 2L, 기본.perInstanceCap(), 기본.rampDownTicks(), 노드);
    }

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
        double[] 지연중_노드당 = new double[1];
        // 틱마다의 분모. **루프가 아니라 판정이 하강 시점을 본다.**
        int[] 틱별_분모 = new int[하강_지연_틱];
        double[] 지연_끝_노드당 = new double[1];
        int[] 실패_끊긴_분모 = new int[1];
        int[] 실패_뒤_분모 = new int[1];
        int[] 회복_분모 = new int[1];
        double[] 회복_노드당 = new double[1];

        ChaosScenario.named("C21 배수가 걸린 채 노드 절반 소실")
                .baseline(() -> {
                    분모.observed(처음_노드);
                    정상_분모[0] = 분모.count();
                    정상_노드당[0] = 노드당_폴링(정상_분모[0], 처음_노드);
                })
                .inject(() -> {
                    // 절반이 사라진 첫 관측. 분모는 아직 안 내려간다 (F5).
                    분모.observed(남는_노드);
                    틱별_분모[0] = 분모.count();
                    지연중_노드당[0] = 노드당_폴링(틱별_분모[0], 남는_노드);
                })
                .duringFault(() -> {
                    // **관측 횟수를 시험이 정한다.** 분모를 보고 도는 루프는
                    // 판정이 루프 종료 조건을 되읽게 만든다.
                    for (int 틱 = 2; 틱 <= 하강_지연_틱; 틱++) {
                        분모.observed(남는_노드);
                        틱별_분모[틱 - 1] = 분모.count();
                    }
                    지연_끝_노드당[0] = 노드당_폴링(분모.count(), 남는_노드);
                    실패가_연속을_끊는_판(실패_끊긴_분모, 실패_뒤_분모);
                })
                .recover(() -> {
                    분모.observed(처음_노드);
                    회복_분모[0] = 분모.count();
                })
                .afterRecovery(() -> 회복_노드당[0] = 노드당_폴링(분모.count(), 처음_노드))
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 전제 — 하강 지연이 1 이면 "한 틱에 안 내려간다" 가 올바른
                        // 동작을 실패로 잡는다. 이 판이 성립하는 배포인지 먼저 본다.
                        하강_지연이_둘_이상이다(),
                        // 전제 — 밴드표가 이 줄을 전원 첫 밴드로 놓는다. 이것이
                        // 깨지면 아래 노드당 값이 다른 판의 값이 된다.
                        손으로_잰_부하와_같다(기대_부하),
                        // 전제 — 평시에 노드당 몫이 설계값이어야 뒤의 배가 뜻을 갖는다.
                        평시에_설계값을_받는다(정상_분모[0], 정상_노드당[0]),
                        // 감소는 즉시 반영 안 된다. 발급 상한에는 그것이 안전한
                        // 방향이고, 여기서는 그 지연이 그대로 실린다 (F5).
                        분모가_한_틱에_안_내려간다(틱별_분모[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 마지막 틱 전까지는 안 내려가고, 그 틱에 내려간다.
                        하강_직전까지_안_내려간다(틱별_분모),
                        마지막_틱에_내려간다(틱별_분모),
                        // 내려간 뒤에는 노드당 몫이 설계값으로 돌아온다.
                        지연_뒤에_설계값을_받는다(지연_끝_노드당[0]),
                        // 관측 실패는 "더 작게 봤다" 가 아니다. 노드가 절반 죽는
                        // 판이면 하트비트도 같이 실패한다.
                        실패가_연속을_끊는다(실패_끊긴_분모[0], 실패_뒤_분모[0]),
                        // 지연 구간의 몫에 상한을 둔다. **두 배를 통과 조건으로
                        // 삼지 않는다** — CY-732 를 고치면 설계값으로 내려오고 그때도
                        // 통과한다. 지금 값(2 배)은 G8.15 에 미충족으로 적혀 있다.
                        지연_구간이_설계_상한을_안_넘는다(지연중_노드당[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        // 증가는 즉시다. 늦추면 기존 노드가 작은 분모로 나눈다.
                        분모가_즉시_올라간다(회복_분모[0]),
                        회복_뒤_설계값을_받는다(회복_노드당[0])))
                // **RC1~RC6 은 여기서 안 잰다.** 이 판은 분모와 예산 계산만 걷는다.
                // 특히 회복 시 배수가 한 틱에 10 → 5 로 떨어지는 것(RC4)과 수확
                // 지연 3 초를 못 잰다 — HANDOFF 의 C21 줄에 적어 두었다.
                .run();
    }

    /**
     * 하강 연속 도중 관측이 한 번 실패하는 판.
     *
     * <p>본 경로와 섞으면 두 가지를 한 분모에서 재게 되어 어느 쪽이 깨졌는지
     * 안 보인다. 같은 배선의 분모를 따로 세운다.
     */
    private void 실패가_연속을_끊는_판(int[] 실패_끊긴_분모, int[] 실패_뒤_분모) {
        GatewayRegistry 섞인 =
                GatewayRegistry.of(배선.rampDownTicks(), 배선.expectedNodes());
        for (int 틱 = 1; 틱 < 하강_지연_틱; 틱++) {
            섞인.observed(남는_노드);
        }
        섞인.observationFailed();
        for (int 틱 = 1; 틱 < 하강_지연_틱; 틱++) {
            섞인.observed(남는_노드);
        }
        실패_끊긴_분모[0] = 섞인.count();
        섞인.observed(남는_노드);
        실패_뒤_분모[0] = 섞인.count();
    }

    private Optional<String> 손으로_잰_부하와_같다(double 부하) {
        return 같다(부하, 손으로_잰_부하) ? Optional.empty()
                : Optional.of("전제 — 이 줄의 폴링이 초당 %.2f 다. 손으로 잰 %.2f 여야 한다"
                        .formatted(부하, 손으로_잰_부하));
    }

    private Optional<String> 평시에_설계값을_받는다(int 분모값, double 노드당) {
        if (분모값 != 처음_노드) {
            return Optional.of("전제 — 평시 분모가 %d 다. %d 여야 한다"
                    .formatted(분모값, 처음_노드));
        }
        return 같다(노드당, 노드당_예산) ? Optional.empty()
                : Optional.of("전제 — 평시 노드당 폴링이 %.2f 다. 설계값 %.2f 여야 한다"
                        .formatted(노드당, 노드당_예산));
    }

    private Optional<String> 분모가_한_틱에_안_내려간다(int 분모값) {
        return 분모값 == 처음_노드 ? Optional.empty()
                : Optional.of("절반이 사라진 첫 틱에 분모가 %d 로 내려갔다 — %d 여야 한다"
                        .formatted(분모값, 처음_노드));
    }

    // 첫 틱은 진입 판정이 이미 봤다. 여기서 또 보면 실패 보고서에 같은 사실이
    // 두 줄로 난다.
    private Optional<String> 하강_직전까지_안_내려간다(int[] 틱별) {
        for (int 틱 = 2; 틱 < 하강_지연_틱; 틱++) {
            if (틱별[틱 - 1] != 처음_노드) {
                return Optional.of("%d 번째 관측에 분모가 %d 로 내려갔다 — %d 틱째까지는 %d 여야 한다"
                        .formatted(틱, 틱별[틱 - 1], 하강_지연_틱 - 1, 처음_노드));
            }
        }
        return Optional.empty();
    }

    private Optional<String> 마지막_틱에_내려간다(int[] 틱별) {
        int 마지막 = 틱별[하강_지연_틱 - 1];
        return 마지막 == 남는_노드 ? Optional.empty()
                : Optional.of("%d 번째 관측에도 분모가 %d 다 — %d 여야 한다"
                        .formatted(하강_지연_틱, 마지막, 남는_노드));
    }

    private Optional<String> 지연_뒤에_설계값을_받는다(double 노드당) {
        return 같다(노드당, 노드당_예산) ? Optional.empty()
                : Optional.of("분모가 내려간 뒤 노드당 폴링이 %.2f 다 — %.2f 여야 한다"
                        .formatted(노드당, 노드당_예산));
    }

    private Optional<String> 실패가_연속을_끊는다(int 끊긴_뒤, int 한_틱_더) {
        if (끊긴_뒤 != 처음_노드) {
            return Optional.of("관측 실패가 하강 연속을 안 끊었다 — 분모가 %d 로 내려갔다"
                    .formatted(끊긴_뒤));
        }
        return 한_틱_더 == 남는_노드 ? Optional.empty()
                : Optional.of("실패 뒤 %d 틱을 다시 봤는데 분모가 %d 다 — %d 여야 한다"
                        .formatted(하강_지연_틱, 한_틱_더, 남는_노드));
    }

    /**
     * 지연 구간의 몫에 상한을 둔다.
     *
     * <p><b>상한을 관측이 아니라 설계 상수에서 뽑는다.</b> 평시 값으로 나누면
     * 분모가 양쪽에서 약분되어 무엇을 바꿔도 넘을 수 없는 항등식이 된다 — 처음에
     * 그렇게 썼다가 예산 상수를 바꾼 뮤턴트에서 이 판정만 침묵했다.
     *
     * <p>CY-732 를 고치면 설계값으로 내려와 그대로 통과한다.
     */
    private Optional<String> 지연_구간이_설계_상한을_안_넘는다(double 지연중) {
        double 상한 = 노드당_예산 * 처음_노드 / 남는_노드;
        return 지연중 <= 상한 + 오차 ? Optional.empty()
                : Optional.of("지연 구간의 노드당 폴링이 %.2f 다 — 설계값 %.2f 의 "
                        .formatted(지연중, 노드당_예산) + "노드 비율 배인 %.2f 이내여야 한다"
                        .formatted(상한));
    }

    /** 하강 지연이 1 인 배포에서는 "한 틱에 안 내려간다" 가 올바른 동작을 실패로 잡는다. */
    private Optional<String> 하강_지연이_둘_이상이다() {
        return 하강_지연_틱 >= 2 ? Optional.empty()
                : Optional.of("전제 — 하강 지연이 %d 틱이다. 이 판은 2 틱 이상인 배포에서만 "
                        .formatted(하강_지연_틱) + "뜻이 있다");
    }

    private Optional<String> 분모가_즉시_올라간다(int 분모값) {
        return 분모값 == 처음_노드 ? Optional.empty()
                : Optional.of("노드가 돌아왔는데 분모가 %d 다 — 증가는 즉시여야 한다"
                        .formatted(분모값));
    }

    /**
     * <b>새 보장이 아니라 산술이다.</b> 분모가 20 이라는 것과 노드당 예산이
     * 200 이라는 것에서 바로 나온다 — 앞의 두 판정에 포함된다. 남겨 둔 것은
     * 실패했을 때 어느 값이 틀어졌는지가 메시지에 바로 보이기 때문이다.
     */
    private Optional<String> 회복_뒤_설계값을_받는다(double 노드당) {
        return 같다(노드당, 노드당_예산) ? Optional.empty()
                : Optional.of("회복 뒤 노드당 폴링이 %.2f 다 — %.2f 로 돌아와야 한다"
                        .formatted(노드당, 노드당_예산));
    }

    private boolean 같다(double 왼쪽, double 오른쪽) {
        return Math.abs(왼쪽 - 오른쪽) < 오차;
    }
}

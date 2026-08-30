package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 3단계 시나리오 뼈대 (8.0.3).
 *
 * <p>Phase 8 의 시나리오 전부가 이걸 쓴다. 여기서 재는 것은 뼈대 자신이다.
 */
// 판정 기준이 시나리오 밖에 있어야 한다. 안에 두면 시나리오마다 기준이 갈리고,
// 그때부터 "전 시나리오 초과 발급 0" 같은 게이트가 이름만 남는다.
@Tag("chaos")
class ChaosScenarioTest {

    /** 어느 구간에서 무엇을 봤는지 순서대로 담는다. */
    private final List<String> 발자국 = new ArrayList<>();

    private ChaosScenario 시나리오() {
        return ChaosScenario.named("시험용")
                .baseline(() -> 발자국.add("정상"))
                .inject(() -> 발자국.add("주입"))
                .duringFault(() -> 발자국.add("유지"))
                .recover(() -> 발자국.add("복구"))
                .afterRecovery(() -> 발자국.add("회복"));
    }

    /**
     * <b>세 구간을 순서대로 지난다.</b>
     *
     * <p>주입이 정상 수집보다 먼저 돌면 정상 구간이 이미 장애다. 그러면 회복
     * 버스트를 비교할 기준이 없어진다.
     */
    @Test
    @DisplayName("세_구간을_순서대로_지난다")
    void 세_구간을_순서대로_지난다() {
        시나리오().run();

        assertThat(발자국).containsExactly("정상", "주입", "유지", "복구", "회복");
    }

    /** 구간별 단언이 그 구간이 끝난 뒤에 걸린다. */
    @Test
    @DisplayName("구간별_단언이_그_구간_뒤에_걸린다")
    void 구간별_단언이_그_구간_뒤에_걸린다() {
        시나리오()
                .assertEntry(() -> {
                    발자국.add("진입판정");
                    return List.of();
                })
                .assertDuring(() -> {
                    발자국.add("유지판정");
                    return List.of();
                })
                .assertRecovery(() -> {
                    발자국.add("회복판정");
                    return List.of();
                })
                .run();

        assertThat(발자국).containsExactly("정상", "주입", "유지", "진입판정",
                "복구", "회복", "유지판정", "회복판정");
    }

    /**
     * <b>단언이 깨지면 시나리오가 실패한다.</b>
     *
     * <p>안 터지면 판정이 있어도 아무 일이 안 일어난다 — 게이트가 이름만 남는
     * 가장 흔한 모양이다.
     */
    @Test
    @DisplayName("회복_단언이_깨지면_실패한다")
    void 회복_단언이_깨지면_실패한다() {
        assertThatThrownBy(() -> 시나리오()
                .assertRecovery(() -> List.of("RC4 회복 버스트 2.00 배"))
                .run())
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("시험용")
                .hasMessageContaining("RC4");
    }

    /** 진입 단계의 위반도 같은 자로 터진다. 한 구간만 재면 나머지가 비어 있다. */
    @Test
    @DisplayName("진입_단언이_깨져도_실패한다")
    void 진입_단언이_깨져도_실패한다() {
        assertThatThrownBy(() -> 시나리오()
                .assertEntry(() -> List.of("전면 차단이 일어났다"))
                .run())
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("진입");
    }

    /**
     * <b>회복은 단언이 깨져도 돈다.</b>
     *
     * <p>진입에서 터뜨리고 멈추면 장애를 주입한 채 시험이 끝난다. 그러면 다음
     * 시험이 그 상태를 물려받아, 원인이 엉뚱한 곳에서 드러난다.
     */
    @Test
    @DisplayName("단언이_깨져도_복구는_돈다")
    void 단언이_깨져도_복구는_돈다() {
        assertThatThrownBy(() -> 시나리오()
                .assertEntry(() -> List.of("깨졌다"))
                .run())
                .isInstanceOf(AssertionError.class);

        assertThat(발자국).contains("복구");
    }

    /** 깨진 것이 여럿이면 다 보여야 한다. 하나만 보이면 나머지를 또 돌려야 한다. */
    @Test
    @DisplayName("깨진_기준을_모두_보여_준다")
    void 깨진_기준을_모두_보여_준다() {
        assertThatThrownBy(() -> 시나리오()
                .assertRecovery(() -> List.of("RC1 초과 발급", "RC4 회복 버스트"))
                .run())
                .hasMessageContaining("RC1")
                .hasMessageContaining("RC4");
    }

    /** 판정기를 그대로 받는다. 뼈대가 기준을 다시 쓰면 만든 뜻이 사라진다. */
    @Test
    @DisplayName("공통_판정기를_그대로_쓴다")
    void 공통_판정기를_그대로_쓴다() {
        assertThatThrownBy(() -> 시나리오()
                .assertRecovery(() -> RecoveryCriteria.violations(
                        RecoveryCriteria.overIssued(101, 100),
                        Optional.empty()))
                .run())
                .hasMessageContaining("RC1");
    }

    /** 이름이 없으면 어느 시나리오가 깨졌는지 못 찾는다. */
    @Test
    @DisplayName("이름_없는_시나리오는_거절한다")
    void 이름_없는_시나리오는_거절한다() {
        assertThatThrownBy(() -> ChaosScenario.named(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 주입만 하고 복구를 안 정하면 장애를 켠 채 끝난다. 만들 때 막는다. */
    @Test
    @DisplayName("복구_없는_시나리오는_거절한다")
    void 복구_없는_시나리오는_거절한다() {
        assertThatThrownBy(() -> ChaosScenario.named("복구 없음")
                .inject(() -> { })
                .run())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("복구");
    }
}

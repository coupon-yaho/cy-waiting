package com.kafkick.waiting.domain.allocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 조여 둔 배분을 푸는 속도.
 *
 * <p>서킷이 배분을 초당 하나로 조인 뒤 닫히면, 조임이 없어진 그 한 틱에 값이
 * 원래 몫으로 그대로 돌아간다. 방금 실패를 끝낸 뒷단을 향한 계단이다 (RC4).
 */
class ReleaseRampTest {

    /** 값을 못 박으려고 시험 안에서만 쓰는 배수. 기본값과 따로 둔다. */
    private static final double STEP = 1.2;

    @Test
    @DisplayName("조인_적이_없으면_그대로_통과시킨다")
    void 조인_적이_없으면_그대로_통과시킨다() {
        // 평상시에 끼어들면 캠페인이 열릴 때 정상적인 증가까지 늦춘다.
        ReleaseRamp ramp = ReleaseRamp.of(STEP);

        assertThat(ramp.next(300, 0, false)).isEqualTo(300);
        assertThat(ramp.next(500, 0, false)).isEqualTo(500);
    }

    @Test
    @DisplayName("조이는_동안은_조인_값을_그대로_낸다")
    void 조이는_동안은_조인_값을_그대로_낸다() {
        // 조임은 램프가 할 일이 아니다. 여기서 더 낮추면 두 장치가 같은 값을
        // 두 번 누른다.
        ReleaseRamp ramp = ReleaseRamp.of(STEP);

        assertThat(ramp.next(1, 0, true)).isEqualTo(1);
        assertThat(ramp.next(1, 0, true)).isEqualTo(1);
    }

    @Test
    @DisplayName("조임이_풀리면_한_틱에_1_2배까지만_올린다")
    void 조임이_풀리면_한_틱에_1_2배까지만_올린다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(100, 0, true);

        // 100 → 120 → 144. 원래 몫이 300 이어도 한 번에 못 간다.
        assertThat(ramp.next(300, 0, false)).isEqualTo(120);
        assertThat(ramp.next(300, 0, false)).isEqualTo(144);
    }

    @Test
    @DisplayName("몫이_1_이어도_올라간다")
    void 몫이_1_이어도_올라간다() {
        // 서킷이 반쯤 열린 동안의 몫이 정확히 1 이다. 1 × 1.2 를 내림하면 1 이라
        // 배수만으로는 영영 못 벗어난다 — 그 회차의 램프는 안 푸는 것과 같다.
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(1, 0, true);

        assertThat(ramp.next(300, 0, false)).isEqualTo(2);
    }

    @Test
    @DisplayName("몫이_0_이어도_올라간다")
    void 몫이_0_이어도_올라간다() {
        // 서킷이 열려 있던 동안의 몫은 0 이다. 0 에 무엇을 곱해도 0 이다.
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(0, 0, true);

        assertThat(ramp.next(300, 0, false)).isEqualTo(1);
    }

    @Test
    @DisplayName("원래_몫에_닿으면_램프가_비켜선다")
    void 원래_몫에_닿으면_램프가_비켜선다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(100, 0, true);

        long value = 100;
        while (value < 300) {
            value = ramp.next(300, 0, false);
        }

        // 닿은 뒤로는 다시 안 누른다. 누르면 정상 구간이 계속 램프에 묶인다.
        assertThat(ramp.next(1000, 0, false)).isEqualTo(1000);
    }


    @Test
    @DisplayName("다시_조이면_그_값에서_다시_시작한다")
    void 다시_조이면_그_값에서_다시_시작한다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(100, 0, true);
        ramp.next(300, 0, false);

        // 회복 도중에 서킷이 다시 열렸다. 램프의 기준도 그 값으로 내려가야
        // 한다 — 안 그러면 두 번째 회복이 120 에서 시작한다.
        assertThat(ramp.next(1, 0, true)).isEqualTo(1);
        assertThat(ramp.next(300, 0, false)).isEqualTo(2);
    }

    @Test
    @DisplayName("목표가_잠깐_내려가도_램프는_남는다")
    void 목표가_잠깐_내려가도_램프는_남는다() {
        // **따라잡은 것과 목표가 내려간 것은 다르다.** 뒷단이 "여유 0" 을 한 번
        // 보고하는 것만으로 램프가 꺼지면, 다음 회차의 계단이 무제한이다.
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(100, 0, true);
        assertThat(ramp.next(50, 0, false)).isEqualTo(50);

        assertThat(ramp.ramping()).isTrue();
        assertThat(ramp.next(300, 0, false)).isEqualTo(60);
    }

    @Test
    @DisplayName("여유_0_을_한_번_보고해도_램프는_남는다")
    void 여유_0_을_한_번_보고해도_램프는_남는다() {
        // 서킷은 오류가 없으면 닫혀 있다. 그 회차의 목표가 0 이면 조인 것이
        // 아니라 뒷단이 스스로 0 이라고 말한 것이다.
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(100, 0, true);
        assertThat(ramp.next(0, 0, false)).isZero();

        assertThat(ramp.next(7300, 0, false)).isEqualTo(1);
    }

    @Test
    @DisplayName("걸려_있는지를_밖에서_알_수_있다")
    void 걸려_있는지를_밖에서_알_수_있다() {
        // 진입과 해제를 쌍으로 남기려면 배선이 이 상태를 봐야 한다 (LG-2).
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        assertThat(ramp.ramping()).isFalse();

        ramp.next(1, 0, true);
        assertThat(ramp.ramping()).isTrue();

        ramp.next(1, 0, false);
        assertThat(ramp.ramping()).as("따라잡으면 비켜선다").isFalse();
    }

    @Test
    @DisplayName("기본_배수는_회복을_삼십초_안에_끝낸다")
    void 기본_배수는_회복을_삼십초_안에_끝낸다() {
        // **이 수의 제약은 RC4 가 아니라 RC3 이다.** 원래 몫으로 돌아가는 것은
        // 정의상 정상의 1.0 배라 RC4 를 안 깬다. 대신 회복은 30초 안에 끝나야
        // 하고, 틱이 1초라 틱 수가 곧 초다.
        ReleaseRamp ramp = ReleaseRamp.of(ReleaseRamp.DEFAULT_STEP);
        ramp.next(0, 0, true);

        int ticks = 0;
        long value = 0;
        while (value < 7_300 && ticks < 100) {
            value = ramp.next(7_300, 0, false);
            ticks++;
        }

        assertThat(value).isEqualTo(7_300);
        assertThat(ticks).as("여유를 두고 들어와야 한다").isLessThanOrEqualTo(20);
    }

    @Test
    @DisplayName("정책_하한_아래로는_안_내려간다")
    void 정책_하한_아래로는_안_내려간다() {
        // 하한 아래로 눌린 회차에는 노드당 몫이 유휴 비율 아래라, 줄 설 이유가
        // 없는 쿠폰이 전 노드에서 줄을 선다 (R1).
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(1, 40, true);

        assertThat(ramp.next(7_300, 40, false)).isEqualTo(40);
        assertThat(ramp.next(7_300, 40, false)).isEqualTo(48);
    }

    @Test
    @DisplayName("하한이_목표보다_높으면_목표를_따른다")
    void 하한이_목표보다_높으면_목표를_따른다() {
        // 없는 여유를 만들어 내지 않는다. 뒷단이 스스로 말한 값이 상한이다.
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(1, 40, true);

        assertThat(ramp.next(20, 40, false)).isEqualTo(20);
    }

    @Test
    @DisplayName("음수_하한은_거절한다")
    void 음수_하한은_거절한다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);

        assertThatThrownBy(() -> ramp.next(100, -1, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("배수는_1_보다_커야_한다")
    void 배수는_1_보다_커야_한다() {
        // 1 이면 영영 안 오르고, 1 미만이면 회복이 곧 감소다.
        assertThatThrownBy(() -> ReleaseRamp.of(1.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ReleaseRamp.of(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("음수_몫은_거절한다")
    void 음수_몫은_거절한다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);

        assertThatThrownBy(() -> ramp.next(-1, 0, false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

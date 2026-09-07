package com.kafkick.waiting.domain.allocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 조여 둔 배분을 푸는 속도.
 *
 * <p>서킷이 배분을 초당 하나로 조인 뒤 닫히면, 조임이 없어진 그 한 틱에 값이
 * 원래 몫으로 그대로 돌아간다. 방금 실패를 끝낸 뒷단을 향한 계단이다.
 */
class ReleaseRampTest {

    /** 값을 못 박으려고 시험 안에서만 쓰는 배수. 기본값과 따로 둔다. */
    private static final double STEP = 1.2;

    /**
     * 운영이 실제로 넘기는 하한. 배분은 노드 수에서 낸 최소를 늘 함께 넘긴다 —
     * 노드가 하나여도 2 다. 0 은 이 클래스의 계약을 재는 자리에만 쓴다.
     */
    private static final long 운영_하한 = 2;

    @Test
    @DisplayName("첫_회차는_그대로_내고_그_뒤는_배수다")
    void 첫_회차는_그대로_내고_그_뒤는_배수다() {
        // **첫 회차는 견줄 것이 없다.** 제한하면 기동이 하한에서 시작해 그동안
        // 한산 통과 상한이 0 이다 (R1). 그 뒤로는 늘 배수를 지킨다 — 회복
        // 구간에만 걸면 그 구간이 끝나는 순간이 새 계단이 된다 (RC4).
        ReleaseRamp ramp = ReleaseRamp.of(STEP);

        assertThat(ramp.next(300, 0, false)).isEqualTo(300);
        // 300 × 1.2 = 360.
        assertThat(ramp.next(500, 0, false)).isEqualTo(360);
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
        //
        // **하한 0 은 이 클래스의 계약을 재는 자리다.** 배분은 늘 2 이상을
        // 넘기므로 운영에서는 하한이 먼저 이긴다. 그래도 계약은 계약이다.
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
    @DisplayName("원래_몫에_닿아도_다음_상승은_배수다")
    void 원래_몫에_닿아도_다음_상승은_배수다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(100, 0, true);

        long value = 100;
        int ticks = 0;
        while (value < 300 && ticks < 50) {
            value = ramp.next(300, 0, false);
            ticks++;
        }
        assertThat(ticks).as("전진이 멎으면 실패로 끝나야 한다").isLessThan(50);

        // **닿았다고 놓지 않는다.** 놓으면 그 뒤의 상승이 계단이다 — 다른 상한이
        // 몫을 눌러 두었다 풀리는 자리가 정확히 그렇다 (RC4).
        assertThat(ramp.next(1000, 0, false)).isEqualTo(360);
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
        // **20 으로 두면 지켜 주지 않는다.** 배수 2 도 14틱이라 통과하는데, 그
        // 값이 바로 실측에서 램프만 11.4초를 물어 RC3 를 넘긴 값이다. 나머지
        // 항(잔여·열림 대기·표본 확보·완화)이 20초 남짓이라 램프의 예산은 열 틱
        // 아래다.
        assertThat(ticks).as("램프가 쓸 수 있는 예산은 열 틱 아래다")
                .isLessThanOrEqualTo(9);
    }

    @Test
    @DisplayName("운영이_넘기는_하한에서_출발한다")
    void 운영이_넘기는_하한에서_출발한다() {
        // 호출부가 실제로 만드는 조합이다 — 조인 몫과 노드 수에서 낸 최소.
        ReleaseRamp ramp = ReleaseRamp.of(ReleaseRamp.DEFAULT_STEP);
        ramp.next(0, 운영_하한, true);

        assertThat(ramp.next(7_300, 운영_하한, false)).isEqualTo(2);
        assertThat(ramp.next(7_300, 운영_하한, false)).isEqualTo(8);
    }

    @Test
    @DisplayName("배수는_내림한다")
    void 배수는_내림한다() {
        // 13 × 1.2 = 15.6 이다. 올림이나 반올림으로 바뀌면 램프가 스스로 선언한
        // 배수를 넘는데, 곱이 정수인 값만 못 박으면 그 변화가 안 잡힌다.
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(13, 0, true);

        assertThat(ramp.next(300, 0, false)).isEqualTo(15);
    }

    @Test
    @DisplayName("정확히_따라잡은_회차에_비켜선다")
    void 정확히_따라잡은_회차에_비켜선다() {
        // 배수 1.2 로 100 에서 허용은 정확히 120 이다. 목표가 그 값이면 이미
        // 따라잡은 것이라 다음 회차부터 안 눌러야 한다 — 한 칸 어긋나면 정상
        // 구간이 한 회차 더 램프에 묶인다.
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(100, 0, true);

        assertThat(ramp.next(120, 0, false)).isEqualTo(120);
        assertThat(ramp.ramping()).isFalse();
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

    /**
     * 승계로 이어받은 노드는 조인 적이 없어 램프가 아예 안 걸린다. 그러면 첫
     * 회차가 발행된 값에서 목표까지 한 번에 뛴다.
     */
    @Test
    @DisplayName("이어받으면_발행된_값에서_올린다")
    void 이어받으면_발행된_값에서_올린다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);

        ramp.resumeFrom(256);

        // 배수 1.2 를 그대로 밟는다. 상수로 견주면 배수를 바꿔도 초록이다.
        assertThat(ramp.next(1200, 0, false))
                .as("한 틱에 256 에서 1200 으로 뛰면 회복이 곧 2차 장애다")
                .isEqualTo(307);
        assertThat(ramp.next(1200, 0, false)).isEqualTo(368);
        assertThat(ramp.next(1200, 0, false)).isEqualTo(441);
    }

    /** 이어받은 값이 이미 목표면 올릴 것이 없다. 램프가 켜진 채 남으면 안 된다. */
    @Test
    @DisplayName("이어받은_값이_목표면_그_회차는_그대로다")
    void 이어받은_값이_목표면_그_회차는_그대로다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);

        ramp.resumeFrom(300);

        assertThat(ramp.next(300, 0, false)).isEqualTo(300);
        assertThat(ramp.next(1200, 0, false))
                .as("따라잡아도 다음 상승은 배수를 지킨다")
                .isEqualTo(360);
    }

    /**
     * <b>잘리지 않은 회차 뒤에도 브레이크가 남아야 한다.</b> 목표가 한 번
     * 내려가면 그 회차는 안 잘리는데, 그때 승계가 나면 낡은 큰 값이 그대로
     * 들어와 다음 한 틱이 무제한이 된다.
     */
    @Test
    @DisplayName("안_잘린_회차_뒤에도_낮추는_쪽으로만_받는다")
    void 안_잘린_회차_뒤에도_낮추는_쪽으로만_받는다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        ramp.next(100, 0, true);
        // 뒷단이 여유를 낮게 보고한 회차. 안 잘린다.
        assertThat(ramp.next(50, 0, false)).isEqualTo(50);

        // 그 순간 승계가 나고, 낡은 재료가 장애 전 값을 들고 온다.
        ramp.resumeFrom(7_300);

        assertThat(ramp.next(7_300, 0, false))
                .as("50 에서 7300 으로 뛰면 고치려던 계단이 그대로다")
                .isEqualTo(60);
    }

    /**
     * 이 값은 승계를 일으킨 바로 그 경로에서 온다. 낡은 큰 값을 그대로 받으면
     * 브레이크가 그 자리에서 풀린다.
     */
    @Test
    @DisplayName("램프_중에는_낮추는_쪽으로만_받는다")
    void 램프_중에는_낮추는_쪽으로만_받는다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);
        // 조인 회차가 기준을 0 으로 내렸다.
        ramp.next(0, 0, true);

        // 낡은 노드가 장애 직전 값을 들고 이어받는다.
        ramp.resumeFrom(1200);

        assertThat(ramp.next(1200, 0, false))
                .as("한 틱에 0 에서 1200 이면 고치려던 계단이 그대로다")
                .isEqualTo(1);
    }

    /** 발행된 값을 모르면(음수) 세울 기준이 없다. */
    @Test
    @DisplayName("음수는_거절한다")
    void 이어받을_값이_음수면_거절한다() {
        ReleaseRamp ramp = ReleaseRamp.of(STEP);

        assertThatThrownBy(() -> ramp.resumeFrom(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>못 만드는 조합을 픽스처가 만들면 안 된다</b> (DS-2). 아무것도 안 낸
     * 상태로 램프가 걸려 있으면 다음 회차가 배수를 안 지킨다.
     */
    @Test
    @DisplayName("안_낸_채_걸린_램프는_못_만든다")
    void 안_낸_채_걸린_램프는_못_만든다() {
        assertThatThrownBy(() -> new ReleaseRamp.State(-1, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** -1 보다 작은 몫은 어느 경로도 안 만든다. 받으면 그 값이 기준이 된다. */
    @Test
    @DisplayName("몫은_음수_하나까지만_받는다")
    void 몫은_음수_하나까지만_받는다() {
        assertThatThrownBy(() -> new ReleaseRamp.State(-2, false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

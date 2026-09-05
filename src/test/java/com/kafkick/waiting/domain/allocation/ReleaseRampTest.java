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

    @Test
    @DisplayName("조인_적이_없으면_그대로_통과시킨다")
    void 조인_적이_없으면_그대로_통과시킨다() {
        // 평상시에 끼어들면 캠페인이 열릴 때 정상적인 증가까지 늦춘다.
        ReleaseRamp ramp = ReleaseRamp.of(1.2);

        assertThat(ramp.next(300, false)).isEqualTo(300);
        assertThat(ramp.next(500, false)).isEqualTo(500);
    }

    @Test
    @DisplayName("조이는_동안은_조인_값을_그대로_낸다")
    void 조이는_동안은_조인_값을_그대로_낸다() {
        // 조임은 램프가 할 일이 아니다. 여기서 더 낮추면 두 장치가 같은 값을
        // 두 번 누른다.
        ReleaseRamp ramp = ReleaseRamp.of(1.2);

        assertThat(ramp.next(1, true)).isEqualTo(1);
        assertThat(ramp.next(1, true)).isEqualTo(1);
    }

    @Test
    @DisplayName("조임이_풀리면_한_틱에_1_2배까지만_올린다")
    void 조임이_풀리면_한_틱에_1_2배까지만_올린다() {
        ReleaseRamp ramp = ReleaseRamp.of(1.2);
        ramp.next(100, true);

        // 100 → 120 → 144. 원래 몫이 300 이어도 한 번에 못 간다.
        assertThat(ramp.next(300, false)).isEqualTo(120);
        assertThat(ramp.next(300, false)).isEqualTo(144);
    }

    @Test
    @DisplayName("몫이_1_이어도_올라간다")
    void 몫이_1_이어도_올라간다() {
        // 서킷이 반쯤 열린 동안의 몫이 정확히 1 이다. 1 × 1.2 를 내림하면 1 이라
        // 배수만으로는 영영 못 벗어난다 — 그 회차의 램프는 안 푸는 것과 같다.
        ReleaseRamp ramp = ReleaseRamp.of(1.2);
        ramp.next(1, true);

        assertThat(ramp.next(300, false)).isEqualTo(2);
    }

    @Test
    @DisplayName("몫이_0_이어도_올라간다")
    void 몫이_0_이어도_올라간다() {
        // 서킷이 열려 있던 동안의 몫은 0 이다. 0 에 무엇을 곱해도 0 이다.
        ReleaseRamp ramp = ReleaseRamp.of(1.2);
        ramp.next(0, true);

        assertThat(ramp.next(300, false)).isEqualTo(1);
    }

    @Test
    @DisplayName("원래_몫에_닿으면_램프가_비켜선다")
    void 원래_몫에_닿으면_램프가_비켜선다() {
        ReleaseRamp ramp = ReleaseRamp.of(1.2);
        ramp.next(100, true);

        long value = 100;
        while (value < 300) {
            value = ramp.next(300, false);
        }

        // 닿은 뒤로는 다시 안 누른다. 누르면 정상 구간이 계속 램프에 묶인다.
        assertThat(ramp.next(1000, false)).isEqualTo(1000);
    }

    @Test
    @DisplayName("원래_몫이_줄면_그_값을_따른다")
    void 원래_몫이_줄면_그_값을_따른다() {
        // 램프는 올리는 쪽만 막는다. 내리는 것을 막으면 뒷단이 약해진 뒤에도
        // 옛 몫을 계속 내보낸다.
        ReleaseRamp ramp = ReleaseRamp.of(1.2);
        ramp.next(100, true);

        assertThat(ramp.next(50, false)).isEqualTo(50);
    }

    @Test
    @DisplayName("다시_조이면_그_값에서_다시_시작한다")
    void 다시_조이면_그_값에서_다시_시작한다() {
        ReleaseRamp ramp = ReleaseRamp.of(1.2);
        ramp.next(100, true);
        ramp.next(300, false);

        // 회복 도중에 서킷이 다시 열렸다. 램프의 기준도 그 값으로 내려가야
        // 한다 — 안 그러면 두 번째 회복이 120 에서 시작한다.
        assertThat(ramp.next(1, true)).isEqualTo(1);
        assertThat(ramp.next(300, false)).isEqualTo(2);
    }

    @Test
    @DisplayName("리더가_바뀌면_기준을_버린다")
    void 리더가_바뀌면_기준을_버린다() {
        // 남이 리더였던 동안 움직인 값을 못 보고 제 옛 값을 이어 쓰면, 승계
        // 직후 한 틱이 옛 기준의 1.2 배로 나간다.
        ReleaseRamp ramp = ReleaseRamp.of(1.2);
        ramp.next(1, true);

        ramp.reset();

        assertThat(ramp.next(300, false)).isEqualTo(300);
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
        ReleaseRamp ramp = ReleaseRamp.of(1.2);

        assertThatThrownBy(() -> ramp.next(-1, false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

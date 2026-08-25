package com.kafkick.waiting.domain.allocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 여유 값을 다듬는다.
 *
 * <p>순간값을 그대로 쓰면 GC 스파이크 한 번이 표시 ETA 를 두 배로 만든다.
 * ETA 오차의 지배항이 여기다.
 */
class CreditSmootherTest {

    @Test
    @DisplayName("첫_관측치가_초기값이_된다")
    void 첫_관측치가_초기값이_된다() {
        // 0 에서 시작하면 첫 몇 틱 동안 실제보다 한참 낮은 값이 나가고,
        // 그 사이 표시 ETA 가 몇 배로 뛴다.
        CreditSmoother s = CreditSmoother.of(0.2);

        assertThat(s.observe(1000)).isEqualTo(1000.0);
    }

    @Test
    @DisplayName("EWMA는_설정된_시상수로_수렴한다")
    void EWMA는_설정된_시상수로_수렴한다() {
        // α=0.2 로 1000 에서 시작해 0 을 5틱 관측하면 1000×0.8^5 = 327.68
        CreditSmoother s = CreditSmoother.of(0.2);
        s.observe(1000);

        double value = 0;
        for (int i = 0; i < 5; i++) {
            value = s.observe(0);
        }

        assertThat(value).isCloseTo(327.68, within(0.01));
    }

    @Test
    @DisplayName("순간_스파이크는_그대로_반영되지_않는다")
    void 순간_스파이크는_그대로_반영되지_않는다() {
        CreditSmoother s = CreditSmoother.of(0.2);
        s.observe(1000);

        // 한 틱 튀어도 20% 만 먹는다
        assertThat(s.observe(2000)).isCloseTo(1200, within(0.01));
    }

    @Test
    @DisplayName("알파가_범위를_벗어나면_거부한다")
    void 알파가_범위를_벗어나면_거부한다() {
        // α=0 이면 영원히 안 움직이고, α>1 이면 값이 진동하며 발산한다.
        assertThatThrownBy(() -> CreditSmoother.of(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CreditSmoother.of(1.1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CreditSmoother.of(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("알파가_1이면_평활화하지_않는다")
    void 알파가_1이면_평활화하지_않는다() {
        // 끄고 싶을 때가 있다. 부하 시험에서 원본을 봐야 할 때다.
        CreditSmoother s = CreditSmoother.of(1.0);
        s.observe(1000);

        assertThat(s.observe(2000)).isEqualTo(2000.0);
    }

    @Test
    @DisplayName("음수_관측치는_거부한다")
    void 음수_관측치는_거부한다() {
        assertThatThrownBy(() -> CreditSmoother.of(0.2).observe(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("평활화_상태를_내보내고_되살릴_수_있다")
    void 평활화_상태를_내보내고_되살릴_수_있다() {
        // F9 — 리더가 바뀌면 평활화가 0 에서 다시 시작해 ETA 가 튄다.
        CreditSmoother original = CreditSmoother.of(0.2);
        original.observe(1000);
        original.observe(500);

        CreditSmoother restored = CreditSmoother.restore(0.2, original.snapshot());

        assertThat(restored.observe(500)).isEqualTo(original.observe(500));
    }

    @Test
    @DisplayName("관측_전_스냅샷을_되살리면_다음_값이_초기값이_된다")
    void 관측_전_스냅샷을_되살리면_다음_값이_초기값이_된다() {
        CreditSmoother restored =
                CreditSmoother.restore(0.2, CreditSmoother.of(0.2).snapshot());

        assertThat(restored.observe(700)).isEqualTo(700.0);
    }

    @Test
    @DisplayName("비유한_관측치는_거부한다")
    void 비유한_관측치는_거부한다() {
        // 한 번 들어오면 EWMA 가 영영 NaN 이고, 표시 ETA 도 함께 죽는다.
        assertThatThrownBy(() -> CreditSmoother.of(0.2).observe(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CreditSmoother.of(0.2).observe(Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("깨진_스냅샷은_되살릴_수_없다")
    void 깨진_스냅샷은_되살릴_수_없다() {
        // 이월받은 값이 NaN 이면 그 순간부터 EWMA 가 영영 NaN 이고, 리더가
        // 바뀐 뒤에야 표시 ETA 가 죽은 것으로 드러난다.
        assertThatThrownBy(() -> new CreditSmoother.Snapshot(Double.NaN, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CreditSmoother.Snapshot(-1, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CreditSmoother.Snapshot(500, false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 신선도의 기준 시각이 뒤로 가면 <b>뒷단 보고가 전부 미래가 된다</b>. 그러면
 * 한꺼번에 낡음이 되고, 시계가 따라잡을 때까지 크레딧이 하한에 박힌다.
 *
 * <p>복제본 승격은 카오스 시나리오에 이미 있는 조건이라 가정이 아니다 (A-9).
 */
class ServerClockTest {

    private static final long 지금 = 1_800_000_000L;

    private final ServerClock clock = ServerClock.create();

    @Test
    @DisplayName("앞으로_가면_그대로_받는다")
    void 앞으로_가면_그대로_받는다() {
        assertThat(clock.observe(지금)).isEqualTo(지금);
        assertThat(clock.observe(지금 + 1)).isEqualTo(지금 + 1);
        assertThat(clock.skew().appliedCount()).isZero();
    }

    @Test
    @DisplayName("뒤로_가면_바닥값을_준다")
    void 뒤로_가면_바닥값을_준다() {
        clock.observe(지금);

        assertThat(clock.observe(지금 - 30)).isEqualTo(지금);
    }

    /** 보정한 사실을 안 남기면 "왜 다 같은 값인가" 를 영영 못 밝힌다. */
    @Test
    @DisplayName("보정한_횟수와_폭을_남긴다")
    void 보정한_횟수와_폭을_남긴다() {
        clock.observe(지금);

        clock.observe(지금 - 30);
        clock.observe(지금 - 5);

        assertThat(clock.skew().appliedCount()).isEqualTo(2);
        assertThat(clock.skew().maxSkewMicros()).isEqualTo(30_000_000L);
    }

    /** 시계가 따라잡으면 바닥값을 놓는다. 안 놓으면 영영 옛 시각에 머문다. */
    @Test
    @DisplayName("따라잡으면_다시_따라간다")
    void 따라잡으면_다시_따라간다() {
        clock.observe(지금);
        clock.observe(지금 - 30);

        assertThat(clock.observe(지금 + 1)).isEqualTo(지금 + 1);
    }

    /**
     * 바닥값이 아직 없는 첫 관측은 단조 가드가 못 막는다. 초가 아닌 값을 그대로
     * 받으면 그 순간 전 인스턴스가 낡음이 된다 — 안 믿는 편이 낫다.
     */
    @Test
    @DisplayName("초로_볼_수_없는_값은_거부한다")
    void 초로_볼_수_없는_값은_거부한다() {
        assertThatThrownBy(() -> clock.observe(0))
                .isInstanceOf(IllegalStateException.class);
        // 밀리초를 받은 경우는 오히려 커서 안 걸린다. 작은 쪽만 막는다.
        assertThatThrownBy(() -> clock.observe(1_700_000_000L))
                .isInstanceOf(IllegalStateException.class);
    }
}

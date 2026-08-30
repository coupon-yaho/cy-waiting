package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 실패가 <b>얼마나 이어졌는지</b>로 백오프 단계를 낸다 (F7 · 8.2.5).
 *
 * <p>요청 수로 세면 안 된다. 피크에서는 한 노드가 초당 수천 건을 처리하므로,
 * 레디스가 끊긴 순간 카운터가 밀리초 만에 상한에 닿는다. 그러면 첫 실패자와
 * 1초 뒤 실패자가 같은 안내를 받고, 백오프가 있으나 마나가 된다.
 */
class FailureAgeTest {

    private static final Instant 시작 = Instant.parse("2026-08-30T00:00:00Z");
    private static final Duration 단위 = Duration.ofSeconds(2);

    @Test
    @DisplayName("첫_실패는_첫_단계다")
    void 첫_실패는_첫_단계다() {
        assertThat(new FailureAge().stepAt(시작, 단위)).isEqualTo(1);
    }

    /** 같은 순간에 만 건이 실패해도 단계는 안 오른다. 오르면 요청 수로 세는 셈이다. */
    @Test
    @DisplayName("같은_순간의_실패가_단계를_안_올린다")
    void 같은_순간의_실패가_단계를_안_올린다() {
        FailureAge age = new FailureAge();

        for (int i = 0; i < 10_000; i++) {
            assertThat(age.stepAt(시작, 단위)).isEqualTo(1);
        }
    }

    /** 이어진 시간만큼 오른다. 단위마다 한 계단이다. */
    @Test
    @DisplayName("이어진_시간만큼_단계가_오른다")
    void 이어진_시간만큼_단계가_오른다() {
        FailureAge age = new FailureAge();
        age.stepAt(시작, 단위);

        assertThat(age.stepAt(시작.plusSeconds(2), 단위)).isEqualTo(2);
        assertThat(age.stepAt(시작.plusSeconds(4), 단위)).isEqualTo(3);
        assertThat(age.stepAt(시작.plusSeconds(5), 단위)).as("단위 안에서는 그대로")
                .isEqualTo(3);
    }

    /** 성공하면 처음으로 돌아간다. 안 그러면 회복한 뒤에도 멀리 보낸다. */
    @Test
    @DisplayName("성공하면_처음으로_돌아간다")
    void 성공하면_처음으로_돌아간다() {
        FailureAge age = new FailureAge();
        age.stepAt(시작, 단위);
        age.stepAt(시작.plusSeconds(10), 단위);

        age.cleared();

        assertThat(age.stepAt(시작.plusSeconds(11), 단위)).isEqualTo(1);
    }

    /**
     * <b>시계가 뒤로 가도 단계가 안 튄다.</b>
     *
     * <p>복제본 승격이나 NTP 보정으로 시각이 되돌아가면 경과가 음수가 된다.
     * 그것을 그대로 나누면 단계가 0 이나 음수가 되어 상한 아래로 떨어진다.
     */
    @Test
    @DisplayName("시계가_뒤로_가도_첫_단계를_지킨다")
    void 시계가_뒤로_가도_첫_단계를_지킨다() {
        FailureAge age = new FailureAge();
        age.stepAt(시작, 단위);

        assertThat(age.stepAt(시작.minusSeconds(30), 단위)).isEqualTo(1);
    }
}

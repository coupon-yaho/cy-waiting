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

    /** 이만큼 실패 없이 성공해야 푼다. 성공 하나로 풀면 부분 장애에서 안 걸린다. */
    private static final Duration 해제_유예 = Duration.ofSeconds(5);

    @Test
    @DisplayName("첫_실패는_첫_단계다")
    void 첫_실패는_첫_단계다() {
        FailureAge age = new FailureAge();
        age.failed(시작);

        assertThat(age.stepAt(시작, 단위)).isEqualTo(1);
    }

    /** 같은 순간에 만 건이 실패해도 단계는 안 오른다. 오르면 요청 수로 세는 셈이다. */
    @Test
    @DisplayName("같은_순간의_실패가_단계를_안_올린다")
    void 같은_순간의_실패가_단계를_안_올린다() {
        FailureAge age = new FailureAge();

        for (int i = 0; i < 10_000; i++) {
            age.failed(시작);
            assertThat(age.stepAt(시작, 단위)).isEqualTo(1);
        }
    }

    /** 이어진 시간만큼 오른다. 단위마다 한 계단이다. */
    @Test
    @DisplayName("이어진_시간만큼_단계가_오른다")
    void 이어진_시간만큼_단계가_오른다() {
        FailureAge age = new FailureAge();
        age.failed(시작);

        assertThat(age.stepAt(시작.plusSeconds(2), 단위)).isEqualTo(2);
        assertThat(age.stepAt(시작.plusSeconds(4), 단위)).isEqualTo(3);
        assertThat(age.stepAt(시작.plusSeconds(5), 단위)).as("단위 안에서는 그대로")
                .isEqualTo(3);
    }

    /** 성공이 이어지면 처음으로 돌아간다. 안 그러면 회복한 뒤에도 멀리 보낸다. */
    @Test
    @DisplayName("성공이_이어지면_처음으로_돌아간다")
    void 성공이_이어지면_처음으로_돌아간다() {
        FailureAge age = new FailureAge();
        age.failed(시작);
        age.failed(시작.plusSeconds(10));

        age.cleared(시작.plusSeconds(10 + 해제_유예.toSeconds()), 해제_유예);

        assertThat(age.stepAt(시작.plusSeconds(30), 단위)).isEqualTo(1);
    }

    /**
     * <b>성공 하나가 실패 이력을 지우면 안 된다.</b>
     *
     * <p>샤드 하나가 죽으면 사용자의 일부만 실패한다. 피크에서는 성공이 초당
     * 수천 건이라, 성공마다 지우면 실패 사이에 반드시 성공이 끼어 단계가 영원히
     * 1 에 머문다. 백오프가 전면 장애에서만 도는 셈이고, 현장에서 더 흔한 것은
     * 부분 장애다.
     */
    @Test
    @DisplayName("성공_하나로는_안_지운다")
    void 성공_하나로는_안_지운다() {
        FailureAge age = new FailureAge();
        age.failed(시작);

        // 실패와 성공이 섞이는 구간을 그대로 만든다.
        for (int i = 1; i <= 10; i++) {
            age.cleared(시작.plusSeconds(i), 해제_유예);
            age.failed(시작.plusSeconds(i));
        }

        assertThat(age.stepAt(시작.plusSeconds(10), 단위))
                .as("실패가 이어지는 동안은 단계가 유지된다").isGreaterThan(1);
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
        age.failed(시작);

        assertThat(age.stepAt(시작.minusSeconds(30), 단위)).isEqualTo(1);
    }

    /**
     * <b>늦게 도착한 옛 표본이 마지막 실패를 뒤로 밀면 안 된다.</b>
     *
     * <p>동시에 들어온 실패들은 각자 다른 순간을 들고 온다. 늦게 처리된 옛
     * 것이 새 것을 덮으면 해제 유예가 실제보다 일찍 차고, 그러면 장애가
     * 이어지는데도 백오프가 풀린다.
     */
    @Test
    @DisplayName("옛_실패가_마지막_실패를_안_민다")
    void 옛_실패가_마지막_실패를_안_민다() {
        FailureAge age = new FailureAge();
        age.failed(시작);
        age.failed(시작.plusSeconds(10));
        age.failed(시작.plusSeconds(3));

        // 마지막 실패는 10초 지점이므로, 그로부터 유예가 지나야 풀린다.
        age.cleared(시작.plusSeconds(12), 해제_유예);
        assertThat(age.stepAt(시작.plusSeconds(12), 단위)).as("아직 안 풀렸다")
                .isGreaterThan(1);

        age.cleared(시작.plusSeconds(16), 해제_유예);
        assertThat(age.stepAt(시작.plusSeconds(16), 단위)).isEqualTo(1);
    }

    /**
     * <b>읽는 것과 적는 것을 가른다.</b>
     *
     * <p>단계를 읽는 것만으로 실패가 시작되면, 조회는 성공했는데 응답을 쓰다
     * 끊긴 경우까지 뒷단 장애로 기록된다. 클라이언트가 끊은 것이 뒷단 타이머를
     * 오염시키면 그다음 진짜 장애의 단계가 이미 올라가 있다.
     */
    @Test
    @DisplayName("읽기만_해서는_실패가_시작되지_않는다")
    void 읽기만_해서는_실패가_시작되지_않는다() {
        FailureAge age = new FailureAge();

        assertThat(age.stepAt(시작, 단위)).isEqualTo(1);
        assertThat(age.stepAt(시작.plusSeconds(60), 단위))
                .as("한참 뒤에 읽어도 첫 단계다").isEqualTo(1);
    }
}

package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 회복 공통 기준 RC1~RC6 (8.0.4 · 4절).
 *
 * <p>재는 것은 판정기 자신이다. <b>위반을 일부러 만들어 잡히는지</b> 본다.
 */
// 판정기가 시나리오보다 먼저다. 시나리오마다 판정을 다시 쓰면 "전 시나리오
// 초과 발급 0" 같은 게이트가 시나리오마다 다른 것을 재게 된다.
// 통과만 확인하면 아무것도 안 잡는 판정기도 초록이다.
@Tag("chaos")
class RecoveryCriteriaTest {

    private static final Duration 회복_한계 = Duration.ofSeconds(30);

    /** RC1 — 재고보다 많이 나가면 제품이 성립하지 않는다. */
    @Test
    @DisplayName("초과_발급을_잡는다")
    void 초과_발급을_잡는다() {
        assertThat(RecoveryCriteria.overIssued(100, 100)).isEmpty();
        assertThat(RecoveryCriteria.overIssued(101, 100))
                .hasValueSatisfying(v -> assertThat(v).contains("RC1"));
    }

    /** 발급이 재고보다 적은 것은 위반이 아니다. 미달은 지연이지 사고가 아니다. */
    @Test
    @DisplayName("미달은_위반이_아니다")
    void 미달은_위반이_아니다() {
        assertThat(RecoveryCriteria.overIssued(40, 100)).isEmpty();
    }

    /**
     * RC2 — 순번은 뒤로 가면 안 된다.
     *
     * <p>사용자가 받은 순번을 시간 순으로 늘어놓고 본다. 한 번이라도 커지면
     * 그가 뒤로 밀린 것이다.
     */
    @Test
    @DisplayName("순번_역행을_잡는다")
    void 순번_역행을_잡는다() {
        assertThat(RecoveryCriteria.rankRegressed(List.of(100L, 80L, 80L, 20L))).isEmpty();
        assertThat(RecoveryCriteria.rankRegressed(List.of(100L, 80L, 120L)))
                .hasValueSatisfying(v -> assertThat(v).contains("RC2"));
    }

    /** 같은 순번이 이어지는 것은 역행이 아니다. 배분이 안 돈 틱일 뿐이다. */
    @Test
    @DisplayName("같은_순번은_역행이_아니다")
    void 같은_순번은_역행이_아니다() {
        assertThat(RecoveryCriteria.rankRegressed(List.of(50L, 50L, 50L))).isEmpty();
    }

    /** RC3 — 회복 뒤 30초 안에 판정이 정상으로 돌아와야 한다. */
    @Test
    @DisplayName("느린_판정_복귀를_잡는다")
    void 느린_판정_복귀를_잡는다() {
        assertThat(RecoveryCriteria.slowVerdictReturn(Duration.ofSeconds(12), 회복_한계))
                .isEmpty();
        assertThat(RecoveryCriteria.slowVerdictReturn(Duration.ofSeconds(31), 회복_한계))
                .hasValueSatisfying(v -> assertThat(v).contains("RC3"));
    }

    /** 영영 안 돌아온 경우도 잡는다. 안 잡으면 가장 나쁜 판이 초록이다. */
    @Test
    @DisplayName("영영_안_돌아온_것도_잡는다")
    void 영영_안_돌아온_것도_잡는다() {
        assertThat(RecoveryCriteria.slowVerdictReturn(null, 회복_한계))
                .hasValueSatisfying(v -> assertThat(v).contains("RC3"));
    }

    /**
     * RC4 — <b>가장 중요하다.</b> 나머지가 다 통과해도 이것이 깨지면 회복이 곧
     * 2차 장애다.
     */
    @Test
    @DisplayName("회복_버스트를_잡는다")
    void 회복_버스트를_잡는다() {
        assertThat(RecoveryCriteria.recoveryBurst(100, 118)).isEmpty();
        assertThat(RecoveryCriteria.recoveryBurst(100, 121))
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
    }

    /**
     * 정상 구간을 못 쟀으면 비교할 것이 없다. 통과로 넘기면 게이트가 사라진다.
     *
     * <p>음수를 같이 본다. 나눗셈만 두면 음수 정상값에서 비율이 음수가 되어
     * <b>한계 아래로 통과한다</b> — 못 잰 판이 가장 조용히 지나간다.
     */
    @Test
    @DisplayName("정상_구간을_못_쟀으면_잡는다")
    void 정상_구간을_못_쟀으면_잡는다() {
        assertThat(RecoveryCriteria.recoveryBurst(0, 50))
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
        assertThat(RecoveryCriteria.recoveryBurst(-100, 50))
                .as("음수는 관측이 아니다")
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
    }

    /** RC5 — 장애 중 줄에 있던 사람이 자기 자리를 지켜야 한다. */
    @Test
    @DisplayName("자리를_잃은_것을_잡는다")
    void 자리를_잃은_것을_잡는다() {
        assertThat(RecoveryCriteria.seatLost(List.of(10.0, 10.0), List.of(10.0, 10.0)))
                .isEmpty();
        assertThat(RecoveryCriteria.seatLost(List.of(10.0, 10.0), List.of(10.0, 99.0)))
                .hasValueSatisfying(v -> assertThat(v).contains("RC5"));
    }

    /** 사람이 사라진 것도 자리를 잃은 것이다. 수가 줄면 누군가 걷혔다. */
    @Test
    @DisplayName("줄에서_사라진_것도_잡는다")
    void 줄에서_사라진_것도_잡는다() {
        assertThat(RecoveryCriteria.seatLost(List.of(10.0, 20.0), List.of(10.0)))
                .hasValueSatisfying(v -> assertThat(v).contains("RC5"));
    }

    /** RC6 — 지표가 장애 이전 값으로 돌아와야 한다. */
    @Test
    @DisplayName("안_수렴한_지표를_잡는다")
    void 안_수렴한_지표를_잡는다() {
        assertThat(RecoveryCriteria.notConverged("크레딧", 1_000, 1_050)).isEmpty();
        assertThat(RecoveryCriteria.notConverged("크레딧", 1_000, 1_400))
                .hasValueSatisfying(v -> assertThat(v).contains("RC6").contains("크레딧"));
    }

    /** 아래로 안 돌아온 것도 잡는다. 한쪽만 보면 절반을 놓친다. */
    @Test
    @DisplayName("아래로_안_돌아온_것도_잡는다")
    void 아래로_안_돌아온_것도_잡는다() {
        assertThat(RecoveryCriteria.notConverged("크레딧", 1_000, 600))
                .hasValueSatisfying(v -> assertThat(v).contains("RC6"));
    }

    /** 여섯을 한 번에 본다. 하나라도 깨지면 그 이름이 남아야 원인을 찾는다. */
    @Test
    @DisplayName("깨진_기준의_이름이_남는다")
    void 깨진_기준의_이름이_남는다() {
        List<String> 위반 = RecoveryCriteria.violations(
                RecoveryCriteria.overIssued(101, 100),
                RecoveryCriteria.recoveryBurst(100, 200));

        assertThat(위반).hasSize(2);
        assertThat(위반.getFirst()).contains("RC1");
    }

    /** 다 통과하면 비어야 한다. 안 비면 시나리오가 영영 빨갛다. */
    @Test
    @DisplayName("다_통과하면_비어_있다")
    void 다_통과하면_비어_있다() {
        assertThat(RecoveryCriteria.violations(
                RecoveryCriteria.overIssued(100, 100),
                RecoveryCriteria.recoveryBurst(100, 110))).isEmpty();
    }

    /** 말이 안 되는 재고는 만들 때 막는다. 조용히 통과하면 RC1 이 사라진다. */
    @Test
    @DisplayName("음수_재고는_거절한다")
    void 음수_재고는_거절한다() {
        assertThatThrownBy(() -> RecoveryCriteria.overIssued(0, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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
     * <b>음수 발급 수는 통과가 아니다.</b>
     *
     * <p>발급 수가 음수라는 것은 세다가 깨졌다는 뜻이다. 재고보다 작으니
     * 통과시키면, 못 잰 판이 초과 발급 0 으로 기록된다.
     */
    @Test
    @DisplayName("음수_발급은_못_잰_것이다")
    void 음수_발급은_못_잰_것이다() {
        assertThat(RecoveryCriteria.overIssued(-1, 100))
                .hasValueSatisfying(v -> assertThat(v).contains("RC1"));
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

    /** 음수 경과는 측정이 깨진 것이다. 한계보다 작다고 통과하면 안 된다. */
    @Test
    @DisplayName("음수_복귀_시간은_못_잰_것이다")
    void 음수_복귀_시간은_못_잰_것이다() {
        assertThat(RecoveryCriteria.slowVerdictReturn(Duration.ofSeconds(-1), 회복_한계))
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
        // **경계를 밟는다.** 1.18 과 1.21 만 보면 `<=` 를 `<` 로 바꿔도 아무도
        // 모른다 — 이 브랜치의 근거가 "실효 임계가 문서와 달랐다" 였다.
        assertThat(RecoveryCriteria.recoveryBurst(100, 120))
                .as("정확히 한계면 통과다").isEmpty();
        assertThat(RecoveryCriteria.recoveryBurst(1_000, 1_201))
                .as("한계를 조금이라도 넘으면 위반이다").isPresent();
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
                .as("음수 정상값은 관측이 아니다")
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
        assertThat(RecoveryCriteria.recoveryBurst(100, -50))
                .as("음수 봉우리도 관측이 아니다")
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
        assertThat(RecoveryCriteria.recoveryBurst(Double.POSITIVE_INFINITY, 50))
                .as("무한대 정상값이면 어떤 봉우리도 작아 보인다")
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
        assertThat(RecoveryCriteria.recoveryBurst(100, Double.NaN))
                .as("셀 수 없는 값은 비교가 성립하지 않는다")
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
    }

    /**
     * <b>봉우리가 0 인데 정상 구간이 0 이 아니면 위반이다.</b>
     *
     * <p>나눗셈만 두면 0 은 "버스트가 없다" 로 읽혀 가장 조용히 통과한다.
     * 실제로는 회복 구간에 뒷단이 하나도 못 받은 것이고, 그건 아직 안 돌아온
     * 것이지 잘 돌아온 것이 아니다. 못 잰 것을 통과로 넘기지 않는 원칙이
     * 여기에도 걸린다.
     */
    @Test
    @DisplayName("회복_구간이_비면_잡는다")
    void 회복_구간이_비면_잡는다() {
        assertThat(RecoveryCriteria.recoveryBurst(100, 0))
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
    }

    /**
     * <b>증폭률 — 뒷단 도착이 클라이언트가 보낸 수를 넘으면 게이트웨이가 스스로
     * 만든 유입이다.</b>
     */
    // 고정 창으로 버스트를 재면 창 밖에서 몰아친 것을 못 본다. 회복을 기다리는
    // 동안 보낸 수를 알고 있으면 도착을 그 수로 나누어 대기 길이를 상쇄할 수
    // 있다. 창을 옮길 필요가 없으므로 재는 도구가 재는 대상을 안 밀어 올린다.
    @Test
    @DisplayName("증폭을_잡는다")
    void 증폭을_잡는다() {
        assertThat(RecoveryCriteria.amplified(100, 100))
                .as("보낸 만큼 닿은 것은 중복이 아니다").isEmpty();
        // **한 건이면 충분하다.** 발급 경로에서 도착 1 건 초과는 요청 1 건
        // 중복이고, 그건 곧 초과 발급이다. 버스트 한계 1.2 를 같이 쓰면
        // 100 건에 18 건 중복이 통과한다.
        assertThat(RecoveryCriteria.amplified(100, 101))
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
        assertThat(RecoveryCriteria.amplified(100, 118))
                .as("버스트 한계를 같이 쓰면 여기가 통과한다").isPresent();
    }

    /** 재전송이 없으면 도착이 보낸 수보다 적을 수 있다. 그건 증폭이 아니다. */
    @Test
    @DisplayName("도착이_적은_것은_증폭이_아니다")
    void 도착이_적은_것은_증폭이_아니다() {
        assertThat(RecoveryCriteria.amplified(100, 3)).isEmpty();
    }

    /**
     * <b>보냈는데 하나도 안 닿았으면 위반이다.</b>
     *
     * <p>비율 0 은 "증폭 없음" 으로 읽혀 가장 조용히 통과한다. 봉우리 0 을
     * 위반으로 돌린 것과 같은 상황이라 여기만 다르면 앞뒤가 안 맞는다.
     */
    @Test
    @DisplayName("하나도_안_닿으면_잡는다")
    void 하나도_안_닿으면_잡는다() {
        assertThat(RecoveryCriteria.amplified(100, 0))
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
    }

    /** 보낸 수를 모르면 비교가 성립하지 않는다. 통과로 안 넘긴다. */
    @Test
    @DisplayName("보낸_수를_모르면_잡는다")
    void 보낸_수를_모르면_잡는다() {
        assertThat(RecoveryCriteria.amplified(0, 50))
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
        assertThat(RecoveryCriteria.amplified(-10, 50))
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
        assertThat(RecoveryCriteria.amplified(100, -1))
                .as("음수 도착은 관측이 아니다")
                .hasValueSatisfying(v -> assertThat(v).contains("RC4"));
    }

    /** RC5 — 장애 중 줄에 있던 사람이 자기 자리를 지켜야 한다. */
    @Test
    @DisplayName("자리를_잃은_것을_잡는다")
    void 자리를_잃은_것을_잡는다() {
        assertThat(RecoveryCriteria.seatLost(Map.of("a", 10.0), Map.of("a", 10.0))).isEmpty();
        assertThat(RecoveryCriteria.seatLost(Map.of("a", 10.0), Map.of("a", 99.0)))
                .hasValueSatisfying(v -> assertThat(v).contains("RC5").contains("a"));
    }

    /** 사람이 사라진 것도 자리를 잃은 것이다. 걷힌 사람은 새 순번으로 다시 선다. */
    @Test
    @DisplayName("줄에서_사라진_것도_잡는다")
    void 줄에서_사라진_것도_잡는다() {
        assertThat(RecoveryCriteria.seatLost(Map.of("a", 10.0, "b", 20.0), Map.of("a", 10.0)))
                .hasValueSatisfying(v -> assertThat(v).contains("RC5").contains("b"));
    }

    /**
     * <b>score 만 비교하면 안 된다.</b>
     *
     * <p>같은 score 를 가진 사람이 여럿이면 값의 목록은 같은데 주인이 바뀔 수
     * 있다. 사람이 바뀌었는데 목록이 같으면 자리를 잃은 것을 못 본다.
     */
    @Test
    @DisplayName("같은_score_라도_주인이_바뀌면_잡는다")
    void 같은_score_라도_주인이_바뀌면_잡는다() {
        assertThat(RecoveryCriteria.seatLost(
                Map.of("a", 10.0, "b", 10.0), Map.of("a", 10.0, "c", 10.0)))
                .hasValueSatisfying(v -> assertThat(v).contains("RC5").contains("b"));
    }

    /** 새 사람이 끼어드는 것은 기존 사람의 자리와 무관하다. 거짓 위반을 만들면 안 된다. */
    @Test
    @DisplayName("새_사람이_늘어난_것은_위반이_아니다")
    void 새_사람이_늘어난_것은_위반이_아니다() {
        assertThat(RecoveryCriteria.seatLost(
                Map.of("a", 10.0), Map.of("a", 10.0, "b", 20.0))).isEmpty();
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

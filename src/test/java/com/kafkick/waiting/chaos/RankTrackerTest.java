package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 사용자별 순번과 자리를 따라간다 (8.3.2 · RC2·RC5).
 *
 * <p>순번은 뒤로 가면 안 되고, 줄에 있던 사람은 자기 자리를 지켜야 한다.
 */
// 응답을 사용자별로 모으지 않으면 전체 평균만 보게 된다. 한 사람이 뒤로 밀린
// 것은 평균에 안 잡힌다 — 밀린 사람과 당겨진 사람이 상쇄되기 때문이다.
@Tag("chaos")
class RankTrackerTest {

    @Test
    @DisplayName("사용자별로_따로_따라간다")
    void 사용자별로_따로_따라간다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 100, 10L);
        tracker.waiting("b", 50, 20L);
        tracker.waiting("a", 80, 10L);

        assertThat(tracker.members()).containsExactlyInAnyOrder("a", "b");
        assertThat(tracker.ranksOf("a")).containsExactly(100L, 80L);
    }

    /** RC2 — 한 사람이라도 뒤로 밀리면 잡는다. 평균으로는 안 잡힌다. */
    @Test
    @DisplayName("한_사람의_역행도_잡는다")
    void 한_사람의_역행도_잡는다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 100, 10L);
        tracker.waiting("a", 80, 10L);
        tracker.waiting("b", 50, 20L);
        tracker.waiting("b", 90, 20L);

        assertThat(tracker.regressions()).hasSize(1);
        assertThat(tracker.regressions().getFirst()).contains("RC2").contains("b");
    }

    /** 아무도 안 밀렸으면 비어야 한다. 안 비면 시나리오가 영영 빨갛다. */
    @Test
    @DisplayName("아무도_안_밀렸으면_비어_있다")
    void 아무도_안_밀렸으면_비어_있다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 100, 10L);
        tracker.waiting("a", 60, 10L);

        assertThat(tracker.regressions()).isEmpty();
    }

    /**
     * RC5 — 자리가 바뀌면 잡는다.
     *
     * <p>score 가 바뀌었다는 것은 걷혔다가 다시 섰다는 뜻이고, 재입장은 새
     * 순번이라 그 자체로 순번 역행이다.
     */
    @Test
    @DisplayName("자리가_바뀌면_잡는다")
    void 자리가_바뀌면_잡는다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 100, 10L);
        tracker.waiting("a", 90, 99L);

        assertThat(tracker.seatChanges()).hasSize(1);
        assertThat(tracker.seatChanges().getFirst()).contains("RC5").contains("a");
    }

    /** 자리를 지켰으면 비어야 한다. */
    @Test
    @DisplayName("자리를_지켰으면_비어_있다")
    void 자리를_지켰으면_비어_있다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 100, 10L);
        tracker.waiting("a", 90, 10L);

        assertThat(tracker.seatChanges()).isEmpty();
    }

    /** 사람마다 관측이 하나뿐이면 볼 수가 없었던 것이다. 통과로 넘기면 안 된다. */
    @Test
    @DisplayName("한_번씩만_봤으면_판정_불가다")
    void 한_번씩만_봤으면_판정_불가다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 100, 10L);

        // **빈 목록은 "역행이 없었다" 로 읽힌다.** 관측이 사람마다 하나뿐이면
        // 비교할 짝이 없어 볼 수가 없었던 것인데, 그것을 통과로 넘기면 폴링을
        // 한 바퀴만 돌고 끝난 시나리오가 RC2 를 통과한 것으로 적힌다.
        assertThat(tracker.regressions())
                .anySatisfy(why -> assertThat(why).contains("RC2").contains("볼 수 없었다"));
        assertThat(tracker.seatChanges())
                .anySatisfy(why -> assertThat(why).contains("RC5").contains("볼 수 없었다"));
    }

    /** 한 사람이라도 두 번 봤으면 판정이 선다. 한 번만 본 사람은 그대로 둔다. */
    @Test
    @DisplayName("한_사람이라도_두_번_봤으면_판정한다")
    void 한_사람이라도_두_번_봤으면_판정한다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 100, 10L);
        tracker.waiting("a", 90, 10L);
        tracker.waiting("b", 50, 20L);

        assertThat(tracker.regressions()).isEmpty();
        assertThat(tracker.seatChanges()).isEmpty();
    }

    /** 줄을 떠난 사람은 끝까지 따라간 것이다. 한 번만 봤어도 판정이 선다. */
    @Test
    @DisplayName("떠난_사람이_있으면_판정한다")
    void 떠난_사람이_있으면_판정한다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 100, 10L);
        tracker.admitted("a");

        assertThat(tracker.regressions()).isEmpty();
        assertThat(tracker.seatChanges()).isEmpty();
    }

    /** 아무도 안 봤으면 통과가 아니라 못 잰 것이다. 통과로 넘기면 게이트가 사라진다. */
    @Test
    @DisplayName("아무도_안_봤으면_못_잰_것이다")
    void 아무도_안_봤으면_못_잰_것이다() {
        assertThat(new RankTracker().regressions())
                .anySatisfy(v -> assertThat(v).contains("관측이 없다"));
    }

    /**
     * <b>앞으로 당겨진 것도 자리를 잃은 것이다.</b>
     *
     * <p>뒤로 밀린 것만 보면 추월당한 쪽만 잡고 추월한 쪽을 놓친다. 둘은 같은
     * 사건의 양면이고, 추월은 불변식 4 의 위반 그 자체다.
     */
    @Test
    @DisplayName("앞으로_당겨진_것도_잡는다")
    void 앞으로_당겨진_것도_잡는다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 100, 50L);
        tracker.waiting("a", 10, 5L);

        assertThat(tracker.seatChanges()).hasSize(1);
    }

    /** 자리 관측이 없어도 못 잰 것이다. 순번 쪽과 같은 자로 본다. */
    @Test
    @DisplayName("자리_관측이_없어도_못_잰_것이다")
    void 자리_관측이_없어도_못_잰_것이다() {
        assertThat(new RankTracker().seatChanges())
                .anySatisfy(v -> assertThat(v).contains("관측이 없다"));
    }

    /** 입장은 자리를 안 갖는 정상 상태다. 자리 이동으로 읽으면 전부 위반이 된다. */
    @Test
    @DisplayName("입장은_자리_상실이_아니다")
    void 입장은_자리_상실이_아니다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 100, 10L);
        tracker.admitted("a");

        assertThat(tracker.seatChanges()).isEmpty();
    }

    /** 도달 불가능한 조합은 만들 때 막는다. 센티널이 들어오면 정상이 위반이 된다. */
    @Test
    @DisplayName("음수_순번과_자리는_거절한다")
    void 음수_순번과_자리는_거절한다() {
        RankTracker tracker = new RankTracker();

        assertThatThrownBy(() -> tracker.waiting("a", -1, 10L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tracker.waiting("a", 10, -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>떠난 뒤의 관측을 옛 세션과 안 잇는다.</b>
     *
     * <p>이으면 새로 선 줄의 순번이 옛 순번보다 크다는 이유로 역행이 되고, 새
     * 자리는 자리 상실이 된다. 둘 다 실제로는 다른 줄의 일이다. 늦게 도착한
     * 옛 응답도 같은 모양으로 들어온다.
     */
    @Test
    @DisplayName("떠난_뒤의_관측은_거절한다")
    void 떠난_뒤의_관측은_거절한다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 10, 5L);
        tracker.admitted("a");

        assertThatThrownBy(() -> tracker.waiting("a", 400, 99L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("떠난");
    }

    /** 줄에서 걷힌 뒤도 마찬가지다. 재입장은 새 순번이라 옛 것과 비교할 수 없다. */
    @Test
    @DisplayName("걷힌_뒤의_관측도_거절한다")
    void 걷힌_뒤의_관측도_거절한다() {
        RankTracker tracker = new RankTracker();
        tracker.waiting("a", 10, 5L);
        tracker.leftQueue("a");

        assertThatThrownBy(() -> tracker.waiting("a", 20, 7L))
                .isInstanceOf(IllegalStateException.class);
    }
}

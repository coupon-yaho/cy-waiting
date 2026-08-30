package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

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
        tracker.saw("a", 100, 10.0);
        tracker.saw("b", 50, 20.0);
        tracker.saw("a", 80, 10.0);

        assertThat(tracker.members()).containsExactlyInAnyOrder("a", "b");
        assertThat(tracker.ranksOf("a")).containsExactly(100L, 80L);
    }

    /** RC2 — 한 사람이라도 뒤로 밀리면 잡는다. 평균으로는 안 잡힌다. */
    @Test
    @DisplayName("한_사람의_역행도_잡는다")
    void 한_사람의_역행도_잡는다() {
        RankTracker tracker = new RankTracker();
        tracker.saw("a", 100, 10.0);
        tracker.saw("a", 80, 10.0);
        tracker.saw("b", 50, 20.0);
        tracker.saw("b", 90, 20.0);

        assertThat(tracker.regressions()).hasSize(1);
        assertThat(tracker.regressions().getFirst()).contains("RC2").contains("b");
    }

    /** 아무도 안 밀렸으면 비어야 한다. 안 비면 시나리오가 영영 빨갛다. */
    @Test
    @DisplayName("아무도_안_밀렸으면_비어_있다")
    void 아무도_안_밀렸으면_비어_있다() {
        RankTracker tracker = new RankTracker();
        tracker.saw("a", 100, 10.0);
        tracker.saw("a", 60, 10.0);

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
        tracker.saw("a", 100, 10.0);
        tracker.saw("a", 90, 99.0);

        assertThat(tracker.seatChanges()).hasSize(1);
        assertThat(tracker.seatChanges().getFirst()).contains("RC5").contains("a");
    }

    /** 자리를 지켰으면 비어야 한다. */
    @Test
    @DisplayName("자리를_지켰으면_비어_있다")
    void 자리를_지켰으면_비어_있다() {
        RankTracker tracker = new RankTracker();
        tracker.saw("a", 100, 10.0);
        tracker.saw("a", 90, 10.0);

        assertThat(tracker.seatChanges()).isEmpty();
    }

    /** 한 번만 본 사람은 비교할 것이 없다. 없는 위반을 만들면 안 된다. */
    @Test
    @DisplayName("한_번만_본_사람은_판정하지_않는다")
    void 한_번만_본_사람은_판정하지_않는다() {
        RankTracker tracker = new RankTracker();
        tracker.saw("a", 100, 10.0);

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
}

package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 표시 순위는 뒤로 가지 않는다 — 타협 불가 기준이다.
 *
 * <p>검증 범위는 <b>"입력이 단조면 출력도 단조"</b> 까지다. {@code localRank} 는
 * 어댑터의 {@code ZCOUNT} 결과라 <b>입력이 단조라는 보장은 Phase 3 의 G3.11</b> 이 진다.
 */
class RankMonotonicityTest {

    private static final long SEED = 20260819L;
    private static final int SEQUENCES = 100_000;

    @Test
    @DisplayName("무작위_10만_시퀀스에서_표시_순위가_증가하지_않는다")
    void 무작위_10만_시퀀스에서_표시_순위가_증가하지_않는다() {
        Random rnd = new Random(SEED);
        int violations = 0;

        for (int seq = 0; seq < SEQUENCES; seq++) {
            int shards = rnd.nextInt(1, 17);
            long localRank = rnd.nextLong(0, 20_000);
            long previous = Long.MAX_VALUE;

            // 단조 감소하는 localRank 를 주입한다 — 줄이 빠지는 모습이다
            for (int step = 0; step < 5 && localRank >= 0; step++) {
                long shown = RankEstimator.globalRank(localRank, shards);
                if (shown > previous) {
                    violations++;
                }
                previous = shown;
                localRank -= rnd.nextLong(0, 500);
            }
        }

        assertThat(violations)
                .withFailMessage("순위 역행 %d 건 (시드 %d)", violations, SEED)
                .isZero();
    }

    @Test
    @DisplayName("샤드가_하나면_로컬_순위가_곧_전역_순위다")
    void 샤드가_하나면_로컬_순위가_곧_전역_순위다() {
        assertThat(RankEstimator.globalRank(3000, 1)).isEqualTo(3000);
    }

    @Test
    @DisplayName("전역_순위는_로컬_순위에_샤드_수를_곱한_값이다")
    void 전역_순위는_로컬_순위에_샤드_수를_곱한_값이다() {
        assertThat(RankEstimator.globalRank(100, 16)).isEqualTo(1600);
    }

    @Test
    @DisplayName("샤드_수가_0이하면_1로_취급한다")
    void 샤드_수가_0이하면_1로_취급한다() {
        assertThat(RankEstimator.globalRank(100, 0)).isEqualTo(100);
    }

    @Test
    @DisplayName("곱셈이_넘치면_음수가_아니라_포화한다")
    void 곱셈이_넘치면_음수가_아니라_포화한다() {
        // 넘치면 음수가 되어 순위가 뒤로 간다 — 역행 0 을 스스로 깬다.
        assertThat(RankEstimator.globalRank(Long.MAX_VALUE, 2)).isEqualTo(Long.MAX_VALUE);
        assertThat(RankEstimator.globalRank(Long.MAX_VALUE / 2 + 1, 2)).isEqualTo(Long.MAX_VALUE);
    }
}

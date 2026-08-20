package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 샤드 분포가 쏠리면 <b>샤딩의 이득이 사라진다</b>.
 *
 * <p>한 샤드만 붐비면 그 큐의 Lua 가 병목이 되고, 나머지 샤드는 놀면서
 * 레디스 커넥션만 차지한다.
 */
class ShardDistributionTest {

    private static final int SHARDS = 16;
    private static final int SAMPLES = 100_000;

    @Test
    @DisplayName("무작위_10만건에서_샤드별_편차가_5퍼센트_미만이다")
    void 무작위_10만건에서_샤드별_편차가_5퍼센트_미만이다() {
        int[] counts = new int[SHARDS];
        for (int i = 0; i < SAMPLES; i++) {
            counts[ShardHash.shardOf("member-" + i, SHARDS)]++;
        }

        double expected = (double) SAMPLES / SHARDS;
        for (int shard = 0; shard < SHARDS; shard++) {
            double deviation = Math.abs(counts[shard] - expected) / expected;
            assertThat(deviation)
                    .withFailMessage(
                            "샤드 %d 편차 %.2f%% (기대 %.0f · 실제 %d)",
                            shard, deviation * 100, expected, counts[shard])
                    .isLessThan(0.05);
        }
    }

    @Test
    @DisplayName("UUID_형식_식별자에서도_고르게_퍼진다")
    void UUID_형식_식별자에서도_고르게_퍼진다() {
        // 실제 memberId 가 어떤 형식일지 모른다. 접두사가 같고 뒤만 다른
        // 경우가 가장 쏠리기 쉬워서 그 형태로도 본다.
        int[] counts = new int[SHARDS];
        for (int i = 0; i < SAMPLES; i++) {
            counts[ShardHash.shardOf("550e8400-e29b-41d4-a716-%012d".formatted(i), SHARDS)]++;
        }

        double expected = (double) SAMPLES / SHARDS;
        for (int shard = 0; shard < SHARDS; shard++) {
            assertThat(Math.abs(counts[shard] - expected) / expected).isLessThan(0.05);
        }
    }
}

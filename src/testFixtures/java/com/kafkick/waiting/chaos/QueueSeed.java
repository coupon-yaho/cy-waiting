package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 줄을 미리 세운다. <b>등록 스크립트가 만드는 상태와 같은 모양이라야 한다</b> —
 * 점수만 넣으면 스위퍼가 그 줄을 못 보고, 그러면 자리 유실 판정이 어떤 장애에서도
 * 안 깨지는 재는 척하는 자리가 된다 (CY-839).
 */
public final class QueueSeed {

    /** 한 샤드만 쓴다. 프로덕션도 지금은 샤드 하나만 허용한다. */
    private static final int SHARDS = 1;

    private static final int SHARD = 0;

    private QueueSeed() {
    }

    /**
     * {@code n} 명을 줄에 세우고 이름별 자리를 돌려준다. 점수는 레디스 시계의
     * 마이크로초라 배분이 올리는 임계와 같은 자에 있고, 생존 신호와 바닥값도
     * 함께 써서 스위퍼가 살아 있는 줄로 읽는다.
     */
    public static Map<String, Double> 줄을_세운다(StatefulRedisConnection<String, String> 연결,
            String couponId, int n, Duration aliveTtl) {
        RedisCommands<String, String> redis = 연결.sync();
        long 지금_마이크로 = 서버_시각_마이크로(redis);
        long 만료_초 = 지금_마이크로 / 1_000_000 + aliveTtl.toSeconds();
        String queue = RedisKeys.queue(couponId, SHARDS, SHARD);
        String alive = RedisKeys.alive(couponId, SHARDS, SHARD);

        Map<String, Double> 자리 = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            String member = "q" + i;
            double score = 지금_마이크로 + i;
            redis.zadd(queue, score, member);
            redis.zadd(alive, 만료_초, member);
            자리.put(member, score);
        }
        redis.set(RedisKeys.maxScore(couponId, SHARDS, SHARD),
                String.valueOf(지금_마이크로 + n));
        return 자리;
    }

    /** 줄에 선 사람들의 지금 자리. 이름으로 짚어야 같은 값을 가진 둘이 안 섞인다. */
    public static Map<String, Double> 자리들(StatefulRedisConnection<String, String> 연결,
            String couponId, int n) {
        RedisCommands<String, String> redis = 연결.sync();
        String queue = RedisKeys.queue(couponId, SHARDS, SHARD);
        Map<String, Double> 자리 = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            String member = "q" + i;
            Double score = redis.zscore(queue, member);
            if (score != null) {
                자리.put(member, score);
            }
        }
        return 자리;
    }

    // 서버 시계를 쓴다. 시험 장비의 시계로 점수를 매기면 컨테이너와 어긋난
    // 만큼 배분 임계와 자가 안 맞아, 심어 둔 줄이 통째로 임계 밖으로 나간다.
    private static long 서버_시각_마이크로(RedisCommands<String, String> redis) {
        List<String> time = redis.time();
        if (time == null || time.size() < 2) {
            throw new IllegalStateException("레디스 시각을 못 읽었다");
        }
        return Long.parseLong(time.get(0)) * 1_000_000L + Long.parseLong(time.get(1));
    }
}

package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;

/**
 * 리더를 죽이거나 내린다 (4.0.1).
 *
 * <p><b>죽음과 종료를 나눈다.</b> 곱게 내리면 락이 즉시 풀려 승계가 빠르지만,
 * 그건 장애 경로가 아니다. 죽이면 락이 lease 만료까지 남아 <b>다음 리더가
 * 기다리는 구간</b>이 생긴다 — G4.2 가 재는 것이 그 구간이다.
 */
public final class LeaderFaults {

    private final StatefulRedisConnection<String, String> redis;

    public LeaderFaults(StatefulRedisConnection<String, String> redis) {
        this.redis = redis;
    }

    public void 리더로_만든다(String ownerId, Duration lease) {
        redis.sync().psetex(RedisKeys.LEADER, lease.toMillis(), ownerId);
    }

    /** 프로세스만 사라진다. 락은 그대로 남는다 — 해제 절차를 못 밟았기 때문이다. */
    public void 프로세스를_죽인다(String ownerId) {
        if (!ownerId.equals(현재_소유자())) {
            throw new IllegalStateException("리더가 아니다: " + ownerId);
        }
        // 아무것도 안 한다. **그것이 죽음이다** — 남는 것은 만료를 기다리는 락뿐.
    }

    /** 종료 경로. 자기 락만 지운다. */
    public void 곱게_내린다(String ownerId) {
        if (ownerId.equals(현재_소유자())) {
            redis.sync().del(RedisKeys.LEADER);
        }
    }

    /** lease 만료를 앞당긴다 — 승계 경로를 재려고 실제 시간을 기다리지 않는다. */
    public void lease를_만료시킨다() {
        redis.sync().del(RedisKeys.LEADER);
    }

    public String 현재_소유자() {
        return redis.sync().get(RedisKeys.LEADER);
    }

    public Duration 남은_lease() {
        long millis = redis.sync().pttl(RedisKeys.LEADER);
        return millis > 0 ? Duration.ofMillis(millis) : Duration.ZERO;
    }
}

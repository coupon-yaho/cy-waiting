package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Clock;
import java.time.Duration;

/**
 * 게이트웨이 노드 수를 흔든다 — {@code N} 이 바뀌는 순간을 만든다 (4.0.3).
 *
 * <p>하트비트는 <b>값에 시각을 담는다.</b> TTL 은 지우는 시점이지 신선한 시점이
 * 아니라, TTL 만 믿으면 낡은 항목이 분모에 남는다.
 */
public final class GatewayNodes {

    /** 키는 {@link RedisKeys} 에서만 만든다 (RD-3). 두 곳에서 만들면 갈라진다. */
    public static final String KEY = RedisKeys.INSTANCES;

    private final StatefulRedisConnection<String, String> redis;
    private final Duration 신선도;
    private final Clock clock;

    public GatewayNodes(StatefulRedisConnection<String, String> redis, Duration 신선도) {
        this(redis, 신선도, Clock.systemUTC());
    }

    /**
     * 시계를 주입받는다 (TS-4).
     *
     * <p>기본은 실제 시계다 — 실물 레디스를 흔드는 것이 이 픽스처의 목적이라
     * 프로덕션이 보는 시각과 같은 축에 있어야 한다.
     */
    public GatewayNodes(StatefulRedisConnection<String, String> redis, Duration 신선도,
            Clock clock) {
        this.redis = redis;
        this.신선도 = 신선도;
        this.clock = clock;
    }

    public void 등록한다(String nodeId) {
        redis.sync().hset(KEY, nodeId, String.valueOf(지금()));
    }

    public void 해제한다(String nodeId) {
        redis.sync().hdel(KEY, nodeId);
    }

    /** 항목은 남기고 시각만 과거로 둔다 — 죽었는데 흔적이 남은 상태다. */
    public void 낡은_하트비트를_심는다(String nodeId, Duration 얼마나_전) {
        redis.sync().hset(KEY, nodeId, String.valueOf(지금() - 얼마나_전.toSeconds()));
    }

    /** 신선한 하트비트만 센다. 이것이 배분의 분모다. */
    public int 살아있는_수() {
        long 하한 = 지금() - 신선도.toSeconds();
        return (int) redis.sync().hgetall(KEY).values().stream()
                .mapToLong(GatewayNodes::초로)
                .filter(at -> at >= 하한)
                .count();
    }

    private static long 초로(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return Long.MIN_VALUE;   // 깨진 값은 낡은 것으로 본다
        }
    }

    private long 지금() {
        return clock.instant().getEpochSecond();
    }
}

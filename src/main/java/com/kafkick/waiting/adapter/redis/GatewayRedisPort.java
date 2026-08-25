package com.kafkick.waiting.adapter.redis;

import java.util.List;
import java.util.Objects;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * 노드가 자기 존재를 알리고, 살아 있는 노드 수를 받아 온다.
 *
 * <p><b>세는 것과 쓰는 것을 나누지 않는다.</b> 나누면 그 사이에 다른 노드가 세어
 * 방금 지운 항목을 살아 있는 것으로 본다 — 분모가 사실보다 커지고, 그만큼 전
 * 노드가 몫을 덜 쓴다.
 */
@Component
public final class GatewayRedisPort {

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> BEAT =
            RedisScript.of(new ClassPathResource("redis/gateway_heartbeat.lua"), List.class);

    private static final RedisScript<Long> LEAVE =
            RedisScript.of(new ClassPathResource("redis/gateway_leave.lua"), Long.class);

    private final ReactiveStringRedisTemplate redis;

    GatewayRedisPort(ReactiveStringRedisTemplate redis) {
        this.redis = Objects.requireNonNull(redis, "redis 는 필수다");
    }

    public static GatewayRedisPort of(ReactiveStringRedisTemplate redis) {
        return new GatewayRedisPort(redis);
    }

    /**
     * 하트비트를 남기고 살아 있는 노드 수를 받는다.
     *
     * @param reapAfterSec 이보다 오래된 항목은 지운다. 시각은 레디스가 찍는다
     */
    public Mono<Integer> beat(String instanceId, long reapAfterSec) {
        return redis.execute(BEAT, List.of(RedisKeys.INSTANCES),
                        List.of(instanceId, Long.toString(reapAfterSec)))
                .next()
                // **첫 칸만 쓴다.** 둘째 칸은 서버 시각이고 분모와 무관하다.
                .map(raw -> (int) ((Number) ((List<?>) raw).get(0)).longValue());
    }

    /** 자발적 종료. 임계를 안 기다리고 즉시 뺀다 — 배포마다 분모가 부풀지 않게. */
    public Mono<Void> leave(String instanceId) {
        return redis.execute(LEAVE, List.of(RedisKeys.INSTANCES), List.of(instanceId))
                .then();
    }
}

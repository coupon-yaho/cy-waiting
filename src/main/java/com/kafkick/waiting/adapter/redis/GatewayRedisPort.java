package com.kafkick.waiting.adapter.redis;

import com.kafkick.waiting.domain.admission.CircuitState;
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
 * 방금 지운 항목을 살아 있는 것으로 본다 — 분모가 사실보다 커진다.
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
    /**
     * 하트비트에 <b>이 노드가 본 서킷을 같이 싣는다</b> (CY-791).
     *
     * <p>안 실으면 배분이 리더 한 대의 로컬 관측으로 전 클러스터의 크레딧을
     * 정한다 — 리더만 정상이면 나머지가 다 열려 있어도 평소 속도로 돈다.
     */
    public Mono<Presence> beat(String instanceId, long reapAfterSec, CircuitState circuit) {
        return redis.execute(BEAT, List.of(RedisKeys.INSTANCES),
                        List.of(instanceId, Long.toString(reapAfterSec), circuit.name()))
                .next()
                // 둘째 칸은 서버 시각이라 분모와 무관하다.
                .map(raw -> {
                    List<?> v = (List<?>) raw;
                    return new Presence((int) ((Number) v.get(0)).longValue(),
                            v.size() > 2 ? (int) ((Number) v.get(2)).longValue() : 0);
                });
    }

    /**
     * 한 판의 관측. <b>둘을 같이 받는다</b> — 나눠 읽으면 그 사이에 노드가
     * 드나들어 분모와 서킷이 다른 판의 것이 된다.
     *
     * @param alive     살아 있는 노드 수
     * @param notClosed 그중 서킷이 닫히지 않았다고 말한 수
     */
    public record Presence(int alive, int notClosed) {
    }

    /** 자발적 종료. 임계를 안 기다리고 즉시 뺀다 — 배포마다 분모가 부풀지 않게. */
    public Mono<Void> leave(String instanceId) {
        return redis.execute(LEAVE, List.of(RedisKeys.INSTANCES), List.of(instanceId))
                .then();
    }
}

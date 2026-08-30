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
     * 하트비트를 남기고 <b>이 노드가 본 서킷을 같이 싣는다</b>.
     *
     * @param reapAfterSec 이보다 오래된 항목은 지운다. 시각은 레디스가 찍는다
     * @param voteFreshSec 표를 인정하는 신선도. 분모의 임계보다 훨씬 짧다
     */
    // 안 실으면 배분이 리더 한 대의 관측으로 전 클러스터의 크레딧을 정한다.
    public Mono<Presence> beat(String instanceId, long reapAfterSec, long voteFreshSec,
            CircuitState circuit) {
        return redis.execute(BEAT, List.of(RedisKeys.INSTANCES),
                        List.of(instanceId, Long.toString(reapAfterSec), circuit.name(),
                                Long.toString(voteFreshSec)))
                .next()
                // 둘째 칸은 서버 시각이라 분모와 무관하다.
                .map(GatewayRedisPort::presence);
    }

    /** 스크립트가 돌려주는 칸 수. 둘째 칸은 서버 시각이라 여기서 안 쓴다. */
    private static final int BEAT_FIELDS = 5;

    /**
     * <b>칸 수를 검증한다.</b> 스크립트와 이 파서가 갈린 것을 기동 직후에
     * 드러내는 자물쇠다.
     */
    // 모자란 칸을 0 으로 메우면 표가 영영 0 이고 클러스터는 항상 닫힌 것으로
    // 보인다 — 기능이 조용히 꺼진 채 다음 장애를 맞는다.
    static Presence presence(Object raw) {
        List<?> v = (List<?>) raw;
        if (v.size() != BEAT_FIELDS) {
            throw new IllegalStateException(
                    "하트비트가 %d 칸을 줘야 한다: %d".formatted(BEAT_FIELDS, v.size()));
        }
        int[] at = new int[BEAT_FIELDS];
        for (int i = 0; i < BEAT_FIELDS; i++) {
            at[i] = (int) ((Number) v.get(i)).longValue();
        }
        // 둘째 칸은 서버 시각이라 여기서 안 쓴다.
        return new Presence(at[0], at[2], at[3], at[4]);
    }

    /**
     * 한 판의 관측. <b>넷을 같이 받는다</b> — 나눠 읽으면 그 사이에 노드가
     * 드나들어 분모와 표가 다른 판의 것이 된다.
     *
     * @param alive    살아 있는 노드 수
     * @param open     그중 서킷이 열렸다고 말한 수
     * @param halfOpen 그중 반쯤 열렸다고 말한 수
     * @param reported 표를 낸 수. 아직 읽는 곳이 없다
     */
    // reported 가 alive 보다 작으면 롤아웃 중이라는 뜻이라 게이지로 낼 재료다.
    // 판정의 분모로 쓰면 안 된다 — ClusterCircuit.of 의 주석을 본다.
    public record Presence(int alive, int open, int halfOpen, int reported) {
    }

    /** 자발적 종료. 임계를 안 기다리고 즉시 뺀다 — 배포마다 분모가 부풀지 않게. */
    public Mono<Void> leave(String instanceId) {
        return redis.execute(LEAVE, List.of(RedisKeys.INSTANCES), List.of(instanceId))
                .then();
    }
}

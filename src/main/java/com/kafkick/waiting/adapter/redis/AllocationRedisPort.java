package com.kafkick.waiting.adapter.redis;

import com.kafkick.waiting.domain.allocation.Grant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 스케줄러가 레디스에 내는 명령.
 *
 * <p>수요 수집은 <b>Lua 가 아니다.</b> 쿠폰마다 슬롯이 갈려 클러스터에서 못 돈다.
 * 재고도 샤드 무관 키라 같은 스크립트에서 못 읽는다. 잃는 것은 진단 편의뿐이다.
 */
public final class AllocationRedisPort {

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> APPLY =
            RedisScript.of(new ClassPathResource("redis/allocation_apply.lua"), List.class);

    private final ReactiveStringRedisTemplate redis;
    private final int shards;

    private AllocationRedisPort(ReactiveStringRedisTemplate redis, int shards) {
        if (shards < 1) {
            throw new IllegalArgumentException("shards 는 1 이상이어야 한다: %d".formatted(shards));
        }
        this.redis = Objects.requireNonNull(redis, "redis 는 필수다");
        this.shards = shards;
    }

    public static AllocationRedisPort of(ReactiveStringRedisTemplate redis, int shards) {
        return new AllocationRedisPort(redis, shards);
    }

    /** 목록에 없는 쿠폰은 보지 않는다. 끝난 쿠폰까지 보면 매 틱 왕복만 늘어난다. */
    public Mono<List<String>> activeCoupons() {
        return redis.opsForSet().members(RedisKeys.ACTIVE_COUPONS).sort().collectList();
    }

    /**
     * 쿠폰별 대기 수. 샤드를 합친다.
     *
     * <p><b>위치가 아니라 쿠폰으로 짝짓는다.</b> 위치로 맞추면 응답이 한 칸만
     * 밀려도 A 의 대기가 B 의 재고와 붙는데, 그 조합은 도메인이 안 막으므로
     * <b>조용히 틀린 배분</b>이 나간다.
     */
    public Mono<Map<String, Long>> queueSizes(List<String> couponIds) {
        return Flux.fromIterable(couponIds)
                .concatMap(couponId -> shardSizes(couponId)
                        .map(size -> Map.entry(couponId, size)))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    private Mono<Long> shardSizes(String couponId) {
        List<String> keys = new ArrayList<>(shards);
        for (int shard = 0; shard < shards; shard++) {
            keys.add(RedisKeys.queue(couponId, shards, shard));
        }
        return Flux.fromIterable(keys)
                .flatMap(key -> redis.opsForZSet().size(key).defaultIfEmpty(0L))
                .reduce(0L, Long::sum);
    }

    /** 쿠폰별 재고. <b>없으면 담지 않는다</b> — 부르는 쪽이 "모른다" 를 0 으로 접는다. */
    public Mono<Map<String, Long>> stocks(List<String> couponIds) {
        List<String> keys = couponIds.stream().map(RedisKeys::stock).toList();
        return redis.opsForValue().multiGet(keys).map(values -> {
            Map<String, Long> byCoupon = new LinkedHashMap<>();
            for (int i = 0; i < couponIds.size(); i++) {
                Long stock = toLong(values.get(i));
                if (stock != null) {
                    byCoupon.put(couponIds.get(i), stock);
                }
            }
            return byCoupon;
        });
    }

    /** 들어온 인원을 돌려준다. 나눠 준 몫과 다르다 — 큐가 짧으면 남는다. */
    public Mono<Long> apply(Grant grant) {
        return redis.execute(APPLY,
                        List.of(RedisKeys.queue(grant.couponId(), shards, 0),
                                RedisKeys.admitted(grant.couponId(), shards, 0)),
                        List.of(Long.toString(grant.credit())))
                .next()
                .map(result -> Long.parseLong(String.valueOf(((List<?>) result).get(1))));
    }

    /**
     * <b>통째로 갈아 끼운다.</b> 남기면 끝난 쿠폰이 스냅샷에 영영 남아, 각 노드가
     * 없는 쿠폰을 계속 판정한다.
     */
    public Mono<Void> publish(Map<String, String> hash) {
        return redis.delete(RedisKeys.SNAPSHOT)
                .then(redis.opsForHash().putAll(RedisKeys.SNAPSHOT, hash))
                .then();
    }

    public Mono<Map<String, String>> load() {
        return redis.<String, String>opsForHash().entries(RedisKeys.SNAPSHOT)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    private Long toLong(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

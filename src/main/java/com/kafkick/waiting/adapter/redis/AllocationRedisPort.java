package com.kafkick.waiting.adapter.redis;

import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.control.FailureWindow;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.allocation.Grant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 스케줄러가 레디스에 내는 명령.
 *
 * <p>수요 수집은 <b>Lua 가 아니다.</b> 쿠폰마다 슬롯이 갈려 클러스터에서 못 돈다.
 * 재고도 샤드 무관 키라 같은 스크립트에서 못 읽는다. 잃는 것은 진단 편의뿐이다.
 */
@Component
public final class AllocationRedisPort implements SnapshotSource {

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> APPLY =
            RedisScript.of(new ClassPathResource("redis/allocation_apply.lua"), List.class);

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> PUBLISH =
            RedisScript.of(new ClassPathResource("redis/snapshot_publish.lua"), List.class);

    /**
     * 스크립트에 한 번에 넘기는 인자 상한.
     *
     * <p>루아 스택이 넘치면 그 판이 통째로 실패한다. 쿠폰이 그만큼 많아지면
     * 나눠 실어야 하는데, 그러면 원자성이 깨지므로 <b>그때는 설계를 다시 본다.</b>
     */
    private static final int MAX_PUBLISH_FIELDS = 3_000;

    /** 한 판이 동시에 낼 수 있는 읽기. 무제한이면 한 판이 커넥션을 독점한다. */
    private static final int MAX_CONCURRENT_READS = 16;

    private static final Logger log = LoggerFactory.getLogger(AllocationRedisPort.class);

    private final ReactiveStringRedisTemplate redis;
    private final int shards;
    private final FailureWindow rejected = FailureWindow.create();

    /**
     * <b>모든 노드가 쓴다.</b> 배분은 리더만 돌지만 판정 재료를 받아 오는 것은
     * 전 노드가 하므로, 배분 토글 뒤에 두면 요청만 받는 노드가 재료를 못 받는다.
     */
    @Autowired
    AllocationRedisPort(ReactiveStringRedisTemplate redis, ControlPlaneProperties properties) {
        this(redis, properties.scheduler().shards());
    }

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

    /**
     * 목록에 없는 쿠폰은 보지 않는다. 끝난 쿠폰까지 보면 매 틱 왕복만 늘어난다.
     *
     * <p><b>밖에서 쓰는 키라 아무 값이나 들어온다.</b> 키에 못 쓰는 멤버 하나가
     * 판을 죽이면 멀쩡한 쿠폰 전부의 배분이 멎는데, 사람이 목록을 고치기 전에는
     * 안 풀린다. 그래서 걸러 내되 걸러 냈다는 사실을 남긴다.
     */
    public Mono<List<String>> activeCoupons() {
        AtomicBoolean dropped = new AtomicBoolean();
        return redis.opsForSet().members(RedisKeys.ACTIVE_COUPONS)
                .filter(couponId -> usable(couponId, dropped))
                .sort()
                .collectList()
                // 걸러 낼 것이 없어진 판에서 창을 닫는다. 안 닫으면 나중에 다른
                // 오염이 들어와도 아무 신호가 안 나온다.
                .doOnNext(coupons -> {
                    if (!dropped.get()) {
                        rejected.exited().ifPresent(recovered ->
                                log.info("배분 대상이 다시 깨끗하다 — {}초 만에, 그동안 {}건 걸렀다",
                                        recovered.elapsedSeconds(), recovered.swallowed()));
                    }
                });
    }

    private boolean usable(String couponId, AtomicBoolean dropped) {
        try {
            RedisKeys.queue(couponId, shards, 0);
            return true;
        } catch (IllegalArgumentException e) {
            dropped.set(true);
            if (rejected.entered()) {
                log.warn("배분 대상에 키로 못 쓰는 값이 있다 — 그것만 빼고 돈다: {}", e.getMessage());
            }
            return false;
        }
    }

    /**
     * 쿠폰별 대기 수. 샤드를 합친다.
     *
     * <p><b>위치가 아니라 쿠폰으로 짝짓는다.</b> 위치로 맞추면 응답이 한 칸만
     * 밀려도 A 의 대기가 B 의 재고와 붙는데, 그 조합은 도메인이 안 막으므로
     * <b>조용히 틀린 배분</b>이 나간다.
     */
    public Mono<Map<String, Long>> queueSizes(List<String> couponIds) {
        // **쿠폰마다 순서대로 왕복하면 틱이 밀린다.** 결과를 쿠폰으로 짝지으므로
        // 순서는 아무 뜻이 없다. 다만 무제한으로 풀면 한 판이 커넥션을 독점해
        // 다른 명령이 뒤로 밀리므로 동시성에 상한을 둔다.
        return Flux.fromIterable(couponIds)
                .flatMap(couponId -> shardSizes(couponId)
                        .map(size -> Map.entry(couponId, size)), MAX_CONCURRENT_READS)
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

    /**
     * 들어온 인원을 돌려준다. 나눠 준 몫과 다르다 — 큐가 짧으면 남는다.
     *
     * <p><b>샤드가 하나인 동안만 옳다.</b> 여럿이면 몫을 샤드에 나눠 각각
     * 적용해야 하는데, 지금은 0번에만 나간다. 그래서 기동에서 하나로 막는다.
     */
    public Mono<Long> apply(Grant grant) {
        return redis.execute(APPLY,
                        List.of(RedisKeys.queue(grant.couponId(), shards, 0),
                                RedisKeys.admitted(grant.couponId(), shards, 0)),
                        List.of(Long.toString(grant.credit())))
                .next()
                .map(result -> Long.parseLong(String.valueOf(((List<?>) result).get(1))));
    }

    /**
     * <b>통째로 갈아 끼우되 사이가 벌어지지 않게 한다.</b>
     *
     * <p>지우고 쓰는 것을 나눠 치면 그 사이에 끊길 때 키가 없는 채로 남고,
     * 전 노드가 판정 재료를 잃는다. 근거는 스크립트 주석에 있다.
     */
    public Mono<Void> publish(Map<String, String> hash) {
        if (hash.isEmpty()) {
            return Mono.error(new IllegalArgumentException("빈 스냅샷은 발행하지 않는다"));
        }
        if (hash.size() > MAX_PUBLISH_FIELDS) {
            return Mono.error(new IllegalStateException(
                    "한 번에 실을 수 있는 필드를 넘었다: %d > %d"
                            .formatted(hash.size(), MAX_PUBLISH_FIELDS)));
        }
        List<String> args = new ArrayList<>(hash.size() * 2);
        hash.forEach((field, value) -> {
            args.add(field);
            args.add(value);
        });
        return redis.execute(PUBLISH, List.of(RedisKeys.SNAPSHOT), args).next().then();
    }

    @Override
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

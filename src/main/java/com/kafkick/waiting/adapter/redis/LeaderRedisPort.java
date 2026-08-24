package com.kafkick.waiting.adapter.redis;

import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.control.LeaderLock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * 리더 락 스크립트를 잇는다.
 *
 * <p><b>확인과 변경이 한 스크립트 안에 있어야 한다.</b> 나눠 치면 그 사이 리스가
 * 만료돼 다른 노드가 잡는데, 깨어난 이쪽이 그냥 연장하면 남의 락을 늘리고 그냥
 * 지우면 남이 리더인 채 락만 사라진다.
 */
@Component
public final class LeaderRedisPort {

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> ACQUIRE =
            RedisScript.of(new ClassPathResource("redis/leader_acquire.lua"), List.class);

    private static final RedisScript<Long> RELEASE =
            RedisScript.of(new ClassPathResource("redis/leader_release.lua"), Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final Duration lease;

    /**
     * <b>여기 쓰는 리스가 락의 실제 수명이다.</b> 판정 쪽 유예와 갈리면 락은
     * 만료됐는데 자기가 아직 리더인 줄 아는 구간이 생긴다 — 리더가 둘이다.
     */
    @Autowired
    LeaderRedisPort(ReactiveStringRedisTemplate redis, ControlPlaneProperties properties) {
        this(redis, properties.leader().lease());
    }

    LeaderRedisPort(ReactiveStringRedisTemplate redis, Duration lease) {
        if (lease == null || lease.isZero() || lease.isNegative()) {
            throw new IllegalArgumentException("lease 는 양수여야 한다: %s".formatted(lease));
        }
        this.redis = Objects.requireNonNull(redis, "redis 는 필수다");
        this.lease = lease;
    }

    /**
     * 스크립트가 돌려주는 셋을 그대로 나른다.
     *
     * <p>참·거짓으로 접으면 "내가 못 잡았다" 와 "누가 잡고 있다" 가 같은 값이 되어,
     * 스플릿 브레인을 사후에 조사할 수 없다.
     */
    public Mono<LeaderLock> acquire(String ownerId) {
        return redis.execute(ACQUIRE, List.of(RedisKeys.LEADER),
                        List.of(ownerId, Long.toString(lease.toMillis())))
                .next()
                .map(raw -> toLock((List<?>) raw));
    }

    public Mono<Void> release(String ownerId) {
        return redis.execute(RELEASE, List.of(RedisKeys.LEADER), List.of(ownerId)).next().then();
    }

    private LeaderLock toLock(List<?> raw) {
        boolean acquired = Long.parseLong(String.valueOf(raw.get(0))) == 1;
        String owner = String.valueOf(raw.get(1));
        long ttlMillis = Long.parseLong(String.valueOf(raw.get(2)));
        return new LeaderLock(acquired, owner, ttlMillis);
    }
}

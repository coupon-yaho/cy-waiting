package com.kafkick.waiting.adapter.redis;

import java.time.Duration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 레디스 어댑터 배선.
 *
 * <p>시간 예산 검증을 빈으로 올린다 — 기동 시 한 번 돌고 어긋나면 컨텍스트가
 * 안 뜬다.
 */
@Configuration
public class RedisConfig {

    /** 큐 샤드 수. 지금은 하나만 지원한다 — 적용이 샤드별로 갈리지 않는다. */
    private static final int SHARDS = 1;

    /** 리더 리스. 시간 예산 검증기가 쓰는 값과 같아야 한다. */
    private static final Duration LEASE = Duration.ofSeconds(2);

    @Bean
    RedisTimeBudget redisTimeBudget(DataRedisProperties properties) {
        RedisTimeBudget budget = RedisTimeBudget.of(properties);
        budget.verify();
        return budget;
    }

    /**
     * <b>모든 노드가 쓴다.</b> 배분은 리더만 돌지만 판정 재료를 받아 오는 것은
     * 전 노드가 하므로, 배분 토글 뒤에 두면 요청만 받는 노드가 재료를 못 받는다.
     */
    /**
     * 인터페이스 타입으로 노출한다. 제어 평면이 구현을 직접 알면 의존 방향이
     * 뒤집히고, 장애를 주입해 감싸는 것도 막힌다.
     */
    @Bean
    AllocationRedisPort allocationRedisPort(ReactiveStringRedisTemplate redis) {
        return AllocationRedisPort.of(redis, SHARDS);
    }

    @Bean
    LeaderRedisPort leaderRedisPort(ReactiveStringRedisTemplate redis) {
        return LeaderRedisPort.of(redis, LEASE);
    }
}

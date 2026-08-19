package com.kafkick.waiting.adapter.redis;

import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 레디스 어댑터 배선.
 *
 * <p>시간 예산 검증을 빈으로 올린다 — 기동 시 한 번 돌고 어긋나면 컨텍스트가
 * 안 뜬다.
 */
@Configuration
public class RedisConfig {

    @Bean
    RedisTimeBudget redisTimeBudget(DataRedisProperties properties) {
        RedisTimeBudget budget = RedisTimeBudget.of(properties);
        budget.verify();
        return budget;
    }
}

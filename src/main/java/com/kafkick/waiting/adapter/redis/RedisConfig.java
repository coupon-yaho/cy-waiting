package com.kafkick.waiting.adapter.redis;

import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 시간 예산 검증.
 *
 * <p><b>어댑터 자신은 여기서 안 만든다</b> — 스스로 설 수 있는 것은 스스로 선다.
 * 여기 남은 것은 값이 필요하고 기동을 막아야 하는 검증뿐이다. 빈으로 올려야
 * 기동 시 한 번 돌고, 어긋나면 컨텍스트가 안 뜬다.
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

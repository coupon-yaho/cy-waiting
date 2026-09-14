package com.kafkick.waiting.adapter.redis;

import java.time.Duration;
import org.springframework.boot.data.redis.autoconfigure.ClientResourcesBuilderCustomizer;
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

    /** 재연결 지연 상한. 연결 상한(1초)·리더 재획득·틱·갱신 주기를 더해도 첫 스냅샷 한계(5초) 안에 든다. */
    static final Duration RECONNECT_DELAY_CAP = Duration.ofSeconds(1);

    /** 첫 재시도 지연의 바탕. 짧은 끊김은 수십 ms 안에 다시 붙는다. */
    private static final Duration RECONNECT_DELAY_BASE = Duration.ofMillis(100);

    @Bean
    RedisTimeBudget redisTimeBudget(DataRedisProperties properties) {
        RedisTimeBudget budget = RedisTimeBudget.of(properties);
        budget.verify();
        return budget;
    }

    /**
     * 재연결 지연에 상한을 건다. <b>기본값은 30초까지 두 배씩 는다</b> — 오래 죽은 레디스가 살아나도 다음
     * 시도까지 기다려, 실측에서 첫 스냅샷까지 16초가 걸렸다. 흔들어 전 노드가 한 박자로 몰리지 않게 한다.
     */
    @Bean
    ClientResourcesBuilderCustomizer redisReconnectDelay() {
        // 공급자로 넘긴다. 상태를 들 수 있는 지연을 연결끼리 나눠 쓰지 않게 한다.
        return builder -> builder.reconnectDelay(
                () -> CappedJitterDelay.of(RECONNECT_DELAY_BASE, RECONNECT_DELAY_CAP));
    }

}

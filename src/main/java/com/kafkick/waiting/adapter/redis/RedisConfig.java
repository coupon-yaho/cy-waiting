package com.kafkick.waiting.adapter.redis;

import com.kafkick.waiting.control.ControlPlaneProperties;
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

    @Bean
    RedisTimeBudget redisTimeBudget(DataRedisProperties properties) {
        RedisTimeBudget budget = RedisTimeBudget.of(properties);
        budget.verify();
        return budget;
    }

    /**
     * <b>모든 노드가 쓴다.</b> 배분은 리더만 돌지만 판정 재료를 받아 오는 것은
     * 전 노드가 하므로, 배분 토글 뒤에 두면 요청만 받는 노드가 재료를 못 받는다.
     *
     * <p>값은 <b>설정에서 받는다.</b> 상수로 복제하면 검증기가 안 보는 값이
     * 실제로 쓰이는 값이 된다.
     */
    @Bean
    AllocationRedisPort allocationRedisPort(ReactiveStringRedisTemplate redis,
            ControlPlaneProperties properties) {
        return AllocationRedisPort.of(redis, properties.scheduler().shards());
    }

    /**
     * 요청 경로가 레디스를 치는 유일한 자리 (RD-4).
     *
     * <p>샤드 수는 스케줄러와 <b>같은 값</b>이어야 한다. 갈리면 배분이 올린 임계와
     * 조회가 보는 줄이 다른 키가 되어, 차례가 와도 아무도 못 들어간다.
     */
    @Bean
    QueueRedisPort queueRedisPort(ReactiveStringRedisTemplate redis,
            ControlPlaneProperties properties) {
        return QueueRedisPort.of(redis, properties.scheduler().shards());
    }

    /**
     * <b>여기 쓰는 리스가 락의 실제 수명이다.</b> 판정 쪽 유예와 갈리면 락은
     * 만료됐는데 자기가 아직 리더인 줄 아는 구간이 생긴다 — 리더가 둘이다.
     */
    @Bean
    LeaderRedisPort leaderRedisPort(ReactiveStringRedisTemplate redis,
            ControlPlaneProperties properties) {
        return LeaderRedisPort.of(redis, properties.leader().lease());
    }
}

package com.kafkick.waiting.adapter.redis;

import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerOptions;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.data.redis.autoconfigure.ClientResourcesBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 명령별 지연 계기 (CY-936).
 *
 * <p>등록 타이머는 부른 시점부터 결과까지라 클라이언트 대기가 섞인다. 첫 응답까지가 레디스가 문 몫이고,
 * 완료와의 차가 우리 쪽 몫이다. 병목을 가르려면 이 둘이 따로 나와야 한다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "waiting.redis.command-latency", name = "enabled",
        havingValue = "true")
public class RedisCommandLatencyConfig {

    /**
     * <b>기본이 꺼짐인 것은 이것이 명령마다 돌기 때문이다.</b> 등록 한 건이 아홉을 치므로 상시로 켜면
     * 재려는 대상을 재는 행위가 민다. 병목을 가르는 회차에서만 켠다.
     */
    @Bean
    ClientResourcesBuilderCustomizer redisCommandLatency(MeterRegistry meters) {
        return builder -> builder.commandLatencyRecorder(
                new MicrometerCommandLatencyRecorder(meters, MicrometerOptions.create()));
    }

}

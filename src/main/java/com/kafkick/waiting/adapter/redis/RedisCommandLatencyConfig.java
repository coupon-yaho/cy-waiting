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
 * <p>첫 응답은 채널에 쓴 시점부터 우리 이벤트 루프가 디코드를 시작할 때까지다 — 레디스 실행과 양쪽
 * 대기가 함께 든다. 완료와의 차는 디코드 시간뿐이라 거의 0 이다. 쓰기 전의 우리 몫은 등록 타이머에서
 * 첫 응답을 빼야 나온다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "waiting.redis.command-latency", name = "enabled",
        havingValue = "true")
public class RedisCommandLatencyConfig {

    /**
     * <b>기본이 꺼짐인 것은 이것이 왕복마다 돌기 때문이다.</b> 실측으로 등록 한 건에 1.02 회다 — Lua
     * 안의 호출은 클라이언트 명령이 아니라 안 센다. 그래도 공짜가 아니라 회차에서만 켠다.
     */
    @Bean
    ClientResourcesBuilderCustomizer redisCommandLatency(MeterRegistry meters) {
        return builder -> builder.commandLatencyRecorder(
                new MicrometerCommandLatencyRecorder(meters, MicrometerOptions.create()));
    }

}

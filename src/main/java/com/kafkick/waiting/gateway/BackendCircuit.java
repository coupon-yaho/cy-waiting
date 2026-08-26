package com.kafkick.waiting.gateway;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.util.Objects;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 뒷단 서킷을 <b>인스턴스별로</b> 만든다 (R-10).
 *
 * <p>뒷단 전체를 하나로 묶으면 한 대가 죽어도 전 트래픽이 막힌다. 인스턴스별이라야
 * 로드밸런서가 그 한 대만 빼고 나머지로 흘린다.
 *
 * <p><b>그래서 판정이 보는 값도 바뀐다</b> — "서킷이 열렸는가" 가 아니라 "살아 있는
 * 인스턴스 비율" 이다 (B-7). 그 배선은 CY-626 이다.
 */
@Configuration
@EnableConfigurationProperties(BackendCircuitProperties.class)
public class BackendCircuit {

    /**
     * 이름 하나에 서킷 하나. <b>레지스트리가 이름별로 들고 있어야 한다</b> —
     * 매번 새로 만들면 창이 늘 비어 표본 하한을 못 넘고, 서킷이 영영 안 열린다.
     */
    public static CircuitBreakerRegistry registry(BackendCircuitProperties props) {
        Objects.requireNonNull(props, "props 는 필수다");
        return CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                // **건수가 아니라 시간이다.** 100K RPS 에서 건수 100 은 수 ms
                // 분량이라 GC 한 번이나 순간 변동에 열린다.
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
                .slidingWindowSize((int) props.slidingWindowSize().toSeconds())
                .minimumNumberOfCalls(props.minimumNumberOfCalls())
                .failureRateThreshold(props.failureRateThreshold())
                // **느린 호출을 실패로 센다.** 안 켜면 타임아웃 직전까지 느려진
                // 인스턴스가 전부 성공으로 집계되어 서킷이 안 열리고, 그동안
                // 격벽이 먼저 굳는다 (6.1.8).
                .slowCallRateThreshold(props.slowCallRateThreshold())
                .slowCallDurationThreshold(props.slowCallDurationThreshold())
                .waitDurationInOpenState(props.waitDurationInOpenState())
                .permittedNumberOfCallsInHalfOpenState(
                        props.permittedNumberOfCallsInHalfOpenState())
                // **스스로 반쯤 열지 않는다.** 자동 전환은 트래픽이 없어도 시각만
                // 보고 넘어가는데, 그러면 아무도 안 두드린 채 닫혀 다음 유입이
                // 통째로 약한 뒷단에 꽂힌다.
                .automaticTransitionFromOpenToHalfOpenEnabled(false)
                .build());
    }

    @Bean
    public CircuitBreakerRegistry backendCircuitRegistry(BackendCircuitProperties props) {
        return registry(props);
    }
}

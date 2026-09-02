package com.kafkick.waiting.gateway;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
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
                // **반쯤 열린 채로 두지 않는다.** 기본값 0 은 무제한이라 프로브가
                // 응답 없는 뒷단에 매달리면 그 상태로 고정된다 — 나갈 조건이 없다.
                .maxWaitDurationInHalfOpenState(props.maxWaitDurationInHalfOpenState())
                // **스스로 반쯤 연다.** 수동 전환은 호출이 와야 상태를 다시 보는데,
                // 판정이 OPEN 에서 유효 credit 을 0 으로 조이면(F3) 서킷에 닿는
                // 호출이 0 이 되어 영영 안 풀린다 — 진입은 있고 해제가 없다.
                //
                // 자동 전환이 가는 곳은 CLOSED 가 아니라 HALF_OPEN 이다. 거기서
                // 프로브를 받아 판정하므로 약한 뒷단에 전량이 꽂히지 않는다.
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build());
    }

    /**
     * <b>전이를 로깅한다</b> (LG-2). 판정이 OPEN 에서 유효 credit 을 조이면(F3)
     * 서킷에 닿는 호출이 0 이라, 요청 쪽 지표만으로는 열린 사실조차 안 보인다.
     */
    @Bean
    public CircuitBreakerRegistry backendCircuitRegistry(BackendCircuitProperties props,
            MeterRegistry meters) {
        CircuitBreakerRegistry registry = registry(props);
        CircuitTransitionLog.create().watch(registry);
        // **직접 묶는다.** 레지스트리를 손으로 만들면 자동 구성이 묶어 주던
        // 계량기가 빠진다. 라이브러리가 클래스패스에 있어도 그렇고, 그 사실은
        // 지표를 실제로 긁어 보기 전까지 아무 데도 안 드러난다 — 서킷이 열려도
        // 알람과 대시보드는 조용하다.
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meters);
        return registry;
    }

    /**
     * 서킷 상태를 판정과 배분이 함께 읽는 자리 (F3).
     *
     * <p><b>하나를 나눠 쓴다.</b> 각자 만들면 판정은 열렸다고 보는데 배분은
     * 아니라고 보는 구간이 생기고, 그 어긋남이 하필 회복 구간에 난다.
     */
    @Bean
    public CircuitStateReader circuitStateReader(CircuitBreakerRegistry circuits,
            MeterRegistry meters) {
        // **보고 있는지를 지표로 낸다.** 안 보는 것과 닫혀 있는 것이 같은 값을
        // 내므로, 배선이 빠지면 F3 이 통째로 꺼진 채 조용히 돈다.
        return CircuitStateReader.of(circuits, GatewayRoutes.CIRCUIT).bind(meters);
    }
}

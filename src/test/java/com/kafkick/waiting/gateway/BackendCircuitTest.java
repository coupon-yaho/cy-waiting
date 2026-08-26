package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 서킷을 <b>인스턴스별로</b> 건다 (R-10).
 *
 * <p>뒷단 전체를 하나로 묶으면 한 대가 죽어도 전 트래픽이 막힌다. 인스턴스별이라야
 * 로드밸런서가 그 한 대만 빼고 나머지로 흘린다.
 */
class BackendCircuitTest {

    private static final BackendCircuitProperties 설정 = new BackendCircuitProperties(
            Duration.ofSeconds(10), 20, 50f, Duration.ofSeconds(3),
            Duration.ofMillis(1500), 50f, Duration.ofSeconds(5), 10);

    private final CircuitBreakerRegistry registry =
            BackendCircuit.registry(설정);

    /**
     * <b>인스턴스마다 다른 서킷이어야 한다.</b> 같은 것을 돌려주면 한 대가 열릴 때
     * 나머지로 가는 길까지 같이 막힌다 — 서킷을 인스턴스별로 둔 뜻이 사라진다.
     */
    @Test
    @DisplayName("인스턴스마다_다른_서킷이다")
    void 인스턴스마다_다른_서킷이다() {
        assertThat(registry.circuitBreaker("backend-1"))
                .isNotSameAs(registry.circuitBreaker("backend-2"));
    }

    /** 같은 이름에는 같은 것을 준다. 매번 새로 만들면 창이 늘 비어 안 열린다. */
    @Test
    @DisplayName("같은_인스턴스에는_같은_서킷이다")
    void 같은_인스턴스에는_같은_서킷이다() {
        assertThat(registry.circuitBreaker("backend-1"))
                .isSameAs(registry.circuitBreaker("backend-1"));
    }

    /**
     * <b>설정이 실제로 실려야 한다.</b> 안 실리면 라이브러리 기본값(건수 창 100)
     * 으로 도는데, 그건 100K RPS 에서 수 ms 분량이라 순간 변동에 열린다.
     */
    @Test
    @DisplayName("설정한_값이_그대로_실린다")
    void 설정한_값이_그대로_실린다() {
        var config = registry.circuitBreaker("backend-1").getCircuitBreakerConfig();

        assertThat(config.getSlidingWindowType())
                .isEqualTo(io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
                        .SlidingWindowType.TIME_BASED);
        assertThat(config.getSlidingWindowSize()).isEqualTo(10);
        assertThat(config.getMinimumNumberOfCalls()).isEqualTo(20);
        assertThat(config.getFailureRateThreshold()).isEqualTo(50f);
        assertThat(config.getPermittedNumberOfCallsInHalfOpenState()).isEqualTo(10);
    }

    /**
     * <b>느린 호출을 실패로 센다.</b> 안 켜면 타임아웃 직전까지 느려진 인스턴스가
     * 전부 성공으로 집계되어 서킷이 안 열리고, 그동안 격벽이 먼저 굳는다 (6.1.8).
     */
    @Test
    @DisplayName("느린_호출을_실패로_집계한다")
    void 느린_호출을_실패로_집계한다() {
        var config = registry.circuitBreaker("backend-1").getCircuitBreakerConfig();

        assertThat(config.getSlowCallRateThreshold()).isEqualTo(50f);
        assertThat(config.getSlowCallDurationThreshold()).isEqualTo(Duration.ofMillis(1500));
    }

    /** 열린 뒤 대기가 길면 G8.12(회복 시도 2회 이하) 안에 못 든다. */
    @Test
    @DisplayName("열린_뒤_대기를_설정대로_잡는다")
    void 열린_뒤_대기를_설정대로_잡는다() {
        assertThat(registry.circuitBreaker("backend-1").getCircuitBreakerConfig()
                .getWaitIntervalFunctionInOpenState().apply(1))
                .isEqualTo(Duration.ofSeconds(5).toMillis());
    }
}

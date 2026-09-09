package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.routing.RoutingProperties;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

/**
 * 프로브 조립.
 *
 * <p><b>라우팅과 같이 못 켠다.</b> 트래픽은 {@code lb://} 로 흩어지는데 프로브는
 * 고정 주소 하나만 치고, 그 결과가 전 인스턴스를 덮는 같은 서킷에 남는다 — 그 한
 * 대가 죽으면 멀쩡한 나머지로 갈 발급이 통째로 폴백으로 떨어진다.
 */
@Tag("unit")
class BackendProbeConfigTest {

    private static final GatewayRoutes.Backend 뒷단 = new GatewayRoutes.Backend(
            "http://localhost:8090", Duration.ofSeconds(2), Duration.ofSeconds(1));

    private static final BackendProbeProperties 프로브 =
            new BackendProbeProperties(true, "/actuator/health", Duration.ofSeconds(1));

    /** 켜진 라우팅 설정 하나만 담는 최소 제공자. 스프링 컨텍스트를 안 띄운다. */
    private ObjectProvider<RoutingProperties> 라우팅(boolean enabled) {
        RoutingProperties 값 = new RoutingProperties(enabled, null, null, null, null, null,
                null, null, enabled ? List.of(".internal") : null,
                enabled ? List.of(9000) : null);
        return new ObjectProvider<>() {
            @Override
            public RoutingProperties getObject() {
                return 값;
            }

            @Override
            public RoutingProperties getIfAvailable() {
                return 값;
            }
        };
    }

    @Test
    @DisplayName("라우팅과_같이_켜면_안_뜬다")
    void 라우팅과_같이_켜면_안_뜬다() {
        BackendProbeConfig 배선 = new BackendProbeConfig();

        assertThatThrownBy(() -> 배선.backendProbe(뒷단, 프로브,
                CircuitBreakerRegistry.ofDefaults(), 라우팅(true)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("같이 켤 수 없다");
    }

    /** 라우팅이 꺼져 있으면 선다. 그것이 지금의 배포 기본값이다. */
    @Test
    @DisplayName("라우팅이_꺼져_있으면_선다")
    void 라우팅이_꺼져_있으면_선다() {
        BackendProbeConfig 배선 = new BackendProbeConfig();

        assertThatCode(() -> 배선.backendProbe(뒷단, 프로브,
                CircuitBreakerRegistry.ofDefaults(), 라우팅(false)))
                .doesNotThrowAnyException();
    }
}

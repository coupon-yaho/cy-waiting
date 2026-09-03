package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceChooser;
import com.kafkick.waiting.domain.routing.WeightedP2c;
import com.kafkick.waiting.domain.routing.WeightedRoundRobin;
import com.kafkick.waiting.gateway.AdmissionGatewayFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.OrderedGatewayFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClientSpecification;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.springframework.context.ApplicationContext;

/**
 * 라우팅을 켠 배포가 실제로 선다.
 *
 * <p><b>조각이 다 초록인데 사이가 비어 있으면</b> 라우팅이 안 붙은 채로 배포가
 * 나간다 — 단일 주소로 도는 것과 겉으로 구분이 안 된다.
 */
@Tag("context")
@SpringBootTest(properties = {"waiting.scheduler.enabled=false", "waiting.routing.enabled=true"})
class RoutingWiringTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private RouteLocator routes;

    @Autowired
    private PrometheusMeterRegistry prometheus;

    @Test
    @DisplayName("켜면_배선이_다_선다")
    void 켜면_배선이_다_선다() {
        assertThat(context.getBeansOfType(InFlightRegistry.class)).hasSize(1);
        assertThat(context.getBeansOfType(InFlightTrackingFilter.class)).hasSize(1);
        // 프레임워크가 제 기본 명세를 같이 올린다. 이름으로 짚어야 우리 것이
        // 빠진 경우와 구분된다.
        assertThat(context.getBeansOfType(LoadBalancerClientSpecification.class).values())
                .anyMatch(spec -> "coupon-service".equals(spec.getName()));
        assertThat(context.getBean(InstanceChooser.class)).isInstanceOf(WeightedP2c.class);
        // 게이지도 같이 선다. 누수는 값이 안 내려가는 것으로만 보인다 (G9.3).
        assertThat(context.getBeansOfType(InFlightMetrics.Binding.class)).hasSize(1);
    }

    /** 걸어 두기만 하고 안 걸면 스크레이프에 줄이 없다 — 이름 검사로는 안 드러난다. */
    @Test
    @DisplayName("물린_수가_긁힌다")
    void 물린_수가_긁힌다() {
        assertThat(prometheus.scrape())
                .containsPattern("waiting_routing_inflight\\{[^}]*\\} [0-9.E-]+\\n")
                .containsPattern("waiting_routing_instances\\{[^}]*\\} [0-9.E-]+\\n")
                .doesNotContain("NaN");
    }

    /** 켜면 균형기를 거친다. 단일 주소로 두면 인스턴스를 고를 자리가 없다. */
    @Test
    @DisplayName("발급이_lb_로_간다")
    void 발급이_lb_로_간다() {
        assertThat(발급_라우트().getUri()).hasToString("lb://coupon-service");
    }

    /**
     * <b>{@code lb://} 로 바꿔도 판정이 그대로 붙는다</b> (9.1.4).
     *
     * <p>주소만 바꾸는 변경으로 보이지만, 필터가 빠지면 이 게이트웨이가 통과
     * 게이트가 아니라 프록시가 된다 — 그것도 초록으로 뜬다.
     */
    @Test
    @DisplayName("lb_로_바꿔도_판정이_붙어_있다")
    void lb_로_바꿔도_판정이_붙어_있다() {
        assertThat(발급_라우트().getFilters()).anySatisfy(filter ->
                assertThat(벗긴다(filter)).isInstanceOf(AdmissionGatewayFilter.class));
    }

    private Route 발급_라우트() {
        return routes.getRoutes().collectList().block().stream()
                .filter(r -> "issue".equals(r.getId()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("발급 라우트가 없다"));
    }

    /** 순서를 붙이면 감싸므로 안엣것을 봐야 한다. */
    private GatewayFilter 벗긴다(GatewayFilter filter) {
        return filter instanceof OrderedGatewayFilter ordered ? ordered.getDelegate() : filter;
    }

    /**
     * <b>전략을 설정으로 바꾼다</b> (R-9 · 9.3.8). 값이 바뀌면 다른 구현이
     * 주입돼야 한다 — 안 그러면 두 전략을 만든 이유인 실측 비교가 성립하지 않는다.
     */
    @Nested
    @Tag("context")
    @SpringBootTest(properties = {"waiting.scheduler.enabled=false",
            "waiting.routing.enabled=true", "waiting.routing.strategy=round-robin"})
    class RoundRobin {

        @Autowired
        private ApplicationContext context;

        @Test
        @DisplayName("설정을_바꾸면_라운드로빈이_선다")
        void 설정을_바꾸면_라운드로빈이_선다() {
            assertThat(context.getBean(InstanceChooser.class))
                    .isInstanceOf(WeightedRoundRobin.class);
        }
    }

    /** 끄면 배선이 아예 안 선다. 코드가 남아도 무해해야 롤백이 성립한다. */
    @Nested
    @Tag("context")
    @SpringBootTest(properties = "waiting.scheduler.enabled=false")
    class Disabled {

        @Autowired
        private ApplicationContext context;

        @Autowired
        private RouteLocator routes;

        @Test
        @DisplayName("끄면_배선이_안_선다")
        void 끄면_배선이_안_선다() {
            assertThat(context.getBeansOfType(InFlightRegistry.class)).isEmpty();
            assertThat(context.getBeansOfType(InFlightTrackingFilter.class)).isEmpty();
            assertThat(context.getBeansOfType(LoadBalancerClientSpecification.class).values())
                    .noneMatch(spec -> "coupon-service".equals(spec.getName()));
        }

        @Test
        @DisplayName("끄면_단일_주소로_간다")
        void 끄면_단일_주소로_간다() {
            assertThat(routes.getRoutes().collectList().block())
                    .allSatisfy(route -> assertThat(route.getUri().getScheme())
                            .isNotEqualTo("lb"));
        }
    }
}

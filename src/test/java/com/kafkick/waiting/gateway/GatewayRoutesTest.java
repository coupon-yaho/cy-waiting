package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.springframework.http.HttpMethod;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.OrderedGatewayFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.boot.webflux.autoconfigure.WebFluxProperties;
import org.springframework.cloud.gateway.handler.predicate.MethodRoutePredicateFactory;
import org.springframework.cloud.gateway.handler.predicate.PathRoutePredicateFactory;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 라우트가 <b>실제로 그 요청을 잡는가.</b>
 *
 * <p>여기서 막는 실패는 전부 조용하다 — 기동은 성공하는데 판정만 사라지거나,
 * 프리픽스 한 글자가 틀려 라우트가 아무것도 안 잡는다. 부하 시험 전까지
 * 아무도 모른다.
 */
class GatewayRoutesTest {

    // 라우트 정의가 술어 팩토리를 컨텍스트에서 꺼낸다. 필요한 것만 등록해
    // 띄운다 — 애플리케이션을 통째로 세우면 라우트 하나 보려고 레디스까지 붙는다.
    private final RouteLocator locator = new GatewayRoutes().routes(
            new RouteLocatorBuilder(술어만_있는_컨텍스트()),
            new GatewayRoutes.Backend("http://backend:8080"));

    private static GenericApplicationContext 술어만_있는_컨텍스트() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean(WebFluxProperties.class);
        context.registerBean(MethodRoutePredicateFactory.class);
        context.registerBean(PathRoutePredicateFactory.class);
        context.refresh();
        return context;
    }

    private List<Route> 라우트() {
        return locator.getRoutes().collectList().block();
    }

    private Route 잡는_라우트(org.springframework.http.HttpMethod method, String path) {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(method, path).build());
        return 라우트().stream()
                .filter(r -> Boolean.TRUE.equals(
                        Mono.from(r.getPredicate().apply(exchange)).block()))
                .findFirst().orElse(null);
    }

    @Test
    @DisplayName("뒷단_주소가_없으면_기동을_막는다")
    void 뒷단_주소가_없으면_기동을_막는다() {
        // 주소가 없으면 프록시가 어디로 갈지 정해지지 않는다.
        assertThatThrownBy(() -> new GatewayRoutes.Backend("  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GatewayRoutes.Backend(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("발급_요청을_잡는다")
    void 발급_요청을_잡는다() {
        assertThat(잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1/issue"))
                .extracting(Route::getId).isEqualTo("issue");
    }

    /** 빌더가 순서를 매기려고 한 겹 감싼다. 감싼 것을 벗겨야 무엇이 붙었는지 보인다. */
    private static List<GatewayFilter> 벗긴_필터(Route route) {
        return route.getFilters().stream()
                .map(f -> f instanceof OrderedGatewayFilter o ? o.getDelegate() : f)
                .toList();
    }

    @Test
    @DisplayName("발급_라우트에_판정이_붙어_있다")
    void 발급_라우트에_판정이_붙어_있다() {
        // **이게 없으면 판정이 통째로 사라져도 기동은 성공한다.** 라우트가 있는
        // 것만 보면 필터를 떼어낸 것과 구별되지 않는다.
        Route 발급 = 잡는_라우트(HttpMethod.POST,
                "/api/v1/coupons/c1/issue");

        assertThat(벗긴_필터(발급)).hasAtLeastOneElementOfType(AdmissionGatewayFilter.class);
    }

    @Test
    @DisplayName("조회_요청에는_판정이_안_붙는다")
    void 조회_요청에는_판정이_안_붙는다() {
        // 조회는 그대로 프록시한다. 판정을 붙이면 조회가 큐에 들어간다.
        Route 조회 = 잡는_라우트(HttpMethod.GET, "/api/v1/coupons/c1");

        assertThat(조회).extracting(Route::getId).isEqualTo("coupons");
        assertThat(벗긴_필터(조회)).doesNotHaveAnyElementsOfTypes(AdmissionGatewayFilter.class);
    }

    @Test
    @DisplayName("순번_조회는_어떤_라우트도_안_잡는다")
    void 순번_조회는_어떤_라우트도_안_잡는다() {
        // 게이트웨이가 직접 답한다. 라우트가 잡으면 폴링이 통째로 백엔드로 가고,
        // 그 순간 대기열의 존재 이유가 없어진다.
        assertThat(잡는_라우트(HttpMethod.GET,
                "/api/v1/coupons/c1/queue")).isNull();
    }

    @Test
    @DisplayName("관리_경로는_어떤_라우트도_안_잡는다")
    void 관리_경로는_어떤_라우트도_안_잡는다() {
        // 프록시로 새면 밖에서 진단 정보와 종료 조작이 닿는다.
        assertThat(잡는_라우트(HttpMethod.GET, "/actuator/health"))
                .isNull();
    }

    @Test
    @DisplayName("프리픽스가_틀린_요청은_안_잡는다")
    void 프리픽스가_틀린_요청은_안_잡는다() {
        // **틀린 프리픽스는 오류 없이 조용히 안 잡힌다.** 라우트 쪽 오타는
        // 이 방향으로만 드러나므로, 안 잡히는 것을 명시적으로 못 박는다.
        assertThat(잡는_라우트(HttpMethod.POST,
                "/api/v2/coupons/c1/issue")).isNull();
        assertThat(잡는_라우트(HttpMethod.POST,
                "/v1/coupons/c1/issue")).isNull();
    }

    @Test
    @DisplayName("발급_옆의_다른_경로는_안_잡는다")
    void 발급_옆의_다른_경로는_안_잡는다() {
        // **경로를 넓히면 의도 안 한 것까지 프록시된다.** 뒷단에 새 엔드포인트가
        // 생기는 순간, 아무도 정하지 않은 채로 판정을 타고 열린다.
        assertThat(잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1/refund")).isNull();
        assertThat(잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1/issue/extra")).isNull();
    }

    @Test
    @DisplayName("순번_조회는_어떤_메서드로도_안_잡힌다")
    void 순번_조회는_어떤_메서드로도_안_잡힌다() {
        // 메서드를 바꿔 들어와도 백엔드로 새면 안 된다.
        for (HttpMethod method : List.of(HttpMethod.GET, HttpMethod.POST,
                HttpMethod.PUT, HttpMethod.DELETE)) {
            assertThat(잡는_라우트(method, "/api/v1/coupons/c1/queue"))
                    .as("%s /api/v1/coupons/c1/queue", method).isNull();
        }
    }

    @Test
    @DisplayName("발급은_POST_만_잡는다")
    void 발급은_POST_만_잡는다() {
        assertThat(잡는_라우트(HttpMethod.GET,
                "/api/v1/coupons/c1/issue")).isNull();
    }
}

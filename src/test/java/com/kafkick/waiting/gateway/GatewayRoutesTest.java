package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.springframework.http.HttpMethod;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;

/**
 * 라우트가 <b>실제로 그 요청을 잡는가.</b>
 *
 * <p>여기서 막는 실패는 전부 조용하다 — 기동은 성공하는데 판정만 사라지거나,
 * 프리픽스 한 글자가 틀려 라우트가 아무것도 안 잡는다. 부하 시험 전까지
 * 아무도 모른다.
 */
class GatewayRoutesTest {

    private final RouteLocator locator = new GatewayRoutes().routes(
            new GatewayRoutes.Backend("http://backend:8080"));

    private List<Route> 라우트() {
        return locator.getRoutes().collectList().block();
    }

    private Route 잡는_라우트(org.springframework.http.HttpMethod method, String path) {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(method, path).build());
        return 라우트().stream()
                .filter(r -> Boolean.TRUE.equals(r.getPredicate().apply(exchange).block()))
                .findFirst().orElse(null);
    }

    @Test
    @DisplayName("발급_요청을_잡는다")
    void 발급_요청을_잡는다() {
        assertThat(잡는_라우트(HttpMethod.POST,
                "/api/v1/coupons/c1/issue")).isNotNull();
    }

    @Test
    @DisplayName("발급_라우트에_판정이_붙어_있다")
    void 발급_라우트에_판정이_붙어_있다() {
        // **이게 없으면 판정이 통째로 사라져도 기동은 성공한다.** 라우트가 있는
        // 것만 보면 필터를 떼어낸 것과 구별되지 않는다.
        Route 발급 = 잡는_라우트(HttpMethod.POST,
                "/api/v1/coupons/c1/issue");

        assertThat(발급.getFilters()).anySatisfy(f ->
                assertThat(f).isInstanceOf(AdmissionGatewayFilter.class));
    }

    @Test
    @DisplayName("조회_요청에는_판정이_안_붙는다")
    void 조회_요청에는_판정이_안_붙는다() {
        // 조회는 그대로 프록시한다. 판정을 붙이면 조회가 큐에 들어간다.
        Route 조회 = 잡는_라우트(HttpMethod.GET, "/api/v1/coupons/c1");

        assertThat(조회).isNotNull();
        assertThat(조회.getFilters()).noneSatisfy(f ->
                assertThat(f).isInstanceOf(AdmissionGatewayFilter.class));
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
    @DisplayName("발급은_POST_만_잡는다")
    void 발급은_POST_만_잡는다() {
        assertThat(잡는_라우트(HttpMethod.GET,
                "/api/v1/coupons/c1/issue")).isNull();
    }
}

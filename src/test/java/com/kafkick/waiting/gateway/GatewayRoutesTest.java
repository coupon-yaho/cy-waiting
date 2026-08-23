package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.OrderedGatewayFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.boot.webflux.autoconfigure.WebFluxProperties;
import org.springframework.cloud.gateway.filter.factory.RemoveRequestHeaderGatewayFilterFactory;
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
        context.registerBean(RemoveRequestHeaderGatewayFilterFactory.class);
        context.registerBean(MethodRoutePredicateFactory.class);
        context.registerBean(PathRoutePredicateFactory.class);
        context.refresh();
        return context;
    }

    private List<Route> 라우트() {
        return locator.getRoutes().collectList().block();
    }

    private Route 잡는_라우트(HttpMethod method, String path) {
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
        // 스킴이 빠진 값은 기동에 성공하고 모든 프록시가 실패한다.
        assertThatThrownBy(() -> new GatewayRoutes.Backend("backend:8080"))
                .isInstanceOf(IllegalArgumentException.class);
        // 경로를 붙이면 그 경로만 조용히 버려진다.
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://backend:8080/api"))
                .isInstanceOf(IllegalArgumentException.class);
        // 호스트가 없으면 스킴만 맞고 프록시가 갈 곳이 없다.
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://:8080"))
                .isInstanceOf(IllegalArgumentException.class);
        // 주소로 읽히지 않는 값.
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://back end"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("발급_요청을_잡는다")
    void 발급_요청을_잡는다() {
        assertThat(잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1/issue"))
                .extracting(Route::getId).isEqualTo("issue");
    }

    /**
     * 빌더가 순서를 매기려고 감싼다. <b>끝까지 벗긴다</b> — 한 겹만 벗기면 두 번
     * 감싼 판정이 안 보이고, 조회에 판정이 붙어도 없다고 읽힌다.
     */
    private static List<GatewayFilter> 벗긴_필터(Route route) {
        return route.getFilters().stream().map(GatewayRoutesTest::끝까지_벗긴다).toList();
    }

    private static GatewayFilter 끝까지_벗긴다(GatewayFilter filter) {
        GatewayFilter 안쪽 = filter;
        while (안쪽 instanceof OrderedGatewayFilter o) {
            안쪽 = o.getDelegate();
        }
        return 안쪽;
    }

    @Test
    @DisplayName("발급_라우트에_판정이_붙어_있다")
    void 발급_라우트에_판정이_붙어_있다() {
        // **이게 없으면 판정이 통째로 사라져도 기동은 성공한다.** 라우트가 있는
        // 것만 보면 필터를 떼어낸 것과 구별되지 않는다.
        Route 발급 = 잡는_라우트(HttpMethod.POST,
                "/api/v1/coupons/c1/issue");

        // 정확히 하나다. 여러 번 붙으면 판정이 그만큼 더 돌고, 그건 통과 수를 흔든다.
        assertThat(벗긴_필터(발급)).filteredOn(AdmissionGatewayFilter.class::isInstance).hasSize(1);
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

    /** 발급과 조회는 같은 방어를 갖는다. 한쪽만 재면 나머지가 통째로 무방비다. */
    private static Stream<Arguments> 두_라우트() {
        return Stream.of(
                Arguments.of(HttpMethod.POST, "/api/v1/coupons/%s/issue"),
                Arguments.of(HttpMethod.GET, "/api/v1/coupons/%s"));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("두_라우트")
    @DisplayName("두_라우트_모두_이상한_식별자를_안_잡는다")
    void 두_라우트_모두_이상한_식별자를_안_잡는다(HttpMethod method, String 틀) {
        for (String 이상한_것 : List.of("a%2Fb", "c1;junk=1", " ", "c".repeat(65))) {
            assertThat(잡는_라우트(method, 틀.formatted(이상한_것)))
                    .as("%s %s", method, 틀.formatted(이상한_것)).isNull();
        }
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("두_라우트")
    @DisplayName("두_라우트_모두_정해진_메서드만_잡는다")
    void 두_라우트_모두_정해진_메서드만_잡는다(HttpMethod method, String 틀) {
        String path = 틀.formatted("c1");
        for (HttpMethod 다른_것 : List.of(HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.PATCH)) {
            assertThat(잡는_라우트(다른_것, path)).as("%s %s", 다른_것, path).isNull();
        }
        // 좁히다 정작 쓰는 메서드까지 막으면 서비스가 통째로 안 된다.
        assertThat(잡는_라우트(method, path)).as("%s %s", method, path)
                .extracting(Route::getId).isIn("issue", "coupons");
    }

    @Test
    @DisplayName("쿠폰_식별자에_이상한_것이_들어오면_안_잡는다")
    void 쿠폰_식별자에_이상한_것이_들어오면_안_잡는다() {
        // **술어는 디코딩해 맞추고 전달은 원본을 그대로 보낸다.** 그래서 게이트웨이가
        // 판정한 값과 뒷단이 받는 값이 갈린다. 그 값이 그대로 레디스 키가 되고
        // 캐시·리미터의 키가 되므로, 여기서 좁히지 않으면 아래 전부가 헐거워진다.
        assertThat(잡는_라우트(HttpMethod.POST, "/api/v1/coupons/a%2Fb/issue")).isNull();
        assertThat(잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1;junk=1/issue")).isNull();
        assertThat(잡는_라우트(HttpMethod.POST, "/api/v1/coupons/ /issue")).isNull();
        assertThat(잡는_라우트(HttpMethod.POST,
                "/api/v1/coupons/" + "c".repeat(65) + "/issue")).isNull();
    }

    @Test
    @DisplayName("뒤에_슬래시가_붙으면_안_잡는다")
    void 뒤에_슬래시가_붙으면_안_잡는다() {
        // 같은 쿠폰을 다른 키로 만들 수 있으면 캐시와 리미터를 돌려 쓸 수 있다.
        assertThat(잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1/issue/")).isNull();
    }

    @Test
    @DisplayName("보통_식별자는_잡는다")
    void 보통_식별자는_잡는다() {
        // 좁히다 실제 쿠폰까지 막으면 서비스가 통째로 안 된다.
        for (String id : List.of("c1", "COUPON-2026", "summer_sale", "a".repeat(64))) {
            assertThat(잡는_라우트(HttpMethod.POST, "/api/v1/coupons/" + id + "/issue"))
                    .as("쿠폰 %s", id).extracting(Route::getId).isEqualTo("issue");
        }
    }

    @Test
    @DisplayName("위조_가능한_클라이언트_IP_헤더를_지운다")
    void 위조_가능한_클라이언트_IP_헤더를_지운다() {
        // 프레임워크는 X-Forwarded-* 만 지운다. 관례로 쓰는 나머지가 그대로 가면
        // 뒷단이 하나라도 믿는 순간 IP 단위 제한이 헤더 한 줄로 우회된다.
        for (String route : List.of("/api/v1/coupons/c1/issue", "/api/v1/coupons/c1")) {
            HttpMethod method = route.endsWith("/issue") ? HttpMethod.POST : HttpMethod.GET;
            MockServerWebExchange exchange = MockServerWebExchange.from(
                    MockServerHttpRequest.method(method, route)
                            .header("X-Real-IP", "1.2.3.4")
                            .header("True-Client-IP", "1.2.3.4")
                            .header("X-Client-IP", "1.2.3.4")
                            .header("CF-Connecting-IP", "1.2.3.4"));

            HttpHeaders 받은_것 = 뒷단이_받는_헤더(잡는_라우트(method, route), exchange);

            assertThat(받은_것.headerNames())
                    .as("%s 로 간 요청", route)
                    .doesNotContain("X-Real-IP", "True-Client-IP",
                            "X-Client-IP", "CF-Connecting-IP");
        }
    }

    /** 필터 사슬을 실제로 태워 본다. 어떤 필터가 붙었는지만 보면 도는지를 못 잰다. */
    private static HttpHeaders 뒷단이_받는_헤더(Route route, MockServerWebExchange exchange) {
        AtomicReference<HttpHeaders> 받은_것 = new AtomicReference<>();
        GatewayFilterChain 끝 = e -> {
            받은_것.set(e.getRequest().getHeaders());
            return Mono.empty();
        };
        List<GatewayFilter> 필터 = route.getFilters();
        GatewayFilterChain 사슬 = 끝;
        for (int i = 필터.size() - 1; i >= 0; i--) {
            GatewayFilter 하나 = 필터.get(i);
            GatewayFilterChain 다음 = 사슬;
            사슬 = e -> 하나.filter(e, 다음);
        }
        사슬.filter(exchange).block();
        return 받은_것.get();
    }

    @Test
    @DisplayName("발급은_POST_만_잡는다")
    void 발급은_POST_만_잡는다() {
        assertThat(잡는_라우트(HttpMethod.GET,
                "/api/v1/coupons/c1/issue")).isNull();
    }
}

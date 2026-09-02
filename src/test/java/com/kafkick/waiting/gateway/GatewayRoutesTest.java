package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.routing.RoutingProperties;
import org.springframework.beans.factory.ObjectProvider;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
import org.springframework.cloud.gateway.filter.NettyWriteResponseFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.boot.webflux.autoconfigure.WebFluxProperties;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreakerFactory;
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigurationProperties;
import org.springframework.cloud.gateway.filter.factory.RemoveRequestHeaderGatewayFilterFactory;
import org.springframework.cloud.gateway.filter.factory.SpringCloudCircuitBreakerResilience4JFilterFactory;
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
    private static final SecondWindowLimiter 공유_리미터 = SecondWindowLimiter.withMaxKeys(10);

    // 띄운다 — 애플리케이션을 통째로 세우면 라우트 하나 보려고 레디스까지 붙는다.
    /** 검증 시험이 주소만 보게 하는 유효한 값. 여기가 초점이 아니다. */
    private static final Duration 응답_상한 = Duration.ofSeconds(12);

    private static final GenericApplicationContext 컨텍스트 = 술어만_있는_컨텍스트();

    // 띄운다 — 애플리케이션을 통째로 세우면 라우트 하나 보려고 레디스까지 붙는다.
    private final RouteLocator locator = new GatewayRoutes().routes(
            new RouteLocatorBuilder(컨텍스트),
            new GatewayRoutes.Backend("http://backend:8080", 응답_상한),
            AdmissionGatewayFilter.withIsolatedSoldOutCache(재료_없는_홀더(),
                    AdmissionDecider.of(공유_리미터, 0.7),
                    Clock.systemUTC(), new SimpleMeterRegistry(),
                    FakeQueuePort.create(),
                    QueueToken.of("not-a-real-secret-0123456789abcdef"),
                    // **판정과 같은 인스턴스다.** 따로 만들면 한 초에 두 예산이 나간다.
                    공유_리미터,
                    EntryToken.of("not-a-real-secret-0123456789abcdef"),
                    IdempotencyKey.passThrough()),
            // 이 시험은 배선을 본다. 켜고 끄는 것은 필터 안에서 갈리므로
            // 여기서는 꺼 두어도 라우트에 실리는 것은 같다.
            QueryCoalescingFilter.of(
                    new CoalescingProperties(false, 1024, 1 << 20, 100, List.of()),
                    Clock.systemUTC(), new SimpleMeterRegistry()),
            SoldOutObserver.ofPublishedAt(
                    SoldOutCache.standard(), Instant::now, new SimpleMeterRegistry()),
            컨텍스트.getBean(SpringCloudCircuitBreakerResilience4JFilterFactory.class),
            new SimpleMeterRegistry(), 라우팅_없음());

    /** 라우팅을 안 켠 경우. 기존 시험들이 보는 것은 단일 주소 그대로다. */
    private static ObjectProvider<RoutingProperties> 라우팅_없음() {
        return 라우팅(null);
    }

    /** 배선만 넘긴다. 컨텍스트를 띄우지 않고 켠 것과 끈 것을 나란히 본다. */
    private static ObjectProvider<RoutingProperties> 라우팅(RoutingProperties properties) {
        return new ObjectProvider<>() {
            @Override
            public RoutingProperties getObject() {
                return properties;
            }

            @Override
            public RoutingProperties getObject(Object... args) {
                return properties;
            }

            @Override
            public RoutingProperties getIfAvailable() {
                return properties;
            }

            @Override
            public RoutingProperties getIfUnique() {
                return properties;
            }
        };
    }

    /**
     * 재료를 한 번도 못 받은 홀더. 이 시험은 <b>라우트가 무엇을 잡는가</b>만 보므로
     * 판정이 끼면 안 된다 — 첫 틱 전에는 판정을 미루고 그대로 흘린다.
     */
    private static SnapshotHolder 재료_없는_홀더() {
        return SnapshotHolder.of(Duration.ofSeconds(3), Duration.ofSeconds(10),
                Clock.systemUTC());
    }

    /** 라우트가 무엇을 잡는지만 보는 시험이라 값 자체는 안 잰다. 온전하면 된다. */
    private static BackendCircuitProperties 서킷_설정() {
        return new BackendCircuitProperties(Duration.ofSeconds(10), 20, 50f,
                Duration.ofMillis(1500), 50f, Duration.ofSeconds(5),
                Duration.ofSeconds(30), 10);
    }

    private static GenericApplicationContext 술어만_있는_컨텍스트() {
        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean(WebFluxProperties.class);
        context.registerBean(RemoveRequestHeaderGatewayFilterFactory.class);
        context.registerBean(MethodRoutePredicateFactory.class);
        context.registerBean(PathRoutePredicateFactory.class);
        // 서킷 필터도 컨텍스트에서 꺼낸다. 안 넣으면 라우트 정의가 통째로 못
        // 만들어져, 이 시험이 "라우트가 무엇을 잡는가" 를 아예 못 재게 된다.
        //
        // 전달 핸들러는 안 넣는다. 이 시험은 무엇이 붙었는지만 보고 실제로
        // 넘기지 않는다 — 넘기는 것은 컨텍스트 시험이 본다.
        context.registerBean(ReactiveCircuitBreakerFactory.class,
                () -> new ReactiveResilience4JCircuitBreakerFactory(
                        BackendCircuit.registry(서킷_설정()),
                        TimeLimiterRegistry.ofDefaults(), null,
                        new Resilience4JConfigurationProperties()));
        context.registerBean(SpringCloudCircuitBreakerResilience4JFilterFactory.class);
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
        assertThatThrownBy(() -> new GatewayRoutes.Backend("  ", 응답_상한))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GatewayRoutes.Backend(null, 응답_상한))
                .isInstanceOf(IllegalArgumentException.class);
        // 스킴이 빠진 값은 기동에 성공하고 모든 프록시가 실패한다.
        assertThatThrownBy(() -> new GatewayRoutes.Backend("backend:8080", 응답_상한))
                .isInstanceOf(IllegalArgumentException.class);
        // 경로를 붙이면 그 경로만 조용히 버려진다.
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://backend:8080/api", 응답_상한))
                .isInstanceOf(IllegalArgumentException.class);
        // 호스트가 없으면 스킴만 맞고 프록시가 갈 곳이 없다.
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://", 응답_상한))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://:8080", 응답_상한))
                .isInstanceOf(IllegalArgumentException.class);
        // 주소로 읽히지 않는 값.
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://back end", 응답_상한))
                .isInstanceOf(IllegalArgumentException.class);
        // 프록시는 스킴·호스트·포트만 가져간다. 나머지는 조용히 버려진다.
        for (String 군더더기 : List.of("http://backend?trace=1", "http://u:p@backend",
                "http://backend#x")) {
            assertThatThrownBy(() -> new GatewayRoutes.Backend(군더더기, 응답_상한))
                    .as("뒷단 %s", 군더더기)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /**
     * <b>상한이 없으면 무한입니다.</b> 무한이면 멎은 뒷단에 걸린 요청이 격벽 자리를
     * 영영 쥐고, 서킷은 표본이 없어 안 열립니다 — 기동은 성공하고 장애 때만 압니다.
     */
    /**
     * <b>끊는 자리가 서킷 안쪽이어야 합니다.</b> 밖에서 끊으면 서킷에 가는 것이
     * 오류가 아니라 취소이고, 취소는 창에 안 쌓입니다 — 멎은 뒷단의 서킷이 영영
     * 안 열리고, 그동안 게이트웨이는 죽은 뒷단에 계속 밀어 넣습니다.
     */
    @Test
    @DisplayName("발급_라우트가_응답_상한을_들고_있다")
    void 발급_라우트가_응답_상한을_들고_있다() {
        Route 발급 = 라우트().stream()
                .filter(r -> "issue".equals(r.getId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("issue 라우트가 없다"));

        assertThat(발급.getMetadata()).containsEntry("response-timeout", 응답_상한.toMillis());
    }

    @Test
    @DisplayName("응답_상한이_없으면_기동을_막는다")
    void 응답_상한이_없으면_기동을_막는다() {
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://backend:8080", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() ->
                new GatewayRoutes.Backend("http://backend:8080", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>순서를 시험으로만 두면 배포 설정 한 줄이 뒤집습니다.</b> 격벽이 먼저
     * 끊으면 서킷이 받는 것은 오류가 아니라 취소이고, 멎은 뒷단의 서킷이 영영
     * 안 열립니다. 기동에서 막아야 그 설정이 운영에 못 나갑니다.
     */
    @Test
    @DisplayName("응답_상한이_격벽_시한_뒤면_기동을_막는다")
    void 응답_상한이_격벽_시한_뒤면_기동을_막는다() {
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://backend:8080",
                AdmissionGatewayFilter.MAX_IN_FLIGHT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GatewayRoutes.Backend("http://backend:8080",
                AdmissionGatewayFilter.MAX_IN_FLIGHT.plusSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        // 바로 앞은 받아야 한다. 안 그러면 상한을 못 올린다.
        assertThatCode(() -> new GatewayRoutes.Backend("http://backend:8080",
                AdmissionGatewayFilter.MAX_IN_FLIGHT.minusMillis(1)))
                .doesNotThrowAnyException();
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
                            // 신원 필터가 앞에서 보장한다. 없으면 판정이 끊는다.
                            .header("X-Member-Id", "812934")
                            .header("X-Real-IP", "1.2.3.4")
                            .header("True-Client-IP", "1.2.3.4")
                            .header("X-Client-IP", "1.2.3.4")
                            .header("CF-Connecting-IP", "1.2.3.4"));
            // 실제로는 술어가 채운다. 없으면 판정이 대상을 못 정해 끊는다.
            exchange.getAttributes().put(
                    ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                    Map.of("couponId", "c1"));

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

    /**
     * <b>발급 라우트에 서킷이 걸려 있어야 한다.</b> 안 걸면 뒷단이 멎었을 때
     * 게이트웨이의 커넥션이 통째로 그 뒷단을 기다리며 물리고, 한산한 쿠폰의
     * 통과 경로까지 같이 죽는다.
     */
    @Test
    @DisplayName("발급_라우트에_서킷이_걸려_있다")
    void 발급_라우트에_서킷이_걸려_있다() {
        // **이름과 넘길 주소까지 본다.** 붙었는지만 보면 엉뚱한 서킷에 물리거나
        // fallback 이 빠져도 초록이고, 그때 서킷이 열리면 404 가 나간다.
        assertThat(필터_이름(잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1/issue")))
                .anySatisfy(name -> assertThat(name)
                        .contains("CircuitBreaker")
                        .contains("name = 'backend'")
                        .contains("fallback = " + GatewayRoutes.FALLBACK_URI));
    }

    /**
     * <b>넘길 주소가 받는 주소와 같아야 한다.</b> 갈리면 기동은 되고 장애 때만
     * 404 가 드러난다 — 사용자에게는 매진으로 읽힌다.
     */
    @Test
    @DisplayName("서킷이_넘길_주소가_받는_주소와_같다")
    void 서킷이_넘길_주소가_받는_주소와_같다() {
        assertThat(GatewayRoutes.FALLBACK_URI)
                .isEqualTo("forward:" + BackendFallbackRoutes.FALLBACK_ISSUE);
    }

    /** 라우트에 걸린 필터의 이름들. 어떤 것이 붙었는지는 이걸로만 볼 수 있다. */
    private List<String> 필터_이름(Route route) {
        return route.getFilters().stream()
                .map(f -> f instanceof OrderedGatewayFilter o
                        ? o.getDelegate().toString() : f.toString())
                .toList();
    }

    /**
     * 필터에 <b>실제로 실린 order</b>. 프레임워크는 선언 위치가 아니라 이 값으로
     * 정렬하므로, 여기를 안 보면 순서를 본 것이 아니다.
     */
    private int 실린_순서(Route route, String 이름) {
        return route.getFilters().stream()
                .filter(f -> 끝까지_벗긴다(f).toString().contains(이름))
                .map(f -> {
                    assertThat(f).as("%s 필터에 order 가 안 실렸다", 이름)
                            .isInstanceOf(OrderedGatewayFilter.class);
                    return ((OrderedGatewayFilter) f).getOrder();
                })
                // **못 찾으면 실패다.** 없는 것을 -1 같은 값으로 대신하면 필터가
                // 통째로 빠져도, 이름이 바뀌어도 이 시험이 조용히 통과한다.
                .findFirst()
                .orElseThrow(() -> new AssertionError(이름 + " 필터가 발급 라우트에 없다"));
    }

    /**
     * <b>판정이 서킷보다 앞이어야 한다.</b> 뒤로 가면 서킷이 열린 동안 판정이
     * 안 돌아 래치가 표식을 못 받고, 다음 창의 신규 유입이 방금 줄 선 사람을
     * 추월한다 (불변식 4).
     *
     * <p><b>목록의 자리가 아니라 실린 order 를 본다</b> — 프레임워크가 그 값으로
     * 다시 정렬한다. 자리만 보면 두 인자를 서로 바꿔 단 회귀를 못 잡는다.
     */
    @Test
    @DisplayName("판정이_서킷보다_앞이다")
    void 판정이_서킷보다_앞이다() {
        Route 발급 = 잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1/issue");

        assertThat(실린_순서(발급, "Admission"))
                .isLessThan(실린_순서(발급, "CircuitBreaker"));
    }

    /**
     * <b>모으기는 응답을 쓰는 필터보다 앞이어야 한다.</b> 뒤에 서면 우리가 감싼
     * 응답을 아무도 안 쓰고, 담는 것이 늘 빈 본문이 된다.
     *
     * <p>상수끼리 비교하면 항진명제다 — 라우트에서 order 인자를 지워도 통과한다.
     * <b>실제로 실린 값</b>을 봐야 그 회귀가 잡힌다.
     */
    @Test
    @DisplayName("조회_라우트가_모으기를_쓰기_필터보다_앞에_단다")
    void 조회_라우트가_모으기를_쓰기_필터보다_앞에_단다() {
        Route 조회 = 잡는_라우트(HttpMethod.GET, "/api/v1/coupons");

        assertThat(실린_순서(조회, "QueryCoalescing"))
                .isEqualTo(FilterOrder.ROUTE_COALESCING)
                .isLessThan(NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER);
    }

    /**
     * <b>본문 상한이 두 라우트에 다 붙어야 한다.</b>
     *
     * <p>지우면 헤더가 나간 뒤 본문이 안 끝나는 뒷단이 커넥션을 영영 붙잡습니다.
     * 필터 자체를 아무리 잘 시험해도, <b>안 붙어 있으면 아무것도 안 막습니다.</b>
     */
    @Test
    @DisplayName("두_라우트가_본문_상한을_쓰기_필터보다_앞에_단다")
    void 두_라우트가_본문_상한을_쓰기_필터보다_앞에_단다() {
        Route 발급 = 잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1/issue");
        Route 조회 = 잡는_라우트(HttpMethod.GET, "/api/v1/coupons");

        assertThat(실린_순서(발급, "BodyDeadline"))
                .isEqualTo(FilterOrder.ROUTE_BODY)
                .isLessThan(NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER);
        assertThat(실린_순서(조회, "BodyDeadline"))
                .isEqualTo(FilterOrder.ROUTE_BODY)
                .isLessThan(NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER);
    }

    /**
     * <b>매진 관찰도 쓰기 필터보다 앞이어야 한다.</b>
     *
     * <p>뒤에 서면 응답을 쓰는 것은 바깥의 쓰기 필터이고 그쪽은 자기가 받은
     * exchange 를 씁니다 — 우리가 감싼 것은 한 번도 안 불리고, 캐시가 영원히
     * 비어 있으면서 지표는 "매진이 없었다" 와 구별되지 않습니다.
     */
    @Test
    @DisplayName("발급_라우트가_매진_관찰을_쓰기_필터보다_앞에_단다")
    void 발급_라우트가_매진_관찰을_쓰기_필터보다_앞에_단다() {
        Route 발급 = 잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1/issue");

        assertThat(실린_순서(발급, "SoldOut"))
                .isEqualTo(FilterOrder.ROUTE_SOLD_OUT)
                .isLessThan(NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER);
    }

    /**
     * <b>조회에는 안 단다.</b> 조회 응답의 매진 코드는 재고 정보이지 뒷단이
     * 발급을 거절한 사실이 아닙니다 — 그걸 관찰로 담으면 팔고 있는 쿠폰이 막힙니다.
     */
    @Test
    @DisplayName("조회_라우트에는_매진_관찰을_안_단다")
    void 조회_라우트에는_매진_관찰을_안_단다() {
        Route 조회 = 잡는_라우트(HttpMethod.GET, "/api/v1/coupons");

        // **빈 목록에서도 통과하면 안 된다.** 필터가 통째로 빠져도 "안 달렸다"
        // 는 참이 되므로, 달려야 할 것이 달렸다는 것을 같이 본다.
        assertThat(이름들(조회))
                .anySatisfy(이름 -> assertThat(이름).contains("QueryCoalescing"))
                .noneSatisfy(이름 -> assertThat(이름).contains("SoldOut"));
    }

    /** 실린 필터의 이름들. <b>이미 있는 벗기기를 쓴다</b> — 두 벗기기가 갈리면 안 된다. */
    private static List<String> 이름들(Route route) {
        return 벗긴_필터(route).stream().map(Object::toString).toList();
    }

    /**
     * <b>조회에도 끊는 자리가 있어야 한다.</b> 모으기가 붙은 뒤로는 멎은 요청
     * 하나가 그 키를 영구히 잠근다 — 뒤이어 오는 모든 조회가 끝나지 않는 것에
     * 붙고, 뒷단이 살아나도 게이트웨이를 재시작해야 풀린다.
     */
    @Test
    @DisplayName("조회_라우트도_응답_상한을_들고_있다")
    void 조회_라우트도_응답_상한을_들고_있다() {
        Route 조회 = 라우트().stream()
                .filter(r -> "coupons".equals(r.getId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("coupons 라우트가 없다"));

        assertThat(조회.getMetadata()).containsEntry("response-timeout", 응답_상한.toMillis());
    }

    /**
     * 값이 계약에서 나와야 한다. 라우트가 우연히 맞는 숫자를 직접 적으면
     * {@link FilterOrder} 를 고쳐도 라우트는 안 따라오고, 둘이 갈린 채로 돈다.
     */
    @Test
    @DisplayName("라우트가_계약에_적힌_순서를_그대로_단다")
    void 라우트가_계약에_적힌_순서를_그대로_단다() {
        Route 발급 = 잡는_라우트(HttpMethod.POST, "/api/v1/coupons/c1/issue");

        assertThat(실린_순서(발급, "Admission")).isEqualTo(FilterOrder.ROUTE_ADMISSION);
        assertThat(실린_순서(발급, "CircuitBreaker")).isEqualTo(FilterOrder.ROUTE_CIRCUIT);
    }

    /** 값 자체도 못 박는다. 순서만 보면 둘 다 0 으로 되돌려도 이번엔 통과한다. */
    @Test
    @DisplayName("판정과_서킷의_앞뒤가_값으로_정해져_있다")
    void 판정과_서킷의_앞뒤가_값으로_정해져_있다() {
        assertThat(FilterOrder.ROUTE_ADMISSION).isLessThan(FilterOrder.ROUTE_CIRCUIT);
    }

    private RouteLocator 라우터(RoutingProperties routing) {
        return new GatewayRoutes().routes(
                new RouteLocatorBuilder(컨텍스트),
                new GatewayRoutes.Backend("http://backend:8080", 응답_상한),
                AdmissionGatewayFilter.withIsolatedSoldOutCache(재료_없는_홀더(),
                        AdmissionDecider.of(공유_리미터, 0.7),
                        Clock.systemUTC(), new SimpleMeterRegistry(),
                        FakeQueuePort.create(),
                        QueueToken.of("not-a-real-secret-0123456789abcdef"),
                        공유_리미터,
                        EntryToken.of("not-a-real-secret-0123456789abcdef"),
                        IdempotencyKey.passThrough()),
                QueryCoalescingFilter.of(
                        new CoalescingProperties(false, 1024, 1 << 20, 100, List.of()),
                        Clock.systemUTC(), new SimpleMeterRegistry()),
                SoldOutObserver.ofPublishedAt(
                        SoldOutCache.standard(), Instant::now, new SimpleMeterRegistry()),
                컨텍스트.getBean(SpringCloudCircuitBreakerResilience4JFilterFactory.class),
                new SimpleMeterRegistry(), 라우팅(routing));
    }

    private static List<String> 주소들(RouteLocator locator) {
        return locator.getRoutes().collectList().block().stream()
                .map(r -> r.getUri().toString()).toList();
    }

    /**
     * <b>켜면 균형기를 거친다.</b> 단일 주소로 두면 인스턴스를 고를 자리가
     * 아예 없어, 이 페이즈가 만든 것이 한 번도 안 돈다.
     */
    @Test
    @DisplayName("라우팅을_켜면_lb_로_보낸다")
    void 라우팅을_켜면_lb_로_보낸다() {
        RouteLocator locator = 라우터(new RoutingProperties(
                true, "coupon-service", null, null, null, null));

        assertThat(주소들(locator)).allMatch("lb://coupon-service"::equals);
    }

    /**
     * <b>끄면 단일 주소로 돌아간다.</b> 설정 한 줄이 롤백 수단이다 —
     * 코드가 남아 있어도 무해해야 그 롤백이 성립한다.
     */
    @Test
    @DisplayName("라우팅을_끄면_단일_주소다")
    void 라우팅을_끄면_단일_주소다() {
        RouteLocator locator = 라우터(new RoutingProperties(
                false, "coupon-service", null, null, null, null));

        assertThat(주소들(locator)).allMatch("http://backend:8080"::equals);
    }

    /** 배선이 아예 없으면 단일 주소다. 라우팅을 안 넣은 배포가 그 자리다. */
    @Test
    @DisplayName("배선이_없으면_단일_주소다")
    void 배선이_없으면_단일_주소다() {
        assertThat(주소들(locator)).allMatch("http://backend:8080"::equals);
    }
}

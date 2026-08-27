package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigurationProperties;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.handler.RoutePredicateHandlerMapping;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.http.HttpMethod;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.reactive.function.server.support.RouterFunctionMapping;
import org.springframework.web.server.ServerWebExchange;

/**
 * 라우트가 <b>실제로 뜬 컨텍스트에</b> 있는가.
 *
 * <p>단위로 만든 라우트 정의가 맞아도, 설정 이름이 어긋나거나 자동 구성이
 * 덮으면 뜬 애플리케이션에는 다른 것이 올라간다.
 */
@Tag("context")
@SpringBootTest
class GatewayWiringTest {

    @Autowired
    private RouteLocator locator;

    @Autowired
    private CorsPolicy.Origins origins;

    @Autowired
    private GatewayRoutes.Backend backend;

    @Autowired
    private RouterFunctionMapping routerFunctionMapping;

    @Autowired
    private BackendCircuitProperties circuit;

    @Autowired
    private CircuitBreakerRegistry circuitRegistry;

    /**
     * <b>라이브러리가 실제로 바인딩한 값을 본다.</b> yml 문자열을 {@code @Value} 로
     * 다시 읽으면 우리가 적은 것을 우리가 확인하는 것뿐이라, 키가 라이브러리가
     * 보는 이름과 어긋나도 초록이다.
     */
    @Autowired
    private Resilience4JConfigurationProperties resilience4j;

    @Autowired
    private RoutePredicateHandlerMapping gatewayMapping;

    @Test
    @DisplayName("적은_라우트만_올라온다")
    void 적은_라우트만_올라온다() {
        // **정확히 같은지 본다.** 자동 구성이 하나 더 얹으면 프록시 범위가
        // 조용히 넓어지는데, 포함만 보면 그걸 못 잡는다.
        assertThat(locator.getRoutes().collectList().block())
                .extracting(Route::getId)
                .containsExactlyInAnyOrder("issue", "coupons");
    }

    @Test
    @DisplayName("라우터_함수가_게이트웨이보다_먼저_잡는다")
    void 라우터_함수가_게이트웨이보다_먼저_잡는다() {
        // **프레임워크가 정한 순서만 본다.** 우리 코드가 사이에 없으므로, 이건
        // 순번 조회가 안 새는 것을 재는 게 아니라 그 전제가 아직 성립하는지만
        // 본다. 안 새는 것은 라우트가 그 경로를 안 잡는 쪽이 잰다.
        //
        // 핸들러 자체는 아직 없다 — CY-402 가 붙일 때 이 순서 위에 선다.
        assertThat(routerFunctionMapping.getOrder()).isLessThan(gatewayMapping.getOrder());
    }

    @Test
    @DisplayName("라우트가_설정한_뒷단으로_간다")
    void 라우트가_설정한_뒷단으로_간다() {
        // id 만 보면 어디로 보내는지는 아무도 안 본다. 주소가 어긋나도 초록이다.
        assertThat(locator.getRoutes().collectList().block())
                .allSatisfy(r -> assertThat(r.getUri().toString())
                        .isEqualTo(backend.uri()));
    }

    @Test
    @DisplayName("설정에_적은_오리진이_그대로_올라온다")
    void 설정에_적은_오리진이_그대로_올라온다() {
        // 설정 이름이 어긋나면 목록이 빈 채로 뜬다. 그러면 프론트가 통째로 막히는데
        // 기동은 성공한다.
        // 값까지 본다. 비지 않았는지만 보면 설정 파일의 오타가 그대로 통과한다.
        // 값까지 못 박는다. 비지 않았는지만 보면 엉뚱한 오리진이 들어가도 통과한다.
        assertThat(origins.allowed()).containsExactly("http://localhost:5173");
    }

    /**
     * <b>단위로 만든 라우터 함수가 맞아도 뜬 컨텍스트에 없으면 404 다.</b>
     * 시험이 설정 클래스를 직접 만들면 {@code @Configuration} 을 지워도 초록이고,
     * 그때 서킷이 열리는 순간 사용자가 받는 것은 "없는 경로" 다.
     */
    @Test
    @DisplayName("fallback_라우터가_컨텍스트에_올라온다")
    void fallback_라우터가_컨텍스트에_올라온다() {
        MockServerWebExchange 넘어옴 = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        BackendFallbackRoutes.FALLBACK_ISSUE));
        // 게이트웨이가 넘긴 표식을 단다. 없으면 안 잡는 것이 맞다 — 밖에서 치면
        // 서킷 상태 지표가 오르고, 그걸로 회복을 판정한다.
        넘어옴.getAttributes().put(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR, "issue");

        // 매핑까지 해석한다. 빈만 보면 경로가 어긋나도 통과한다.
        //
        // **무엇에 물렸는지까지 본다.** 아무 핸들러나 잡혀도 통과하면, 다른
        // 라우터 함수가 이 경로를 먼저 가져가는 회귀를 못 본다.
        assertThat(routerFunctionMapping.getHandler(넘어옴).block())
                .asString()
                .contains("BackendFallback");
    }

    /**
     * 서킷 필터가 넘길 주소와 받는 주소가 <b>같은 상수에서 나와야</b> 한다.
     * 갈리면 기동은 되고 장애 때만 404 가 드러난다.
     */
    @Test
    @DisplayName("fallback_주소가_한_곳에서_나온다")
    void fallback_주소가_한_곳에서_나온다() {
        assertThat(BackendFallbackRoutes.FALLBACK_ISSUE).isEqualTo("/fallback/issue");
    }

    /**
     * <b>설정이 실제로 실려야 한다.</b> yml 의 키가 하나라도 어긋나면 그 값만
     * 조용히 라이브러리 기본값으로 돌아간다 — 건수 창 100 은 100K RPS 에서 수 ms
     * 분량이라 순간 변동에 서킷이 열린다. 기동은 성공한다.
     */
    @Test
    @DisplayName("서킷_설정이_yml에서_올라온다")
    void 서킷_설정이_yml에서_올라온다() {
        assertThat(circuit.slidingWindowSize()).isEqualTo(Duration.ofSeconds(10));
        assertThat(circuit.minimumNumberOfCalls()).isEqualTo(20);
        assertThat(circuit.failureRateThreshold()).isEqualTo(50f);
        assertThat(circuit.slowCallDurationThreshold()).isEqualTo(Duration.ofMillis(1500));
        assertThat(circuit.slowCallRateThreshold()).isEqualTo(50f);
        assertThat(circuit.waitDurationInOpenState()).isEqualTo(Duration.ofSeconds(5));
        assertThat(circuit.maxWaitDurationInHalfOpenState()).isEqualTo(Duration.ofSeconds(30));
        assertThat(circuit.permittedNumberOfCallsInHalfOpenState()).isEqualTo(10);
    }

    /**
     * <b>실제로 쓰이는 서킷이 yml 값을 달고 있어야 한다.</b> 레지스트리 빈이
     * 있는지만 보면, 그것을 {@code ofDefaults()} 로 되돌려도 초록이다 — 그때는
     * 건수 창 100 으로 돌고 100K RPS 에서 수 ms 마다 열린다.
     */
    @Test
    @DisplayName("서킷_레지스트리가_yml_값을_달고_올라온다")
    void 서킷_레지스트리가_yml_값을_달고_올라온다() {
        CircuitBreakerConfig 실린_것 = circuitRegistry
                .circuitBreaker(GatewayRoutes.CIRCUIT).getCircuitBreakerConfig();

        assertThat(실린_것.getSlidingWindowType())
                .isEqualTo(CircuitBreakerConfig.SlidingWindowType.TIME_BASED);
        assertThat(실린_것.getSlidingWindowSize())
                .isEqualTo((int) circuit.slidingWindowSize().toSeconds());
        assertThat(실린_것.getMinimumNumberOfCalls()).isEqualTo(circuit.minimumNumberOfCalls());
        assertThat(실린_것.getFailureRateThreshold()).isEqualTo(circuit.failureRateThreshold());
        assertThat(실린_것.getSlowCallRateThreshold()).isEqualTo(circuit.slowCallRateThreshold());
        assertThat(실린_것.getSlowCallDurationThreshold())
                .isEqualTo(circuit.slowCallDurationThreshold());
        assertThat(실린_것.getMaxWaitDurationInHalfOpenState())
                .isEqualTo(circuit.maxWaitDurationInHalfOpenState());
        assertThat(실린_것.getPermittedNumberOfCallsInHalfOpenState())
                .isEqualTo(circuit.permittedNumberOfCallsInHalfOpenState());
        assertThat(실린_것.getWaitIntervalFunctionInOpenState().apply(1))
                .isEqualTo(circuit.waitDurationInOpenState().toMillis());
    }

    /**
     * <b>아무도 정하지 않은 타임아웃이 켜져 있으면 안 된다.</b> 라이브러리는
     * 안 끄면 1초를 거는데, 그 값은 뒷단이 정상 처리한 발급을 중간에 끊는다.
     * 입장 토큰은 소모되지 않으므로 같은 사람이 다시 발급받는다 — 초과 발급이다.
     *
     * <p>뒷단 요청 타임아웃은 6.2 에서 멱등 키와 함께 정한다.
     */
    @Test
    @DisplayName("정하지_않은_타임아웃이_안_켜진다")
    void 정하지_않은_타임아웃이_안_켜진다() {
        assertThat(resilience4j.isDisableTimeLimiter()).isTrue();
        // 이름별 예외로 발급 서킷만 다시 켜 두지 않았는지도 본다. 여기 한 줄이면
        // 위의 전역 끄기가 그 서킷에 대해서만 조용히 뒤집힌다.
        assertThat(resilience4j.getDisableTimeLimiterMap())
                .doesNotContainEntry(GatewayRoutes.CIRCUIT, false);
    }

    /**
     * <b>밖에서 직접 치면 안 잡는다.</b> 이 경로는 신원 필터와 남용 리미터의
     * {@code /api/**} 밖이라 아무나 칠 수 있는데, 한 번마다 서킷 상태 지표가
     * 오른다 — 밖에서 회복 판정을 흔들 수 있다.
     */
    @Test
    @DisplayName("밖에서_친_fallback은_컨텍스트도_안_잡는다")
    void 밖에서_친_fallback은_컨텍스트도_안_잡는다() {
        ServerWebExchange 직접 = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                        BackendFallbackRoutes.FALLBACK_ISSUE));

        assertThat(routerFunctionMapping.getHandler(직접).block()).isNull();
    }

    /**
     * <b>끊는 자리의 앞뒤가 값으로 정해져 있어야 합니다.</b> 격벽 시한이 뒷단 응답
     * 상한보다 먼저 오면, 서킷에 가는 것이 오류가 아니라 취소가 됩니다. 취소는
     * 창에 안 쌓이므로 멎은 뒷단의 서킷이 영영 안 열리고, 그동안 게이트웨이는
     * 죽은 뒷단에 계속 밀어 넣습니다.
     */
    @Test
    @DisplayName("격벽_시한은_뒷단_응답_상한보다_뒤다")
    void 격벽_시한은_뒷단_응답_상한보다_뒤다() {
        assertThat(AdmissionGatewayFilter.MAX_IN_FLIGHT)
                .isGreaterThan(backend.responseTimeout());
    }

    /**
     * <b>느림의 기준은 상한보다 앞이어야 합니다.</b> 뒤에 있으면 상한 직전까지
     * 느려진 인스턴스가 전부 성공으로 집계되어 서킷이 안 열립니다 (6.1.8).
     */
    @Test
    @DisplayName("느림_기준은_뒷단_응답_상한보다_앞이다")
    void 느림_기준은_뒷단_응답_상한보다_앞이다() {
        assertThat(circuit.slowCallDurationThreshold())
                .isLessThan(backend.responseTimeout());
    }
}

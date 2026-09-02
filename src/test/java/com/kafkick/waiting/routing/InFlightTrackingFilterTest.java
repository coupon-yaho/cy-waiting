package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.DefaultResponse;
import org.springframework.cloud.client.loadbalancer.EmptyResponse;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.ReactiveLoadBalancerClientFilter;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * 나간 요청을 세고 <b>어느 경로로 끝나든</b> 되돌린다.
 *
 * <p>감소를 한 경로라도 놓치면 그 인스턴스의 카운터가 영구히 부풀고, 고르개가
 * 그 인스턴스를 영원히 배제한다 (G9.3).
 */
@Tag("unit")
class InFlightTrackingFilterTest {

    private static final long 지금 = 1_800_000_000_000L;

    private final InFlightRegistry 레지스트리 = InFlightRegistry.of(Duration.ofSeconds(30));

    private final InFlightTrackingFilter 필터 =
            InFlightTrackingFilter.of(레지스트리, () -> 지금);

    private static ServerWebExchange 고른_판(String instanceId) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/api/v1/coupons/c1/issue"));
        ServiceInstance instance =
                new DefaultServiceInstance(instanceId, "coupon-service", "10.0.1.7", 8080, false);
        exchange.getAttributes().put(ServerWebExchangeUtils.GATEWAY_LOADBALANCER_RESPONSE_ATTR,
                new DefaultResponse(instance));
        return exchange;
    }

    private static ServerWebExchange 안_고른_판() {
        return MockServerWebExchange.from(MockServerHttpRequest.post("/api/v1/coupons/c1/issue"));
    }

    /** 뒷단이 답할 때까지 물려 있다. 안 세면 부하율이 늘 0 이라 고르개가 눈이 먼다. */
    @Test
    @DisplayName("도는_동안_물린_것으로_센다")
    void 도는_동안_물린_것으로_센다() {
        ServerWebExchange 판 = 고른_판("be-1");
        GatewayFilterChain 사슬 = ex ->
                Mono.fromRunnable(() -> assertThat(레지스트리.count("be-1", 지금)).isEqualTo(1));

        StepVerifier.create(필터.filter(판, 사슬)).verifyComplete();

        assertThat(레지스트리.count("be-1", 지금)).isZero();
    }

    /** <b>에러로 끝나도 되돌린다.</b> 여기를 놓치면 실패가 잦은 대가 영영 배제된다. */
    @Test
    @DisplayName("에러로_끝나도_되돌린다")
    void 에러로_끝나도_되돌린다() {
        GatewayFilterChain 사슬 = ex -> Mono.error(new IllegalStateException("뒷단이 끊었다"));

        StepVerifier.create(필터.filter(고른_판("be-1"), 사슬))
                .verifyError(IllegalStateException.class);

        assertThat(레지스트리.count("be-1", 지금)).isZero();
    }

    /** <b>취소돼도 되돌린다.</b> 클라이언트가 끊는 것은 장애 구간에 가장 흔하다. */
    @Test
    @DisplayName("취소돼도_되돌린다")
    void 취소돼도_되돌린다() {
        GatewayFilterChain 사슬 = ex -> Mono.never();

        필터.filter(고른_판("be-1"), 사슬).subscribe().dispose();

        assertThat(레지스트리.count("be-1", 지금)).isZero();
    }

    /** 균형기를 안 탄 요청은 안 센다. 세면 없는 인스턴스의 카운터가 는다. */
    @Test
    @DisplayName("안_고른_요청은_안_센다")
    void 안_고른_요청은_안_센다() {
        StepVerifier.create(필터.filter(안_고른_판(), ex -> Mono.empty())).verifyComplete();

        assertThat(레지스트리.instances()).isEmpty();
    }

    /** 고를 대가 없었던 판도 안 센다. 빈 답에는 인스턴스가 없다. */
    @Test
    @DisplayName("빈_답인_판은_안_센다")
    void 빈_답인_판은_안_센다() {
        MockServerWebExchange 판 = MockServerWebExchange.from(
                MockServerHttpRequest.post("/api/v1/coupons/c1/issue"));
        판.getAttributes().put(ServerWebExchangeUtils.GATEWAY_LOADBALANCER_RESPONSE_ATTR,
                new EmptyResponse());

        StepVerifier.create(필터.filter(판, ex -> Mono.empty())).verifyComplete();

        assertThat(레지스트리.instances()).isEmpty();
    }

    /**
     * <b>고르는 자리보다 뒤여야 한다.</b> 앞에 서면 아직 아무도 안 골랐고,
     * 균형기 안에서 세면 재시도가 다시 골라 한 요청이 두 번 세어진다.
     */
    @Test
    @DisplayName("균형기_뒤에_선다")
    void 균형기_뒤에_선다() {
        assertThat(필터.getOrder()).isGreaterThan(
                ReactiveLoadBalancerClientFilter.LOAD_BALANCER_CLIENT_FILTER_ORDER);
    }

    /** 부하가 끝나면 전 인스턴스가 0 이다 (G9.3). */
    @Test
    @DisplayName("전부_끝나면_0_으로_수렴한다")
    void 전부_끝나면_0_으로_수렴한다() {
        for (String id : List.of("be-1", "be-2", "be-1")) {
            StepVerifier.create(필터.filter(고른_판(id), ex -> Mono.empty())).verifyComplete();
        }

        assertThat(레지스트리.count("be-1", 지금)).isZero();
        assertThat(레지스트리.count("be-2", 지금)).isZero();
    }
}

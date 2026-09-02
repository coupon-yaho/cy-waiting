package com.kafkick.waiting.routing;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.ReactiveLoadBalancerClientFilter;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 인스턴스로 나간 요청을 세고, <b>어느 경로로 끝나든</b> 되돌린다.
 *
 * <p>감소를 한 경로라도 놓치면 그 인스턴스의 카운터가 영구히 부풀고, 부하율이
 * 계속 높게 보여 고르개가 그 인스턴스를 영원히 배제한다 (G9.3).
 */
// **`doFinally` 한 자리에 모은다.** 성공·실패·취소를 각각 처리하면 그중 하나가
// 빠지고, 빠진 것은 누수가 쌓인 뒤에야 보인다.
public final class InFlightTrackingFilter implements GlobalFilter, Ordered {

    /**
     * 균형기가 인스턴스를 고른 <b>뒤</b>, 뒷단으로 보내기 <b>전</b>이다.
     *
     * <p>고르는 자리에서 세면 안 된다 — 재시도가 다시 고르므로 한 요청이 두 번
     * 세어지고, 그중 하나는 영영 안 빠진다.
     */
    private static final int ORDER =
            ReactiveLoadBalancerClientFilter.LOAD_BALANCER_CLIENT_FILTER_ORDER + 1;

    private final InFlightRegistry registry;

    private final LongSupplier nowMillis;

    private InFlightTrackingFilter(InFlightRegistry registry, LongSupplier nowMillis) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.nowMillis = Objects.requireNonNull(nowMillis, "nowMillis");
    }

    public static InFlightTrackingFilter of(InFlightRegistry registry, LongSupplier nowMillis) {
        return new InFlightTrackingFilter(registry, nowMillis);
    }

    @Override
    public int getOrder() {
        return ORDER;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String instanceId = chosenInstanceId(exchange);
        if (instanceId == null) {
            // 균형기를 안 탄 요청이다. 세면 없는 인스턴스의 카운터가 는다.
            return chain.filter(exchange);
        }
        InFlightRegistry.Ticket ticket = registry.started(instanceId, nowMillis.getAsLong());
        return chain.filter(exchange).doFinally(signal -> ticket.finished());
    }

    /** 균형기가 고른 인스턴스. 안 골랐으면 {@code null} 이다. */
    private String chosenInstanceId(ServerWebExchange exchange) {
        Object raw = exchange.getAttribute(
                ServerWebExchangeUtils.GATEWAY_LOADBALANCER_RESPONSE_ATTR);
        if (!(raw instanceof Response<?> response) || !response.hasServer()) {
            return null;
        }
        Object server = response.getServer();
        return server instanceof ServiceInstance instance ? instance.getInstanceId() : null;
    }
}

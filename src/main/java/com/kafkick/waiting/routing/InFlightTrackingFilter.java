package com.kafkick.waiting.routing;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.ReactiveLoadBalancerClientFilter;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

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

    private final InstanceOutliers outliers;

    private final LongSupplier nowMillis;

    private InFlightTrackingFilter(InFlightRegistry registry, InstanceOutliers outliers,
            LongSupplier nowMillis) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.outliers = Objects.requireNonNull(outliers, "outliers");
        this.nowMillis = Objects.requireNonNull(nowMillis, "nowMillis");
    }

    public static InFlightTrackingFilter of(InFlightRegistry registry,
            InstanceOutliers outliers, LongSupplier nowMillis) {
        return new InFlightTrackingFilter(registry, outliers, nowMillis);
    }

    @Override
    public int getOrder() {
        return ORDER;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ReservedInstance reserved = reserved(exchange);
        if (reserved == null) {
            // 균형기를 안 탄 요청이다. 놓을 자리도 없다.
            return chain.filter(exchange);
        }
        // **놓는 것만 여기서 한다.** 잡는 것은 고르는 자리에서 원자적으로 끝났다 —
        // 여기서 잡으면 읽고 세는 사이에 동시 요청이 상한을 넘긴다.
        return chain.filter(exchange).doFinally(signal -> {
            reserved.release();
            record(reserved.getInstanceId(), exchange, signal);
        });
    }

    /**
     * 이 인스턴스가 제 몫을 했는지 남긴다.
     *
     * <p><b>이 자리는 서킷 안쪽이다.</b> 폴백이 오류를 정상 코드로 바꾸기 전이라
     * 뒷단이 실제로 낸 것을 본다. 바깥에서 보면 전부 성공으로 보인다.
     */
    private void record(String instanceId, ServerWebExchange exchange, SignalType signal) {
        // **취소는 어느 쪽으로도 안 센다.** 클라이언트가 끊은 것이라 인스턴스의
        // 잘못이 아니고, 실패로 세면 사용자가 창을 닫은 것만으로 뒷단이 빠진다.
        if (signal == SignalType.CANCEL) {
            return;
        }
        if (signal == SignalType.ON_ERROR) {
            outliers.failed(instanceId, nowMillis.getAsLong());
            return;
        }
        HttpStatusCode status = exchange.getResponse().getStatusCode();
        // **4xx 는 실패가 아니다.** 잘못된 요청은 어느 인스턴스로 보내도 같은
        // 답이 온다. 그걸로 빼면 나쁜 클라이언트 하나가 뒷단을 지운다.
        if (status != null && status.is5xxServerError()) {
            outliers.failed(instanceId, nowMillis.getAsLong());
            return;
        }
        outliers.succeeded(instanceId);
    }

    /** 균형기가 자리를 잡아 준 인스턴스. 안 골랐으면 {@code null} 이다. */
    private ReservedInstance reserved(ServerWebExchange exchange) {
        Object raw = exchange.getAttribute(
                ServerWebExchangeUtils.GATEWAY_LOADBALANCER_RESPONSE_ATTR);
        if (!(raw instanceof Response<?> response) || !response.hasServer()) {
            return null;
        }
        Object server = response.getServer();
        return server instanceof ReservedInstance instance ? instance : null;
    }
}

package com.kafkick.waiting.routing;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.ReactiveLoadBalancerClientFilter;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
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

    /**
     * 인스턴스마다 다른 답이 오는 4xx. <b>이것들은 그 대의 상태다.</b>
     *
     * <p>포화된 대는 429 로 흘리고, 시크릿이 안 풀린 대는 401·403 을, 자기 쪽
     * 시한이 지난 대는 408 을 낸다. 넷 다 즉시 끝나 물린 건수가 안 쌓이므로,
     * 성공으로 세면 그 대가 계속 가장 한가해 보여 트래픽이 오히려 몰린다.
     */
    private static final Set<Integer> INSTANCE_FAULT = Set.of(
            HttpStatus.UNAUTHORIZED.value(), HttpStatus.FORBIDDEN.value(),
            HttpStatus.REQUEST_TIMEOUT.value(), HttpStatus.TOO_MANY_REQUESTS.value());

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
        return chain.filter(exchange)
                // **오류는 여기서 적는다.** `doFinally` 는 콜백을 하류로 신호를
                // 넘긴 **뒤에** 돌린다. 재시도는 백오프가 없어 그 신호 안에서
                // 곧바로 다시 구독하므로, `doFinally` 에 두면 두 번째 선택이
                // 빈 목록을 보고 방금 죽은 대를 또 고른다. `doOnError` 는
                // 하류보다 먼저 돈다.
                .doOnError(e -> failed(reserved.getInstanceId(), exchange))
                .doFinally(signal -> {
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
        // **취소는 어느 쪽으로도 안 센다.** 우리 시한이 끊은 것과 사용자가 받다가
        // 끊은 것이 지금 구조로는 안 갈린다 — 응답이 시작됐는지로 가르려 했으나
        // 받다가 끊는 이탈도 그 자리다. 잘못 빼는 쪽이 더 나쁘다: 이탈은 장애
        // 구간에 몰리므로, 그때 멀쩡한 대가 차례로 빠진다.
        //
        // **그래서 놓치는 것은 거의 없다.** 헤더만 주고 본문을 안 끝내는 대는
        // 본문 시한이 오류로 끊으므로 위에서 실패로 세어진다. 응답 자체가 안
        // 오는 대는 응답 상한이 오류로 끊는다. 취소로만 끝나는 것은 격벽 시한
        // 뿐인데, 그것은 응답 상한보다 뒤라 대개 앞에서 이미 잡힌다.
        if (signal == SignalType.CANCEL) {
            return;
        }
        // 오류는 위 `doOnError` 가 이미 적었다. 여기서 또 적으면 한 시도가
        // 두 번 세어져 배제 임계가 절반이 된다.
        if (signal == SignalType.ON_ERROR) {
            return;
        }
        HttpStatusCode status = exchange.getResponse().getStatusCode();
        if (status != null && status.is5xxServerError()) {
            failed(instanceId, exchange);
            return;
        }
        if (status != null && INSTANCE_FAULT.contains(status.value())) {
            failed(instanceId, exchange);
            return;
        }
        // **나머지 4xx 는 어느 쪽으로도 안 센다.** 잘못된 요청은 어느 대로 보내도
        // 같은 답이 오므로 실패가 아니고, 그렇다고 성공으로 세면 500·400 을
        // 번갈아 내는 대가 매번 연속을 끊어 영영 안 빠진다.
        if (status != null && status.is4xxClientError()) {
            return;
        }
        outliers.succeeded(instanceId, nowMillis.getAsLong());
    }

    /**
     * 실패를 배제기에 세고 <b>요청에도 적는다.</b> 재시도는 같은 요청을 다시
     * 고르므로, 여기 적힌 것을 균형기가 읽어야 다음 대로 넘어간다.
     */
    @SuppressWarnings("unchecked")
    private void failed(String instanceId, ServerWebExchange exchange) {
        outliers.failed(instanceId, nowMillis.getAsLong());
        // 재시도가 되돌리는 것은 응답 쪽뿐이라 요청 속성은 시도 사이에 남는다.
        Set<String> tried = (Set<String>) exchange.getAttributes()
                .computeIfAbsent(RoutingAttributes.TRIED, k -> ConcurrentHashMap.newKeySet());
        tried.add(instanceId);
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

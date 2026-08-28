package com.kafkick.waiting.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import com.kafkick.waiting.control.HealthConfig;
import java.util.function.IntSupplier;
import org.springframework.cloud.gateway.filter.factory.SpringCloudCircuitBreakerFilterFactory;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.GatewayFilterSpec;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.time.Duration;
import java.util.function.Predicate;
import org.springframework.http.HttpMethod;
import org.springframework.web.server.ServerWebExchange;

/**
 * 프록시할 경로를 <b>명시적으로만</b> 적는다. 설정에 이름으로 적으면 필터가
 * 안 풀려도 기동이 성공하고 판정만 사라진다 — 인스턴스를 직접 붙여 그 실패를 없앤다.
 */
@Configuration
@EnableConfigurationProperties(GatewayRoutes.Backend.class)
public class GatewayRoutes {

    /**
     * 술어는 디코딩해 맞추고 전달은 원본을 그대로 보낸다. 좁히지 않으면 판정한
     * 값과 뒷단이 받는 값이 갈리고, 그 값이 레디스 키·캐시 키·리미터 키가 된다.
     */
    private static final String COUPON_ID = "{couponId:[A-Za-z0-9_-]{1,64}}";

    /** 프레임워크가 라우트별 응답 상한을 읽는 키. 이름을 틀리면 조용히 안 걸린다. */
    private static final String RESPONSE_TIMEOUT_ATTR = "response-timeout";

    /**
     * 서킷이 열렸을 때 넘길 주소.
     *
     * <p><b>받는 주소와 같은 상수에서 나온다.</b> 갈리면 기동은 되고 장애 때만
     * 404 가 드러난다 — 사용자에게 404 는 매진으로 읽혀 다시 오지 않는다.
     */
    public static final String FALLBACK_URI = "forward:" + BackendFallbackRoutes.FALLBACK_ISSUE;

    /**
     * 서킷의 이름.
     *
     * <p>지금은 뒷단 주소가 하나라 하나뿐이다. 가용량 기반 분배(Phase 9)가 붙으면
     * <b>인스턴스마다 따로 이름을 잡는다</b> (R-10) — 뒷단 전체를 하나로 묶으면
     * 한 대가 죽어도 전 트래픽이 막힌다.
     */
    public static final String CIRCUIT = "backend";

    /**
     * 관례로 쓰이는 클라이언트 IP 헤더. 프레임워크는 {@code X-Forwarded-*} 만
     * 지우므로 이것들은 그대로 넘어가고, 뒷단이 하나라도 믿으면 IP 단위 제한이
     * 헤더 한 줄로 우회된다.
     */
    private static final String[] SPOOFABLE_CLIENT_IP =
            {"X-Real-IP", "X-Client-IP", "True-Client-IP", "CF-Connecting-IP"};

    private GatewayFilterSpec stripSpoofableClientIp(GatewayFilterSpec spec) {
        GatewayFilterSpec stripped = spec;
        for (String header : SPOOFABLE_CLIENT_IP) {
            stripped = stripped.removeRequestHeader(header);
        }
        return stripped;
    }

    /**
     * 원본 경로도 같이 본다. 술어는 세그먼트를 디코딩하고 매트릭스 파라미터를
     * 떼어 낸 값으로 맞추는데 전달은 원본을 보낸다 — 갈리면 판정한 쿠폰과 뒷단이
     * 받는 쿠폰이 달라진다.
     */
    private Predicate<ServerWebExchange> rawPathIsPlain() {
        return exchange -> {
            String raw = exchange.getRequest().getURI().getRawPath();
            return raw.indexOf('%') < 0 && raw.indexOf(';') < 0;
        };
    }

    /** 뒷단 쿠폰 서비스. 가용량 기반 분배는 Phase 9 다 — 여기서는 하나만 본다. */
    @ConfigurationProperties("waiting.backend")
    public record Backend(String uri, Duration responseTimeout) {

        // 검증은 밖에 둔다. 압축 생성자에서 부를 수 있는 것은 정적뿐이라,
        // 안에 두면 그 자리에서만 쓰이는 정적 메서드가 생긴다.
        public Backend {
            ConfigUris.create().backend(uri);
            if (responseTimeout == null || responseTimeout.toMillis() < 1) {
                throw new IllegalArgumentException(
                        "responseTimeout 은 1ms 이상이어야 한다: " + responseTimeout);
            }
            // **격벽 시한보다 앞이어야 한다.** 뒤에 있으면 격벽이 먼저 끊고, 그때
            // 서킷에 가는 것은 오류가 아니라 취소다 — 취소는 창에 안 쌓여 멎은
            // 뒷단의 서킷이 영영 안 열린다. 시험으로만 두면 배포 설정 한 줄이
            // 이 순서를 뒤집고, 그 사실은 장애 때만 드러난다.
            if (responseTimeout.compareTo(AdmissionGatewayFilter.MAX_IN_FLIGHT) >= 0) {
                throw new IllegalArgumentException(
                        "responseTimeout 은 격벽 시한(" + AdmissionGatewayFilter.MAX_IN_FLIGHT
                                + ") 보다 짧아야 한다: " + responseTimeout);
            }
        }
    }

    /**
     * 서킷 필터를 손으로 만든다.
     *
     * <p>{@code circuitBreaker(...)} 는 order 를 줄 자리가 없어 0 으로 붙는다.
     * 판정도 0 이면 둘의 앞뒤가 안정 정렬에만 기대게 된다.
     */
    private GatewayFilter circuit(SpringCloudCircuitBreakerFilterFactory breakers) {
        SpringCloudCircuitBreakerFilterFactory.Config config =
                new SpringCloudCircuitBreakerFilterFactory.Config();
        config.setName(CIRCUIT);
        config.setFallbackUri(FALLBACK_URI);
        config.setRouteId("issue");
        return breakers.apply(config);
    }

    /**
     * 걸려 있는 건수를 값으로 냅니다.
     *
     * <p>제어 평면이 종료할 때 이 값을 봅니다. <b>타입이 아니라 값으로 냅니다</b> —
     * 게이트웨이 타입을 그쪽에서 참조하면 제어 평면이 요청 경로를 알게 됩니다.
     */
    @Bean(HealthConfig.IN_FLIGHT)
    public IntSupplier inFlightRequests(AdmissionGatewayFilter admission) {
        return admission::inFlight;
    }

    /**
     * 본문 쓰기 상한. <b>응답 상한의 배수</b>라 배포 없이 같이 움직인다.
     *
     * <p>따로 적으면 응답 상한만 고쳤을 때 두 값이 갈리고, 그때 어느 쪽이 먼저
     * 끊는지가 바뀐다.
     */
    private BodyDeadline bodyDeadline(Backend backend, MeterRegistry meters) {
        return BodyDeadline.of(backend.responseTimeout().multipliedBy(2),
                backend.responseTimeout(), meters);
    }

    @Bean
    public RouteLocator routes(RouteLocatorBuilder builder, Backend backend,
            AdmissionGatewayFilter admission, QueryCoalescingFilter coalescing,
            SoldOutObserver soldOut, SpringCloudCircuitBreakerFilterFactory breakers,
            MeterRegistry meters) {
        BodyDeadline bodyDeadline = bodyDeadline(backend, meters);
        return builder.routes()
                .route("issue", r -> r
                        .method(HttpMethod.POST)
                        .and().path("/api/v1/coupons/" + COUPON_ID + "/issue")
                        .and().predicate(rawPathIsPlain())
                        // **앞뒤를 값으로 정한다.** 안 정하면 둘 다 0 이라 선언
                        // 위치를 옮기는 것만으로 순서가 바뀌고, 서킷이 판정 앞으로
                        // 가면 래치가 죽는다 (FilterOrder).
                        .filters(f -> stripSpoofableClientIp(f)
                                .filter(admission, FilterOrder.ROUTE_ADMISSION)
                                .filter(circuit(breakers), FilterOrder.ROUTE_CIRCUIT)
                                // **발급에만 붙인다.** 조회 응답에는 매진 코드가
                                // 재고 정보로 실릴 수 있고, 그건 관찰이 아니다.
                                .filter(soldOut, FilterOrder.ROUTE_SOLD_OUT)
                                // **본문이 안 끝나는 뒷단을 끊는다.** 응답 상한은
                                // 헤더가 오기까지만 재므로 그 뒤로는 아무것도
                                // 안 걸리고, 헤더가 나간 뒤라 판정 쪽 시한도
                                // 커넥션을 못 끊는다.
                                .filter(bodyDeadline, FilterOrder.ROUTE_BODY))
                        // **끊는 자리가 서킷 안쪽이어야 한다.** 밖에서 끊으면
                        // 서킷에 가는 것은 오류가 아니라 취소이고, 취소는 창에
                        // 안 쌓인다 — 멎은 뒷단의 서킷이 영영 안 열린다.
                        .metadata(RESPONSE_TIMEOUT_ATTR, backend.responseTimeout().toMillis())
                        .uri(backend.uri()))
                .route("coupons", r -> r
                        .method(HttpMethod.GET)
                        .and().path("/api/v1/coupons", "/api/v1/coupons/" + COUPON_ID)
                        .and().predicate(rawPathIsPlain())
                        // **조회에만 붙인다.** 발급에 붙이면 같은 응답을 여럿이
                        // 받고, 그건 곧 초과 발급이다.
                        .filters(f -> stripSpoofableClientIp(f)
                                .filter(coalescing, FilterOrder.ROUTE_COALESCING)
                                .filter(bodyDeadline, FilterOrder.ROUTE_BODY))
                        // **여기도 끊는 자리가 있어야 한다.** 모으기가 붙은 뒤로는
                        // 멎은 요청 하나가 그 키를 영구히 잠근다 — 뒤이어 오는
                        // 모든 조회가 끝나지 않는 것에 붙고, 뒷단이 살아나도
                        // 게이트웨이를 재시작해야 풀린다.
                        .metadata(RESPONSE_TIMEOUT_ATTR, backend.responseTimeout().toMillis())
                        .uri(backend.uri()))
                .build();
    }
}

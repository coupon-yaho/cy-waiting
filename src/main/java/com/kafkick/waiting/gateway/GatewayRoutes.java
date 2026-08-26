package com.kafkick.waiting.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.GatewayFilterSpec;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
    private static final String CIRCUIT = "backend";

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
    public record Backend(String uri) {

        // 검증은 밖에 둔다. 압축 생성자에서 부를 수 있는 것은 정적뿐이라,
        // 안에 두면 그 자리에서만 쓰이는 정적 메서드가 생긴다.
        public Backend {
            ConfigUris.create().backend(uri);
        }
    }

    @Bean
    public RouteLocator routes(RouteLocatorBuilder builder, Backend backend,
            AdmissionGatewayFilter admission) {
        return builder.routes()
                .route("issue", r -> r
                        .method(HttpMethod.POST)
                        .and().path("/api/v1/coupons/" + COUPON_ID + "/issue")
                        .and().predicate(rawPathIsPlain())
                        .filters(f -> stripSpoofableClientIp(f)
                                .filter(admission)
                                // **판정 뒤에 건다.** 앞에 걸면 서킷이 열린 동안
                                // 판정이 아예 안 돌아, 이 노드가 줄을 세운 적 없는
                                // 것으로 보이고 래치가 표식을 못 받는다.
                                .circuitBreaker(c -> c.setName(CIRCUIT)
                                        .setFallbackUri(FALLBACK_URI)))
                        .uri(backend.uri()))
                .route("coupons", r -> r
                        .method(HttpMethod.GET)
                        .and().path("/api/v1/coupons", "/api/v1/coupons/" + COUPON_ID)
                        .and().predicate(rawPathIsPlain())
                        .filters(this::stripSpoofableClientIp)
                        .uri(backend.uri()))
                .build();
    }
}

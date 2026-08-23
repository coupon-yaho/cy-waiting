package com.kafkick.waiting.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.GatewayFilterSpec;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.net.URI;
import java.net.URISyntaxException;
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
     * 원본 경로도 같이 본다. 술어는 세그먼트를 디코딩하고 매트릭스 파라미터를
     * 떼어 낸 값으로 맞추는데, 전달은 원본을 그대로 보낸다 — 그 둘이 갈리면
     * 판정한 쿠폰과 뒷단이 받는 쿠폰이 달라진다.
     */
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

    private Predicate<ServerWebExchange> rawPathIsPlain() {
        return exchange -> {
            String raw = exchange.getRequest().getURI().getRawPath();
            return raw.indexOf('%') < 0 && raw.indexOf(';') < 0;
        };
    }

    /** 뒷단 쿠폰 서비스. 가용량 기반 분배는 Phase 9 다 — 여기서는 하나만 본다. */
    @ConfigurationProperties("waiting.backend")
    public record Backend(String uri) {

        // **형태까지 본다.** 스킴이 빠진 값은 기동에 성공하고 모든 프록시가
        // 실패한다 — 없애려던 조용한 실패가 그대로 남는다. 경로가 붙은 값도
        // 경로만 조용히 버려진다. 값은 안 싣는다. 자격 증명이 들어갈 수 있다.
        public Backend {
            URI parsed = parsed(uri);
            if (!"http".equals(parsed.getScheme()) && !"https".equals(parsed.getScheme())) {
                throw new IllegalArgumentException("waiting.backend.uri 는 http 나 https 여야 한다");
            }
            if (parsed.getHost() == null) {
                throw new IllegalArgumentException("waiting.backend.uri 에 호스트가 없다");
            }
            if (parsed.getPath() != null && !parsed.getPath().isEmpty()) {
                throw new IllegalArgumentException("waiting.backend.uri 에 경로를 붙일 수 없다");
            }
        }

        // RULE-EXCEPTION(JS-13): 레코드의 압축 생성자는 인스턴스가 서기 전에
        // 돈다. 인스턴스 메서드로 둘 수 없다.
        private static URI parsed(String uri) {
            if (uri == null || uri.isBlank()) {
                throw new IllegalArgumentException("waiting.backend.uri 가 비어 있다");
            }
            try {
                return new URI(uri);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("waiting.backend.uri 를 못 읽었다", e);
            }
        }
    }

    @Bean
    public RouteLocator routes(RouteLocatorBuilder builder, Backend backend) {
        return builder.routes()
                .route("issue", r -> r
                        .method(HttpMethod.POST)
                        .and().path("/api/v1/coupons/" + COUPON_ID + "/issue")
                        .and().predicate(rawPathIsPlain())
                        .filters(f -> stripSpoofableClientIp(f).filter(new AdmissionGatewayFilter()))
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

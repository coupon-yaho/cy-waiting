package com.kafkick.waiting.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;

/**
 * 프록시할 경로를 <b>명시적으로만</b> 적는다. 설정에 이름으로 적으면 필터가
 * 안 풀려도 기동이 성공하고 판정만 사라진다 — 인스턴스를 직접 붙여 그 실패를 없앤다.
 */
@Configuration
@EnableConfigurationProperties(GatewayRoutes.Backend.class)
public class GatewayRoutes {

    /** 뒷단 쿠폰 서비스. 가용량 기반 분배는 Phase 9 다 — 여기서는 하나만 본다. */
    @ConfigurationProperties("waiting.backend")
    public record Backend(String uri) {

        // 주소가 없으면 프록시가 어디로 갈지 정해지지 않는다. 기동에서 막는다.
        public Backend {
            if (uri == null || uri.isBlank()) {
                throw new IllegalArgumentException("waiting.backend.uri 가 비어 있다");
            }
        }
    }

    @Bean
    public RouteLocator routes(RouteLocatorBuilder builder, Backend backend) {
        return builder.routes()
                .route("issue", r -> r
                        .method(HttpMethod.POST)
                        .and().path("/api/v1/coupons/{couponId}/issue")
                        .filters(f -> f.filter(new AdmissionGatewayFilter()))
                        .uri(backend.uri()))
                .route("coupons", r -> r
                        .method(HttpMethod.GET)
                        .and().path("/api/v1/coupons", "/api/v1/coupons/{couponId}")
                        .uri(backend.uri()))
                .build();
    }
}

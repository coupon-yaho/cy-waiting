package com.kafkick.waiting.gateway;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsConfigurationSource;
import org.springframework.web.cors.reactive.CorsWebFilter;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

/**
 * <b>필터 계층에 둔다.</b> 라우트에만 걸면 순번 조회가 빠진다 — 그 경로는
 * 게이트웨이 라우트를 안 타고 게이트웨이가 직접 답한다.
 */
@Configuration
@EnableConfigurationProperties(CorsPolicy.Origins.class)
public class CorsPolicy {

    private static final String API = "/api/**";


    private final Origins origins;

    /** 프론트 오리진 허용 목록. 넓히면 아무 사이트나 사용자 브라우저로 이 API 를 부른다. */
    @ConfigurationProperties("waiting.cors")
    public record Origins(List<String> allowed) {

        // **비어 있으면 기동을 막는다.** 그대로 뜨면 프론트가 통째로 막히는데
        // 기동은 성공한다 — 배포하고 나서야 드러난다.
        public Origins {
            if (allowed == null || allowed.isEmpty()) {
                throw new IllegalArgumentException("waiting.cors.allowed 가 비어 있다");
            }
            // 값 하나가 어긋나면 목록이 있다는 사실이 무의미해지거나, 아무와도
            // 안 맞아 프론트가 통째로 막힌다. 검증은 밖에 둔다.
            ConfigUris uris = ConfigUris.create();
            allowed.forEach(uris::origin);
        }
    }

    CorsPolicy(Origins origins) {
        this.origins = origins;
    }

    /**
     * 설정만 만들어 두면 아무 요청에도 안 걸린다. 웹플럭스는 그 빈을 스스로
     * 집어가지 않으므로 필터로 직접 잇는다.
     */

    /**
     * 설정만 만들어 두면 아무 요청에도 안 걸린다. 웹플럭스는 그 빈을 스스로
     * 집어가지 않으므로 필터로 직접 잇는다. 앞뒤는 {@link FilterOrder} 가 정한다.
     */
    @Bean
    @Order(FilterOrder.CORS)
    public CorsWebFilter corsWebFilter(CorsConfigurationSource source) {
        return new CorsWebFilter(source);
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration config = new CorsConfiguration();
        // 와일드카드를 안 쓴다. 열어 두면 허용 목록이 있다는 사실이 무의미해진다.
        config.setAllowedOrigins(List.copyOf(origins.allowed()));
        config.setAllowedMethods(List.of(HttpMethod.GET.name(), HttpMethod.POST.name()));
        // 뒷단이 요구하는 것을 다 넣는다. 하나라도 빠지면 브라우저가 사전 요청에서
        // 막고 본 요청을 아예 안 보낸다 — 그 엔드포인트가 브라우저에서 통째로 안 된다.
        config.setAllowedHeaders(List.of("Content-Type", "X-Member-Id", "X-Member-Grade",
                "Entry-Token", "Idempotency-Key", "X-Request-Id"));
        // **읽게 해 주지 않으면 안 보낸 것과 같다.** 교차 출처 스크립트는 기본
        // 여섯 헤더만 볼 수 있어, 여기 없으면 추적 키도 재시도 안내도 못 읽는다.
        config.setExposedHeaders(List.of("X-Request-Id", HttpHeaders.RETRY_AFTER));
        config.setMaxAge(3_600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        // 관리 경로는 뺀다. 브라우저가 부를 것이 아니다.
        source.registerCorsConfiguration(API, config);
        return source;
    }
}

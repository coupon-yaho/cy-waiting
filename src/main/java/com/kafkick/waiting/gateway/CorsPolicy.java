package com.kafkick.waiting.gateway;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
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

    /**
     * 둘의 앞뒤를 값으로 못 박는다. 안 정하면 선언 순서로 정해져, 자리를 옮기는
     * 것만으로 사전 요청이 막힌다.
     */
    private static final int CORS_ORDER = Ordered.HIGHEST_PRECEDENCE + 100;

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
     * <b>형식 검증보다 앞에 선다.</b> 브라우저는 사전 요청에 회원 헤더를 안 붙이는데,
     * 뒤에 서면 그게 막히고 브라우저는 본 요청을 아예 안 보낸다.
     */
    @Bean
    @Order(CORS_ORDER)
    public CorsWebFilter corsWebFilter(CorsConfigurationSource source) {
        return new CorsWebFilter(source);
    }

    @Bean
    @Order(CORS_ORDER + 1)
    public MemberIdentityFilter memberIdentityFilter() {
        return MemberIdentityFilter.create();
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration config = new CorsConfiguration();
        // 와일드카드를 안 쓴다. 열어 두면 허용 목록이 있다는 사실이 무의미해진다.
        config.setAllowedOrigins(List.copyOf(origins.allowed()));
        config.setAllowedMethods(List.of(HttpMethod.GET.name(), HttpMethod.POST.name()));
        config.setAllowedHeaders(List.of("Content-Type", "X-Member-Id", "X-Member-Grade"));
        config.setMaxAge(3_600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        // 관리 경로는 뺀다. 브라우저가 부를 것이 아니다.
        source.registerCorsConfiguration(API, config);
        return source;
    }
}

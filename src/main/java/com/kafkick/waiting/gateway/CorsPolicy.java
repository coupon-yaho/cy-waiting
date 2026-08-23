package com.kafkick.waiting.gateway;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsConfigurationSource;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

/**
 * 브라우저가 대기열 API 를 부를 수 있게 한다.
 *
 * <p><b>필터 계층에 둔다.</b> 라우트에만 걸면 순번 조회가 빠진다 — 그 경로는
 * 게이트웨이 라우트를 안 타고 게이트웨이가 직접 답하기 때문이다.
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
        }
    }

    CorsPolicy(Origins origins) {
        this.origins = origins;
    }

    /** 스프링이 아닌 곳에서 만들 때 쓴다. 생성자를 열면 우회 경로가 하나 더 생긴다. */
    public static CorsPolicy of(Origins origins) {
        return new CorsPolicy(origins);
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

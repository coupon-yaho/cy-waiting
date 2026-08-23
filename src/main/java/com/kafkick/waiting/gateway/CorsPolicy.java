package com.kafkick.waiting.gateway;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
            // 안 맞아 프론트가 통째로 막힌다. 어긋난 항목만 짚는다 — 목록을
            // 통째로 실으면 기동 실패 로그에 아직 안 알려진 호스트명이 흘러간다.
            allowed.forEach(Origins::checked);
        }

        /** {@code "null"} 은 와일드카드 검사를 지나면서 와일드카드처럼 맞는다. */
        // RULE-EXCEPTION(JS-13): 레코드의 압축 생성자는 인스턴스가 서기 전에
        // 돈다. 인스턴스 메서드로 둘 수 없다.
        private static void checked(String origin) {
            if (origin == null || origin.isBlank() || origin.contains("*")
                    || "null".equalsIgnoreCase(origin.trim())) {
                throw new IllegalArgumentException("오리진에 쓸 수 없는 값이 있다");
            }
            URI parsed;
            try {
                parsed = new URI(origin);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("오리진을 못 읽었다", e);
            }
            // 스킴이 빠지면 기동은 되고 아무와도 안 맞는다. 프론트가 조용히 막힌다.
            if (!"http".equals(parsed.getScheme()) && !"https".equals(parsed.getScheme())
                    || parsed.getHost() == null) {
                throw new IllegalArgumentException("오리진은 http 나 https 로 시작하는 주소여야 한다");
            }
        }
    }

    CorsPolicy(Origins origins) {
        this.origins = origins;
    }

    /**
     * 설정만 만들어 두면 아무 요청에도 안 걸린다. 웹플럭스는 그 빈을 스스로
     * 집어가지 않으므로 필터로 직접 잇는다.
     */
    @Bean
    public CorsWebFilter corsWebFilter(CorsConfigurationSource source) {
        return new CorsWebFilter(source);
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

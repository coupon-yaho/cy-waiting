package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.server.ServerWebExchange;

/**
 * 브라우저가 대기열 API 를 부를 수 있는가.
 *
 * <p>허용 목록을 넓히면 아무 사이트나 사용자 브라우저로 이 API 를 부른다.
 * 좁으면 프론트가 통째로 막힌다 — 둘 다 조용해서 배포 뒤에야 드러난다.
 */
class CorsPolicyTest {

    private final CorsPolicy policy = new CorsPolicy(
            new CorsPolicy.Origins(List.of("https://front.example")));

    private CorsConfiguration 적용되는_설정(String path) {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.GET, path).build());
        return policy.corsConfigurationSource().getCorsConfiguration(exchange);
    }

    @Test
    @DisplayName("허용_목록의_오리진만_통과한다")
    void 허용_목록의_오리진만_통과한다() {
        CorsConfiguration 설정 = 적용되는_설정("/api/v1/coupons/c1/issue");

        assertThat(설정.checkOrigin("https://front.example")).isEqualTo("https://front.example");
        assertThat(설정.checkOrigin("https://evil.example")).isNull();
    }

    @Test
    @DisplayName("순번_조회에도_같은_설정이_걸린다")
    void 순번_조회에도_같은_설정이_걸린다() {
        // 순번 조회는 게이트웨이 라우트를 안 탄다. 라우트에만 걸면 폴링이
        // 브라우저에서 통째로 막히고, 그건 대기 화면이 안 도는 것이다.
        assertThat(적용되는_설정("/api/v1/coupons/c1/queue"))
                .extracting(c -> c.checkOrigin("https://front.example"))
                .isEqualTo("https://front.example");
    }

    @Test
    @DisplayName("모든_오리진을_열지_않는다")
    void 모든_오리진을_열지_않는다() {
        // 와일드카드로 열면 허용 목록이 있다는 사실 자체가 무의미해진다.
        CorsConfiguration 설정 = 적용되는_설정("/api/v1/coupons/c1/issue");

        assertThat(설정.getAllowedOrigins()).doesNotContain("*");
        assertThat(설정.getAllowedOriginPatterns()).isNullOrEmpty();
    }

    @Test
    @DisplayName("오류_메시지에_목록을_안_싣는다")
    void 오류_메시지에_목록을_안_싣는다() {
        // 기동 실패 로그가 그대로 흘러간다. 아직 안 알려진 내부 호스트명이 거기 있다.
        assertThatThrownBy(() -> new CorsPolicy.Origins(List.of("https://internal.example", "*")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("internal.example");
    }

    @Test
    @DisplayName("허용_목록이_비면_기동을_막는다")
    void 허용_목록이_비면_기동을_막는다() {
        // 빈 목록으로 그대로 뜨면 프론트가 통째로 막히는데 기동은 성공한다.
        assertThatThrownBy(() -> new CorsPolicy.Origins(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CorsPolicy.Origins(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("와일드카드나_빈_값은_기동을_막는다")
    void 와일드카드나_빈_값은_기동을_막는다() {
        // 값 하나로 전부 열린다. 나머지가 맞아도 목록이 있다는 사실이 무의미해진다.
        assertThatThrownBy(() -> new CorsPolicy.Origins(List.of("*")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CorsPolicy.Origins(List.of("https://*.example")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CorsPolicy.Origins(List.of("https://front.example", " ")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("주소가_아닌_값은_기동을_막는다")
    void 주소가_아닌_값은_기동을_막는다() {
        // "null" 은 와일드카드 검사를 지나면서 샌드박스 프레임에 그대로 맞는다.
        assertThatThrownBy(() -> new CorsPolicy.Origins(List.of("null")))
                .isInstanceOf(IllegalArgumentException.class);
        // 스킴이 빠지면 기동은 되고 아무와도 안 맞는다 — 프론트가 조용히 막힌다.
        assertThatThrownBy(() -> new CorsPolicy.Origins(List.of("localhost:5173")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CorsPolicy.Origins(List.of("htp://front.example")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("관리_경로에는_안_걸린다")
    void 관리_경로에는_안_걸린다() {
        // 관리 포트는 브라우저가 부를 것이 아니다.
        assertThat(적용되는_설정("/actuator/health")).isNull();
    }
}

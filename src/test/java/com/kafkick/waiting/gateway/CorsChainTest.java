package com.kafkick.waiting.gateway;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * CORS 가 <b>실제 요청 체인에</b> 걸리는가.
 *
 * <p>설정 객체만 만들어 두면 그 자체는 맞는데 아무 요청에도 안 걸린다.
 * 정책을 직접 불러 보는 시험은 그 상태와 제대로 걸린 상태를 구별하지 못한다.
 */
@Tag("context")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CorsChainTest {

    private static final String ORIGIN = "http://localhost:5173";

    @LocalServerPort
    private int port;

    private WebTestClient client;

    @BeforeEach
    void 붙는다() {
        client = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port).build();
    }

    @Test
    @DisplayName("사전_요청에_허용_헤더가_실린다")
    void 사전_요청에_허용_헤더가_실린다() {
        client.options().uri("/api/v1/coupons/c1/queue")
                .header(HttpHeaders.ORIGIN, ORIGIN)
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .exchange()
                .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, ORIGIN);
    }

    @Test
    @DisplayName("허용_밖_오리진은_사전_요청이_막힌다")
    void 허용_밖_오리진은_사전_요청이_막힌다() {
        client.options().uri("/api/v1/coupons/c1/queue")
                .header(HttpHeaders.ORIGIN, "https://evil.example")
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    @DisplayName("발급_경로에도_같이_걸린다")
    void 발급_경로에도_같이_걸린다() {
        client.options().uri("/api/v1/coupons/c1/issue")
                .header(HttpHeaders.ORIGIN, ORIGIN)
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "POST")
                .exchange()
                .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, ORIGIN);
    }
}

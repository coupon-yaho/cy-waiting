package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
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
    @DisplayName("실제_요청_응답에도_허용_헤더가_실린다")
    void 실제_요청_응답에도_허용_헤더가_실린다() {
        // **사전 요청만 보면 절반만 재는 것이다.** 사전 요청은 필터가 그 자리에서
        // 끊어서 라우팅도 핸들러도 안 탄다. 실제 응답에 헤더가 안 실리면 브라우저는
        // 결과를 못 읽는데, 사전 요청 시험은 그대로 초록이다.
        client.get().uri("/api/v1/coupons/c1/queue")
                .header(HttpHeaders.ORIGIN, ORIGIN)
                .exchange()
                .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, ORIGIN);
    }

    @Test
    @DisplayName("보낼_수_있는_헤더를_사전_요청이_알려_준다")
    void 보낼_수_있는_헤더를_사전_요청이_알려_준다() {
        // 회원 식별자를 못 보내면 뒷단이 누구인지 모른다. 목록에서 빠지면
        // 브라우저가 그 요청을 아예 안 보낸다.
        client.options().uri("/api/v1/coupons/c1/issue")
                .header(HttpHeaders.ORIGIN, ORIGIN)
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "POST")
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS,
                        "X-Member-Id,X-Member-Grade,Entry-Token,Idempotency-Key")
                .exchange()
                .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, ORIGIN)
                .expectHeader().value(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS,
                        v -> assertThat(v).contains("X-Member-Id", "X-Member-Grade",
                                "Entry-Token", "Idempotency-Key"));
    }

    @Test
    @DisplayName("계약이_요구하는_헤더가_전부_열려_있다")
    void 계약이_요구하는_헤더가_전부_열려_있다() {
        // 발급은 `Entry-Token` 이, 사용·취소는 멱등키가 필수다. 하나라도 빠지면
        // 브라우저가 본 요청을 아예 안 보내 그 엔드포인트가 통째로 안 된다.
        for (String 헤더 : List.of("Content-Type", "X-Member-Id", "X-Member-Grade",
                "Entry-Token", "Idempotency-Key", "X-Request-Id")) {
            client.options().uri("/api/v1/coupons/c1/issue")
                    .header(HttpHeaders.ORIGIN, ORIGIN)
                    .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .header(HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS, 헤더)
                    .exchange()
                    .expectStatus().value(s -> assertThat(s)
                            .as("헤더 %s", 헤더).isNotEqualTo(403));
        }
    }

    @Test
    @DisplayName("정해진_메서드만_사전_요청이_허용한다")
    void 정해진_메서드만_사전_요청이_허용한다() {
        // 넓히면 브라우저가 쓰기 요청을 더 보낼 수 있게 된다.
        client.options().uri("/api/v1/coupons/c1/issue")
                .header(HttpHeaders.ORIGIN, ORIGIN)
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "DELETE")
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

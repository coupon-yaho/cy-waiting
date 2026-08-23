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
 * 형식 검증이 <b>실제 요청 체인에</b> 걸리는가.
 *
 * <p>필터를 만들어 두고 안 걸면 그 자체는 맞는데 아무 요청도 안 지난다.
 * 필터를 직접 부르는 시험은 그 상태와 제대로 걸린 상태를 구별하지 못한다.
 */
@Tag("context")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IdentityChainTest {

    @LocalServerPort
    private int port;

    private WebTestClient client;

    @BeforeEach
    void 붙는다() {
        client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    @DisplayName("헤더가_없으면_순번_조회가_막힌다")
    void 헤더가_없으면_순번_조회가_막힌다() {
        // 이 경로는 게이트웨이 라우트를 안 탄다. 라우트 필터로만 붙이면 안 걸린다.
        client.get().uri("/api/v1/coupons/c1/queue")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody().jsonPath("$.error.code").isEqualTo("COMMON-001");
    }

    @Test
    @DisplayName("헤더가_없으면_발급도_막힌다")
    void 헤더가_없으면_발급도_막힌다() {
        client.post().uri("/api/v1/coupons/c1/issue")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("관리_경로는_안_막힌다")
    void 관리_경로는_안_막힌다() {
        // 프로브가 막히면 살아 있는 노드가 통째로 빠진다.
        client.get().uri("/actuator/health")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("사전_요청은_회원_헤더_없이_통과한다")
    void 사전_요청은_회원_헤더_없이_통과한다() {
        // **브라우저는 사전 요청에 회원 헤더를 안 붙인다.** 여기서 막으면
        // 본 요청을 아예 안 보내고, 대기 화면이 통째로 안 돈다.
        client.options().uri("/api/v1/coupons/c1/queue")
                .header(HttpHeaders.ORIGIN, "http://localhost:5173")
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .exchange()
                .expectStatus().is2xxSuccessful();
    }
}

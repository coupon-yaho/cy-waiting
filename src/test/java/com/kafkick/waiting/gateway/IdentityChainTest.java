package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.WebFilter;
import org.springframework.boot.test.web.server.LocalManagementPort;
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
@Import(IdentityChainTest.CapturingBackend.class)
class IdentityChainTest {

    /**
     * 형식 검증 뒤에 서서 <b>지나간 요청을 잡는다.</b> 실서버 시험이 거절만 재면
     * 필터가 뜨는 것만 알 뿐, 제대로 된 요청이 살아남는지는 모른다.
     */
    @TestConfiguration
    static class CapturingBackend {

        static final AtomicReference<HttpHeaders> 마지막 = new AtomicReference<>();

        @Bean
        @Order(FilterOrder.IDENTITY + 1)
        WebFilter capturing() {
            return (exchange, chain) -> {
                // 회원 API 만 가로챈다. 전부 잡으면 관리 경로 시험이 이 스텁을 잰다.
                if (!exchange.getRequest().getPath().value().startsWith("/api/")) {
                    return chain.filter(exchange);
                }
                마지막.set(exchange.getRequest().getHeaders());
                exchange.getResponse().setStatusCode(HttpStatus.NO_CONTENT);
                return exchange.getResponse().setComplete();
            };
        }
    }

    private static final String ORIGIN = "http://localhost:5173";

    @LocalServerPort
    private int port;

    @LocalManagementPort
    private int managementPort;

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
                .expectStatus().isBadRequest()
                .expectBody().jsonPath("$.error.code").isEqualTo("COMMON-001");
    }

    @ParameterizedTest
    @ValueSource(strings = {"/%61pi/v1/coupons/c1/queue", "/ap%69/v1/coupons/c1/issue"})
    @DisplayName("인코딩해_들어와도_막는다")
    void 인코딩해_들어와도_막는다(String 인코딩된_경로) {
        // **원본 경로를 문자열로 비교하면 여기가 뚫린다.** 필터에는 회원 API 로
        // 안 보이는데 라우터는 그대로 잡는다 — 검증 없이 지나간다.
        //
        // 목 요청으로는 못 만든다. 그 하네스가 `%` 를 다시 인코딩한다.
        client.get().uri(URI.create("http://localhost:" + port + 인코딩된_경로))
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("헤더가_두_줄이면_막힌다")
    void 헤더가_두_줄이면_막힌다() {
        // 판정은 첫 줄만 보는데 전달은 전부 그대로 간다.
        client.get().uri("/api/v1/coupons/c1/queue")
                .header("X-Member-Id", "1")
                .header("X-Member-Id", "99999999999999999999")
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("제대로_된_요청은_헤더째_지나간다")
    void 제대로_된_요청은_헤더째_지나간다() {
        // 거절만 재면 필터가 뜨는 것만 알 뿐, 통과가 되는지는 모른다.
        CapturingBackend.마지막.set(null);

        client.get().uri("/api/v1/coupons/c1/queue")
                .header("X-Member-Id", "12345")
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .expectStatus().isNoContent();

        // 이름 집합을 정확히 못 박는다. 하나라도 늘거나 줄면 걸린다.
        assertThat(CapturingBackend.마지막.get().headerNames())
                .as("뒷단까지 닿아야 잡힌다")
                .filteredOn(n -> n.startsWith("X-Member"))
                .containsExactlyInAnyOrder("X-Member-Id", "X-Member-Grade");
        assertThat(CapturingBackend.마지막.get().getFirst("X-Member-Id")).isEqualTo("12345");
        assertThat(CapturingBackend.마지막.get().getFirst("X-Member-Grade")).isEqualTo("GOLD");
    }

    @Test
    @DisplayName("프로브가_도는_포트에서_안_막힌다")
    void 프로브가_도는_포트에서_안_막힌다() {
        // **관리 포트로 재야 한다.** 서비스 포트의 404 는 필터가 허용해서가 아니라
        // 거기 핸들러가 없어서다. 하위 컨텍스트도 이 필터를 물려받으므로,
        // 막히면 살아 있는 노드가 통째로 빠진다.
        WebTestClient 관리 = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + managementPort).build();

        // **판정까지 못 박는다.** 400 이 아닌 것만 보면 404 나 500 도 통과해서,
        // 프로브가 다른 이유로 죽어도 이 시험은 초록이다.
        //
        // 여기서는 재료가 없어 준비 판정이 내려간다 — 그게 정상이고, 볼 것은
        // 필터가 아니라 헬스가 답했다는 것이다.
        byte[] 본문 = 관리.get().uri("/actuator/health")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
                .expectBody().returnResult().getResponseBody();

        assertThat(new String(본문, StandardCharsets.UTF_8))
                .doesNotContain(ApiError.INVALID_REQUEST)
                .contains("status");
    }

    @Test
    @DisplayName("사전_요청은_회원_헤더_없이_통과한다")
    void 사전_요청은_회원_헤더_없이_통과한다() {
        // **브라우저는 사전 요청에 회원 헤더를 안 붙인다.** 여기서 막으면
        // 본 요청을 아예 안 보내고, 대기 화면이 통째로 안 돈다.
        // **2xx 만으로는 성공이 아니다.** 허용 헤더가 안 실리면 브라우저는
        // 사전 요청이 200 이어도 본 요청을 안 보낸다.
        client.options().uri("/api/v1/coupons/c1/queue")
                .header(HttpHeaders.ORIGIN, ORIGIN)
                .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .exchange()
                .expectStatus().is2xxSuccessful()
                .expectHeader().valueEquals(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, ORIGIN)
                .expectHeader().value(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS,
                        v -> assertThat(v).contains("GET"));
    }
}

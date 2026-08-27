package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.SnapshotSource;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.cloud.gateway.filter.NettyWriteResponseFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * 모으기가 <b>실제 요청 체인에서</b> 도는가.
 *
 * <p>단위 시험은 목 응답을 씁니다. 그건 쓰기가 그 자리에서 끝나서, 본문을 감싸는
 * 자리가 틀려도 통과합니다 — 실제로 그 상태로 부하까지 돌렸고, 응답은 200 인데
 * 길이만 0 이었습니다. 상태만 보는 검사로는 완벽히 도는 것처럼 보입니다.
 */
@Tag("context")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "waiting.coalescing.enabled=true",
            "waiting.coalescing.max-body-bytes=262144",
            "waiting.coalescing.max-keys=100",
            "waiting.coalescing.routes[0].path=/api/v1/coupons",
            "waiting.coalescing.routes[0].ttl=300ms",
        })
@Import(CoalescingChainTest.NoMaterial.class)
class CoalescingChainTest {

    /** 몇 번 불렸는지가 이 시험의 값이다. */
    private static final AtomicInteger 뒷단_호출 = new AtomicInteger();

    /** 매번 다른 본문을 낸다. 담아 둔 것을 받았는지가 본문으로 갈린다. */
    private static final DisposableServer 뒷단 = HttpServer.create()
            .port(0)
            .handle((request, response) -> response
                    .header("Content-Type", "application/json")
                    .sendString(Mono.fromSupplier(
                            () -> "{\"n\":" + 뒷단_호출.incrementAndGet() + "}")))
            .bindNow();

    @DynamicPropertySource
    static void 뒷단을_가리킨다(DynamicPropertyRegistry registry) {
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
    }

    @AfterAll
    static void 내린다() {
        뒷단.disposeNow();
    }

    @TestConfiguration
    static class NoMaterial {

        /** 조회는 판정을 안 지난다. 재료가 있으면 이 시험이 판정을 같이 잰다. */
        @Bean
        @Primary
        SnapshotSource 빈_재료() {
            return () -> Mono.just(Map.of());
        }
    }

    @LocalServerPort
    private int port;

    /**
     * <b>본문을 감싸는 자리가 맞아야 합니다.</b> 프레임워크의 쓰기 필터보다 뒤에
     * 서면 우리가 감싼 응답을 아무도 안 쓰고, 담는 것이 늘 빈 본문이 됩니다.
     */
    @Test
    @DisplayName("모으기는_응답을_쓰는_필터보다_앞에_선다")
    void 모으기는_응답을_쓰는_필터보다_앞에_선다() {
        assertThat(FilterOrder.ROUTE_COALESCING)
                .as("모으기 순서")
                .isLessThan(NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER);
    }

    /**
     * <b>모아 준 응답도 온전해야 합니다.</b> 뒷단 도달 수를 줄이면서 본문이 비면
     * 그건 보호가 아니라 사고입니다.
     */
    @Test
    @DisplayName("모아_준_응답도_본문이_온전하다")
    void 모아_준_응답도_본문이_온전하다() {
        WebTestClient client = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port).build();
        int 시작 = 뒷단_호출.get();

        String 첫째 = 본문(client);
        String 둘째 = 본문(client);

        assertThat(첫째).as("첫 응답").isNotBlank().startsWith("{\"n\":");
        // 뒷단은 매번 다른 본문을 낸다. 같다는 것이 곧 담아 둔 것을 받았다는 뜻이다.
        assertThat(둘째).as("담아 둔 것을 받은 응답").isEqualTo(첫째);
        assertThat(뒷단_호출.get() - 시작).as("뒷단 호출").isEqualTo(1);
    }

    private String 본문(WebTestClient client) {
        byte[] body = client.get().uri("/api/v1/coupons")
                .header("X-Member-Id", "812934")
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.OK)
                .expectBody()
                .returnResult()
                .getResponseBody();
        return body == null ? "" : new String(body, StandardCharsets.UTF_8);
    }
}

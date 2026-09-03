package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.gateway.GatewayRoutes;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
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
 * <b>뒷단이 TCP 는 받고 응답을 안 보내는 구간</b> (C18).
 *
 * <p>이 구간을 안 재면 끊는 자리가 서킷 바깥으로 새도 아무 시험이 안 깨집니다.
 * 그때 서킷은 표본이 0 이라 영원히 닫힌 채고, 게이트웨이는 죽은 뒷단에 계속
 * 밀어 넣습니다.
 */
@Tag("context")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(BackendStallTest.StallingBackend.class)
class BackendStallTest {

    /** 판정 재료의 발행 시각. 시계와 같은 값이라 늘 갓 받은 재료다. */
    private static final Instant 지금 = Instant.parse("2026-08-27T00:00:00Z");

    /** 붙기는 하는데 응답을 안 주는 뒷단. 커넥션 거부와 다른 상황이다. */
    private static final DisposableServer 멎은_뒷단 = HttpServer.create()
            .port(0)
            .handle((request, response) -> Mono.never())
            .bindNow();

    /**
     * <b>짧게 잡는다.</b> 운영값(12초)으로 재면 시험 하나가 그만큼 걸린다. 재는
     * 것은 값이 아니라 "끊기는가, 그리고 그 끊김이 서킷에 쌓이는가" 다.
     */
    private static final Duration 응답_상한 = Duration.ofMillis(300);
    // 응답 상한을 줄인 판이라 연결 상한도 그보다 짧아야 한다 —
    // 기본값(500ms)을 그대로 두면 기동이 막힌다.
    private static final Duration 연결_상한 = Duration.ofMillis(100);

    @DynamicPropertySource
    static void 멎은_뒷단을_가리킨다(DynamicPropertyRegistry registry) {
        registry.add("waiting.backend.uri",
                () -> "http://localhost:" + 멎은_뒷단.port());
        registry.add("waiting.backend.response-timeout", () -> 응답_상한);
        registry.add("waiting.backend.connect-timeout", () -> 연결_상한);
        // 표본 하한을 낮춘다. 운영값 20 건을 이 시험에서 채우면 6초가 걸린다.
        registry.add("waiting.backend.circuit.minimum-number-of-calls", () -> 3);
        registry.add("waiting.backend.circuit.slow-call-duration-threshold", () -> "100ms");
    }

    @AfterAll
    static void 내린다() {
        멎은_뒷단.disposeNow();
    }

    @TestConfiguration
    static class StallingBackend {

        /** 판정이 보는 시각을 고정한다. 실시계로 두면 낡음 경계가 시험마다 다르다. */
        @Bean
        @Primary
        Clock 고정_시계() {
            return Clock.fixed(지금, ZoneOffset.UTC);
        }

        /**
         * <b>한산한 쿠폰을 심는다.</b> 비워 두면 재료가 없어 낡음 경로로 빠지고,
         * 그때 요청은 뒷단에 닿기도 전에 끝난다 — 뒷단이 멎은 것을 재려는 시험이
         * 뒷단을 안 부른다.
         */
        @Bean
        @Primary
        SnapshotSource 한산한_재료() {
            Map<String, String> 재료 = SnapshotCodec.create().encode(
                    new GatewaySnapshot(
                            Map.of("c0", CouponStates.idle(1_000_000),
                                    "c1", CouponStates.idle(1_000_000),
                                    "c2", CouponStates.idle(1_000_000)),
                            new SnapshotMeta(1_000, 1), 지금),
                    CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
            return () -> Mono.just(재료);
        }
    }

    @LocalServerPort
    private int port;

    @Autowired
    private CircuitBreakerRegistry circuits;

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                // 게이트웨이가 끊는 것을 재려면 클라이언트가 먼저 안 끊어야 한다.
                .responseTimeout(응답_상한.multipliedBy(20))
                .build();
    }

    /**
     * <b>504 가 아니라 503 + Retry-After 입니다</b> (6.2.4). 대기열 문맥에서 504 는
     * "게이트웨이가 고장났다" 로 읽히고, 다시 오라는 안내가 없습니다.
     */
    @Test
    @DisplayName("멎은_뒷단은_503_과_다시_올_시각으로_끝난다")
    void 멎은_뒷단은_503_과_다시_올_시각으로_끝난다() {
        클라이언트().post().uri("/api/v1/coupons/c1/issue")
                .header("X-Member-Id", "812934")
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
                .expectHeader().exists("Retry-After")
                .expectBody().jsonPath("$.error.code").isEqualTo("BACKEND_UNAVAILABLE");
    }

    /**
     * <b>끊긴 요청이 서킷에 쌓여야 합니다</b> (6.2.2). 끊는 자리가 서킷 바깥이면
     * 서킷이 받는 것은 오류가 아니라 취소이고, 취소는 창에 안 쌓입니다 — 표본이
     * 0 이라 서킷이 영원히 안 열리고, 그동안 죽은 뒷단에 계속 밀어 넣습니다.
     */
    @Test
    @DisplayName("끊긴_요청이_서킷의_실패로_쌓인다")
    void 끊긴_요청이_서킷의_실패로_쌓인다() {
        for (int i = 0; i < 3; i++) {
            클라이언트().post().uri("/api/v1/coupons/c" + i + "/issue")
                    .header("X-Member-Id", "81293" + i)
                    .header("X-Member-Grade", "GOLD")
                    .exchange()
                    .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        }

        // **이름으로 짚는다.** "아무 서킷이나 표본이 있으면" 으로 재면, 발급
        // 라우트가 이 서킷을 안 지나도 다른 서킷의 표본으로 통과한다.
        var backend = circuits.getAllCircuitBreakers().stream()
                .filter(c -> GatewayRoutes.CIRCUIT.equals(c.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "'" + GatewayRoutes.CIRCUIT + "' 서킷이 없다 — 요청이 서킷을 안 지났다"));

        var metrics = backend.getMetrics();
        assertThat(metrics.getNumberOfBufferedCalls())
                .as("%s 의 창에 쌓인 표본", backend.getName())
                .isPositive();
        assertThat(metrics.getNumberOfFailedCalls() + metrics.getNumberOfSlowCalls())
                .as("%s 가 센 실패·느림", backend.getName())
                .isPositive();
    }
}

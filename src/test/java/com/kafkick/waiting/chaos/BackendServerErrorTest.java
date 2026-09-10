package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.gateway.GatewayRoutes;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
 * <b>뒷단이 응답은 주는데 5xx 인 구간.</b>
 *
 * <p>응답이 오면 서킷은 성공으로 센다 — 상태를 실패로 바꾸는 배선이 따로 없으면
 * 뒷단이 전부 500 을 내도 서킷이 영영 안 열린다.
 */
@Tag("context")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(BackendServerErrorTest.IdleCoupons.class)
class BackendServerErrorTest {

    private static final Instant 지금 = Instant.parse("2026-09-11T00:00:00Z");

    /** 표본 하한. 운영값 20 건을 채우면 시험이 느려진다. */
    private static final int 표본 = 3;

    private static final AtomicInteger 받은_수 = new AtomicInteger();

    private static final DisposableServer 망가진_뒷단 = HttpServer.create()
            .port(0)
            .handle((request, response) -> {
                받은_수.incrementAndGet();
                return response.status(500).sendString(Mono.just("{}")).then();
            })
            .bindNow();

    @DynamicPropertySource
    static void 망가진_뒷단을_가리킨다(DynamicPropertyRegistry registry) {
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 망가진_뒷단.port());
        registry.add("waiting.backend.circuit.minimum-number-of-calls", () -> 표본);
    }

    @AfterAll
    static void 내린다() {
        망가진_뒷단.disposeNow();
    }

    @TestConfiguration
    static class IdleCoupons {

        @Bean
        @Primary
        Clock 고정_시계() {
            return Clock.fixed(지금, ZoneOffset.UTC);
        }

        /** 한산한 쿠폰이라야 뒷단까지 간다. 줄이 서면 뒷단을 안 부른다. */
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
        return WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    private CircuitBreaker 뒷단_서킷() {
        return circuits.getAllCircuitBreakers().stream()
                .filter(c -> GatewayRoutes.CIRCUIT.equals(c.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "'" + GatewayRoutes.CIRCUIT + "' 서킷이 없다 — 요청이 서킷을 안 지났다"));
    }

    /**
     * <b>500 을 실패로 센다.</b> 표본을 채운 뒤 서킷이 열려야 한다 — 창에 쌓였는지만
     * 보면 성공으로 쌓인 것도 통과한다.
     */
    @Test
    @DisplayName("뒷단의_500_이_서킷을_연다")
    void 뒷단의_500_이_서킷을_연다() {
        for (int i = 0; i < 표본; i++) {
            클라이언트().post().uri("/api/v1/coupons/c" + i + "/issue")
                    .header("X-Member-Id", "91234" + i)
                    .header("X-Member-Grade", "GOLD")
                    .exchange()
                    // **뒷단의 500 을 그대로 흘리지 않는다.** 대기열 문맥의 500 은
                    // 다시 오라는 안내가 없어, 폴백과 같은 답을 해야 한다.
                    .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
                    .expectHeader().exists("Retry-After")
                    .expectBody().jsonPath("$.error.code").isEqualTo("BACKEND_UNAVAILABLE");
        }

        CircuitBreaker 서킷 = 뒷단_서킷();
        assertThat(받은_수).as("뒷단까지 간 수 — 안 가면 서킷이 아니라 판정이 막은 것이다")
                .hasValue(표본);
        assertThat(서킷.getMetrics().getNumberOfFailedCalls()).as("실패로 센 수")
                .isEqualTo(표본);
        assertThat(서킷.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }
}

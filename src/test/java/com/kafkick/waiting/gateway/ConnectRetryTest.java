package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.routing.InstanceAddress;
import com.kafkick.waiting.domain.routing.InstanceRouting;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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
 * <b>연결이 안 된 인스턴스로 간 요청이 다음 대로 넘어간다</b> (9.3.11 · G9.11).
 *
 * <p>배선을 값으로만 재면(재시도 횟수·필터 순서) 배선이 <b>안 도는</b> 것을 못
 * 잡는다. 실제로 그랬다 — 재시도가 서킷 폴백에 먹혀 한 번도 안 돌았는데
 * 값 검사는 전부 초록이었다.
 */
@Tag("context")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"waiting.scheduler.enabled=false", "waiting.routing.enabled=true",
                "waiting.routing.strategy=round-robin"})
@Import(ConnectRetryTest.TwoInstances.class)
class ConnectRetryTest {

    private static final Instant 지금 = Instant.parse("2026-09-03T00:00:00Z");

    /** 살아 있는 뒷단. 넘어온 요청을 센다. */
    private static final AtomicInteger 산_대가_받은_수 = new AtomicInteger();

    private static final DisposableServer 산_대 = HttpServer.create()
            .port(0)
            .handle((request, response) -> {
                산_대가_받은_수.incrementAndGet();
                return response.status(HttpStatus.OK.value())
                        .header("Content-Type", "application/json")
                        .sendString(Mono.just("{\"ok\":true}"))
                        .then();
            })
            .bindNow();

    /** 죽은 뒷단의 자리. 띄웠다가 곧바로 내려 아무도 안 듣는 포트를 만든다. */
    private static final int 죽은_포트 = 죽은_포트를_만든다();

    private static int 죽은_포트를_만든다() {
        DisposableServer 잠깐 = HttpServer.create().port(0)
                .handle((request, response) -> response.send()).bindNow();
        int port = 잠깐.port();
        잠깐.disposeNow();
        return port;
    }

    /**
     * <b>정말로 거절하는지 먼저 본다.</b> 임시 포트 대역이라 내린 뒤 다른
     * 프로세스가 물 수 있고, 그러면 연결이 성립해 이 시험이 재려던 것 대신
     * 엉뚱한 프로세스로 프록시한 채 초록이 뜬다.
     */
    private void 죽은_자리가_맞는지_본다() {
        try (Socket probe = new Socket()) {
            probe.connect(new InetSocketAddress("127.0.0.1", 죽은_포트), 500);
            throw new AssertionError(
                    "죽은 자리 " + 죽은_포트 + " 에 누가 듣고 있다 — 이 실행으로는 못 잰다");
        } catch (IOException expected) {
            // 거절이 정상이다.
        }
    }

    /**
     * <b>둘 다 짧게 잡는다.</b> 운영값으로 재면 시험 하나가 그만큼 걸린다.
     * 연결 거절은 즉시 오므로 이 값이 판정을 흔들지 않는다.
     */
    private static final Duration 응답_상한 = Duration.ofMillis(2_000);

    private static final Duration 연결_상한 = Duration.ofMillis(500);

    @DynamicPropertySource
    static void 상한을_줄인다(DynamicPropertyRegistry registry) {
        // 라우팅이 켜지면 이 주소는 안 쓰이지만, 검증이 값을 요구한다.
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 산_대.port());
        registry.add("waiting.backend.response-timeout", () -> 응답_상한);
        registry.add("waiting.backend.connect-timeout", () -> 연결_상한);
    }

    @AfterAll
    static void 내린다() {
        산_대.disposeNow();
    }

    @TestConfiguration
    static class TwoInstances {

        @Bean
        @Primary
        Clock 고정_시계() {
            return Clock.fixed(지금, ZoneOffset.UTC);
        }

        /**
         * 죽은 대와 산 대를 <b>같은 여유</b>로 싣는다.
         *
         * <p>여유가 같아야 라운드로빈이 번갈아 고르고, 그래야 죽은 대를 먼저
         * 고르는 요청이 반드시 생긴다.
         */
        @Bean
        @Primary
        SnapshotSource 두_대짜리_재료() {
            GatewaySnapshot snapshot = new GatewaySnapshot(
                    Map.of("c1", CouponStates.idle(1_000_000)),
                    new SnapshotMeta(1_000, 1), 지금,
                    List.of(
                            new InstanceRouting("dead",
                                    new InstanceAddress("127.0.0.1", 죽은_포트), 100),
                            new InstanceRouting("live",
                                    new InstanceAddress("127.0.0.1", 산_대.port()), 100)));
            Map<String, String> 재료 = SnapshotCodec.create().encode(snapshot,
                    CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
            return () -> Mono.just(재료);
        }
    }

    @LocalServerPort
    private int port;

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(응답_상한.multipliedBy(10))
                .build();
    }

    /**
     * <b>죽은 대를 고른 요청도 200 으로 끝나야 한다.</b>
     *
     * <p>여섯 번 보내면 라운드로빈이 죽은 대를 세 번 고른다. 재시도가 안 돌면
     * 그 셋이 503 이 되고, 돌면 여섯이 다 산 대로 넘어간다.
     */
    @Test
    @DisplayName("연결이_안_된_대를_고르면_다음_대로_넘어간다")
    void 연결이_안_된_대를_고르면_다음_대로_넘어간다() {
        죽은_자리가_맞는지_본다();
        산_대가_받은_수.set(0);

        int 보낸_수 = 6;
        for (int i = 0; i < 보낸_수; i++) {
            클라이언트().post().uri("/api/v1/coupons/c1/issue")
                    .header("X-Member-Id", "90000" + i)
                    .header("X-Member-Grade", "GOLD")
                    .exchange()
                    .expectStatus().isEqualTo(HttpStatus.OK);
        }

        // **산 대가 전부 받아야 한다.** 상태만 보면 폴백이 200 을 낼 때 못 가른다.
        assertThat(산_대가_받은_수.get())
                .as("산 대가 실제로 받은 요청 수")
                .isEqualTo(보낸_수);
    }
}

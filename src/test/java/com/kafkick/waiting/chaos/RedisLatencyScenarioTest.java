package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
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
 * C2 — Redis 지연 500ms → 해제 (8.3.4 · 5절).
 *
 * <p><b>판정이 Redis 를 안 치므로 발급 지연이 변하면 안 된다.</b> 변한다면
 * 어딘가에서 치고 있다는 뜻이다 — 불변식 1 의 실전 검증이다.
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(RedisLatencyScenarioTest.StubbedBackend.class)
class RedisLatencyScenarioTest {

    private static final Instant 지금 = Instant.parse("2026-08-31T00:00:00Z");
    private static final String COUPON = "c1";

    /** 주입할 지연. 판정이 레디스를 치면 이만큼 그대로 느려진다. */
    private static final Duration 지연 = Duration.ofMillis(500);

    /**
     * 허용 배수. 지연을 500ms 넣었는데 판정이 그 절반만큼도 안 느려지면,
     * 그 경로가 레디스를 안 친다는 뜻이다.
     */
    private static final double 허용_배수 = 3.0;

    private static final int 보낼_수 = 10;

    private static final AtomicLong 뒷단이_받은_수 = new AtomicLong();

    private static final DisposableServer 뒷단 = HttpServer.create()
            .port(0)
            .handle((request, response) -> {
                뒷단이_받은_수.incrementAndGet();
                return response.status(HttpStatus.OK.value()).send();
            })
            .bindNow();

    private static RedisWireFaults 선;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        선 = RedisWireFaults.시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.host", 선::호스트);
        registry.add("spring.data.redis.port", 선::포트);
    }

    @AfterAll
    static void 내린다() {
        뒷단.disposeNow();
        if (선 != null) {
            선.close();
        }
    }

    @TestConfiguration
    static class StubbedBackend {

        @Bean
        @Primary
        Clock 고정_시계() {
            return Clock.fixed(지금, ZoneOffset.UTC);
        }

        /** 한산한 쿠폰. 통과 경로가 레디스를 치는지 재는 것이 목적이다. */
        @Bean
        @Primary
        SnapshotSource 한산한_재료() {
            Map<String, String> 재료 = SnapshotCodec.create().encode(
                    new GatewaySnapshot(Map.of(COUPON, CouponStates.idle(1_000_000)),
                            new SnapshotMeta(10_000, 1), 지금),
                    CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
            return () -> Mono.just(재료);
        }
    }

    @LocalServerPort
    private int port;

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(30))
                .build();
    }

    /**
     * 발급 한 번의 걸린 시간(ms). 판정 경로의 지연을 여기서 잰다.
     *
     * <p><b>여기서는 실시계를 쓴다.</b> 재려는 것이 시각이 아니라 경과이고,
     * 그것이 이 시나리오의 관측 대상이다. 판정이 보는 시계는 여전히 고정이다.
     */
    private long 발급_지연(int member) {
        long 시작 = System.nanoTime();
        클라이언트().post()
                .uri("/api/v1/coupons/" + COUPON + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(Void.class);
        return Duration.ofNanos(System.nanoTime() - 시작).toMillis();
    }

    /** 여러 번 재서 중앙값을 낸다. 한 번은 JIT 과 커넥션 수립에 흔들린다. */
    private long 중앙값_지연(int 시작_회원) {
        List<Long> 잰_것 = new ArrayList<>();
        for (int i = 0; i < 보낼_수; i++) {
            잰_것.add(발급_지연(시작_회원 + i));
        }
        잰_것.sort(Long::compare);
        return 잰_것.get(잰_것.size() / 2);
    }

    /**
     * <b>지연을 넣어도 판정이 안 느려진다</b> (불변식 1).
     *
     * <p>느려진다면 통과 경로 어딘가가 레디스를 치고 있다. 이 게이트웨이의
     * 존재 이유가 그 왕복을 없앤 것이라, 그때는 설계가 무너진 것이다.
     */
    @Test
    @DisplayName("C2_지연을_넣어도_판정이_안_느려진다")
    void C2_지연을_넣어도_판정이_안_느려진다() {
        long[] 정상 = new long[1];
        long[] 장애중 = new long[1];
        long[] 회복 = new long[1];

        ChaosScenario.named("C2 Redis 지연 %s".formatted(지연))
                .baseline(() -> 정상[0] = 중앙값_지연(1_000))
                .inject(() -> 지연을_넣는다(지연))
                .duringFault(() -> 장애중[0] = 중앙값_지연(2_000))
                .recover(this::지연을_걷는다)
                .afterRecovery(() -> 회복[0] = 중앙값_지연(3_000))
                .assertEntry(ChaosScenario.Verdict.none())
                .assertDuring(() -> RecoveryCriteria.violations(판정이_안_느려졌다(정상[0], 장애중[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(판정이_안_느려졌다(정상[0], 회복[0])))
                .run();

        assertThat(장애중[0]).as("전제 — 장애 구간을 실제로 쟀다").isNotNegative();
    }

    private void 지연을_넣는다(Duration 만큼) {
        try {
            선.느리게(만큼);
        } catch (IOException e) {
            throw new IllegalStateException("지연을 못 넣었다", e);
        }
    }

    private void 지연을_걷는다() {
        try {
            선.걷는다();
        } catch (IOException e) {
            throw new IllegalStateException("지연을 못 걷었다", e);
        }
    }

    /**
     * 판정 지연이 정상의 허용 배수를 안 넘는지 본다.
     *
     * <p>정상을 못 쟀으면 통과가 아니다 — 비교 대상이 없으면 이 기준은 아무것도
     * 안 재는 것이다.
     */
    private Optional<String> 판정이_안_느려졌다(long 정상, long 지금_지연) {
        if (정상 <= 0) {
            return Optional.of("정상 구간 지연을 못 쟀다 — 비교할 것이 없다");
        }
        double 배수 = (double) 지금_지연 / 정상;
        return 배수 <= 허용_배수 ? Optional.empty()
                : Optional.of("판정이 %.1f 배 느려졌다 (%dms → %dms) — 통과 경로가 레디스를 친다"
                        .formatted(배수, 정상, 지금_지연));
    }
}

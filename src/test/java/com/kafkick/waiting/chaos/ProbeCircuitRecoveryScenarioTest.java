package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

/**
 * C8c — 합성 프로브로 서킷이 회복 시도 두 번 안에 닫힌다 (G8.12, CY-834).
 *
 * <p>C8 은 스케줄러를 꺼 half-open 에 뒷단으로 가는 길이 없어 CLOSED 에 못 닿고, 세던 것도 회복 시도가 아니라
 * 열린 전이였다. 여기서는 프로브를 켜 half-open 진입을 회복 시도로 센다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(ProbeCircuitRecoveryScenarioTest.IdleCoupon.class)
class ProbeCircuitRecoveryScenarioTest {

    private static final Instant 지금 = Instant.parse("2026-09-14T00:00:00Z");
    private static final String COUPON = "c8c";

    /** 계획서 G8.12 — 회복 시도는 두 번을 안 넘는다. */
    private static final int 시도_한계 = 2;

    /** 뒷단을 안 살린 판에서 시도를 세는 창. 열린 대기(1초)와 프로브 한 회차가 여러 번 들어가야 한다. */
    private static final Duration 대조_창 = Duration.ofSeconds(8);

    private static final Duration 기다림 = Duration.ofSeconds(20);

    private static final AtomicBoolean 멎었다 = new AtomicBoolean();

    private static final BackendStub 뒷단 = BackendStub.멎을_수_있다(멎었다::get);

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("spring.data.redis.url", faults::주소);
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("waiting.backend.response-timeout", () -> Duration.ofMillis(300));
        registry.add("waiting.backend.connect-timeout", () -> Duration.ofMillis(250));
        registry.add("waiting.backend.circuit.minimum-number-of-calls", () -> 3);
        registry.add("waiting.backend.circuit.sliding-window-size", () -> "2s");
        registry.add("waiting.backend.circuit.wait-duration-in-open-state", () -> "1s");
        registry.add("waiting.backend.circuit.permitted-number-of-calls-in-half-open-state",
                () -> 2);
        registry.add("waiting.backend.probe.enabled", () -> true);
        registry.add("waiting.backend.probe.path", () -> "/health/load");
        registry.add("waiting.backend.probe.interval", () -> "200ms");
    }

    @AfterAll
    static void 내린다() {
        뒷단.close();
        if (faults != null) {
            faults.close();
        }
    }

    /** 한산한 쿠폰 재료. 스케줄러를 껐으니 안 심으면 요청이 뒷단에 안 닿아 서킷이 안 열린다. */
    @TestConfiguration
    static class IdleCoupon {

        @Bean
        @Primary
        Clock 고정_시계() {
            return Clock.fixed(지금, ZoneOffset.UTC);
        }

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

    @Autowired
    private CircuitBreakerRegistry circuits;

    /** half-open 진입 수. 회복 시도 한 번이 half-open 한 번이다. 컨텍스트가 공유돼 정적으로 둔다. */
    private static final AtomicInteger 반쯤_열린_수 = new AtomicInteger();

    /** 전이 구독은 한 번만. 시험마다 붙이면 계수가 겹쳐 두 배로 센다. */
    private static final AtomicBoolean 구독했다 = new AtomicBoolean();

    private final AtomicInteger 회원 = new AtomicInteger(60_000);

    private void 발급을_시도한다(int 횟수) {
        for (int i = 0; i < 횟수; i++) {
            WebTestClient.bindToServer().baseUrl("http://localhost:" + port)
                    .responseTimeout(Duration.ofSeconds(10)).build()
                    .post().uri("/api/v1/coupons/" + COUPON + "/issue")
                    .header("X-Member-Id", String.valueOf(회원.getAndIncrement()))
                    .header("X-Member-Grade", "GOLD")
                    .exchange();
        }
    }

    private CircuitBreaker 서킷() {
        return circuits.find("backend").orElseThrow(
                () -> new IllegalStateException("서킷이 없다 — 이름이 바뀌었는지 본다"));
    }

    /** 서킷을 만들고 전이를 세기 시작한 뒤, 뒷단을 멈춰 연다. */
    private void 연다() {
        발급을_시도한다(3);
        if (구독했다.compareAndSet(false, true)) {
            서킷().getEventPublisher().onStateTransition(e -> {
                if (e.getStateTransition().getToState() == CircuitBreaker.State.HALF_OPEN) {
                    반쯤_열린_수.incrementAndGet();
                }
            });
        }
        멎었다.set(true);
        Awaitility.await().atMost(기다림).until(() -> {
            발급을_시도한다(1);
            return 서킷().getState() != CircuitBreaker.State.CLOSED;
        });
    }

    @Test
    @DisplayName("C8c_뒷단이_살아나면_회복_시도_두_번_안에_닫힌다")
    void C8c_뒷단이_살아나면_회복_시도_두_번_안에_닫힌다() {
        int[] 살린_뒤_시도 = {-1};
        boolean[] 닫혔다 = new boolean[1];
        int[] 장애중_시도 = new int[1];

        ChaosScenario.named("C8c 프로브 회복")
                .baseline(() -> 발급을_시도한다(2))
                .inject(this::연다)
                .duringFault(() -> {
                    // **실패한 회복 시도를 한 번은 본다.** 프로브가 멎은 뒷단에서 실패해 다시 열려야
                    // half-open 이 두 번째로 온다. 첫 half-open 에 곧바로 살리면 실패 경로를 안 밟는다.
                    try {
                        Awaitility.await().atMost(기다림).until(() -> 반쯤_열린_수.get() >= 2);
                    } catch (ConditionTimeoutException e) {
                        // 판정이 이유를 남긴다.
                    }
                    장애중_시도[0] = 반쯤_열린_수.get();
                })
                .recover(() -> {
                    // 살리는 순간 이미 half-open 이면 그 회차가 첫 시도다.
                    반쯤_열린_수.set(서킷().getState() == CircuitBreaker.State.HALF_OPEN ? 1 : 0);
                    멎었다.set(false);
                })
                .afterRecovery(() -> {
                    try {
                        Awaitility.await().pollInterval(Duration.ofMillis(20)).atMost(기다림)
                                .until(() -> 서킷().getState() == CircuitBreaker.State.CLOSED);
                        닫혔다[0] = true;
                    } catch (ConditionTimeoutException e) {
                        닫혔다[0] = false;
                    }
                    살린_뒤_시도[0] = 반쯤_열린_수.get();
                })
                .assertEntry(ChaosScenario.Verdict.none())
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 전제 — 프로브가 half-open 을 만든다. 안 만들면 아래 계수가 늘 0 이다.
                        장애중_시도[0] >= 2 ? Optional.empty()
                                : Optional.of("전제 — 장애 중에 실패한 회복 시도가 없었다: half-open %d 번"
                                        .formatted(장애중_시도[0]))))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        닫혔다[0] ? Optional.empty()
                                : Optional.of("뒷단을 살렸는데 %s 안에 서킷이 안 닫혔다".formatted(기다림)),
                        // 닫히려면 half-open 을 한 번은 지난다. 0 이면 계수가 안 돈 것이다.
                        살린_뒤_시도[0] >= 1 && 살린_뒤_시도[0] <= 시도_한계 ? Optional.empty()
                                : Optional.of("G8.12 회복 시도가 %d 번이다 (한계 %d)"
                                        .formatted(살린_뒤_시도[0], 시도_한계)),
                        뒷단.중복_수신이_없다()))
                .run();
    }

    /**
     * <b>대조 판.</b> 뒷단을 끝내 안 살리면 같은 계수가 한계를 넘어야 한다. 넘지 않으면 위 판정은 시도를
     * 못 세는 채로 초록이다 — 옛 판정이 그 모양이었다.
     */
    @Test
    @DisplayName("C8c_뒷단이_안_살아나면_회복_시도가_한계를_넘어_쌓인다")
    void C8c_뒷단이_안_살아나면_회복_시도가_한계를_넘어_쌓인다() {
        연다();
        반쯤_열린_수.set(0);

        Awaitility.await().during(대조_창).atMost(대조_창.plusSeconds(2))
                .until(() -> 서킷().getState() != CircuitBreaker.State.CLOSED);

        int 쌓인_시도 = 반쯤_열린_수.get();
        멎었다.set(false);
        assertThat(쌓인_시도)
                .as("안 살아난 뒷단에서 %s 동안 half-open 진입", 대조_창)
                .isGreaterThan(시도_한계);
    }
}

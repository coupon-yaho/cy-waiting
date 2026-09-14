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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
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

    /** half-open 한 번에 프로브가 쓰는 허가 수. 살아난 뒤 이만큼씩 실패시키면 시도가 하나씩 는다. */
    private static final int 허가 = 2;

    private static final Duration 기다림 = Duration.ofSeconds(20);

    private static final AtomicBoolean 멎었다 = new AtomicBoolean();

    /** 살아난 뒤에도 실패시킬 프로브 수. 쿠폰 아닌 경로(프로브)만 센다. */
    private static final AtomicInteger 남은_프로브_실패 = new AtomicInteger();

    private static final BackendStub 뒷단 = BackendStub.멎거나_실패할_수_있다(멎었다::get,
            쿠폰 -> 쿠폰.isEmpty() && 남은_프로브_실패.getAndUpdate(n -> Math.max(0, n - 1)) > 0);

    /** half-open 에 든 시각(나노). 컨텍스트가 시험끼리 공유돼 정적으로 둔다. */
    private static final Queue<Long> 반쯤_열린_시각 = new ConcurrentLinkedQueue<>();

    /** 전이 구독은 한 번만. 시험마다 붙이면 같은 전이를 두 번 센다. */
    private static final AtomicBoolean 구독했다 = new AtomicBoolean();

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
                () -> 허가);
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

    private final AtomicInteger 회원 = new AtomicInteger(60_000);

    @BeforeEach
    void 뒷단을_되돌린다() {
        멎었다.set(false);
        남은_프로브_실패.set(0);
    }

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

    /**
     * 서킷을 만들고 전이를 세기 시작한 뒤, 뒷단을 멈춰 연다. <b>센 시작점을 돌려준다</b> — 시험 순서가 바뀌어
     * 앞 시험의 계수가 남아도 이 뒤의 전이만 센다.
     */
    private long 연다() {
        발급을_시도한다(3);
        if (구독했다.compareAndSet(false, true)) {
            서킷().getEventPublisher().onStateTransition(e -> {
                if (e.getStateTransition().getToState() == CircuitBreaker.State.HALF_OPEN) {
                    반쯤_열린_시각.add(System.nanoTime());
                }
            });
        }
        멎었다.set(true);
        Awaitility.await().atMost(기다림).until(() -> {
            발급을_시도한다(1);
            return 서킷().getState() != CircuitBreaker.State.CLOSED;
        });
        return System.nanoTime();
    }

    private static int 시도_수(long 부터) {
        return (int) 반쯤_열린_시각.stream().filter(시각 -> 시각 - 부터 >= 0).count();
    }

    /** 결과 하나. 닫혔는지와 살린 뒤 회복 시도 수. */
    private record Recovery(boolean 닫혔다, int 시도) {
    }

    /**
     * 뒷단을 살리고 닫히기를 기다린다. 살리는 순간 이미 half-open 이면 그 회차를 첫 시도로 센다. 시각을 먼저
     * 찍고 상태를 읽어, 그 사이 전이가 끼면 두 번 세는 쪽으로 틀린다 — 판정이 빨개지는 방향이다.
     */
    private Recovery 살리고_닫히기를_기다린다(int 살린_뒤_실패시킬_프로브) {
        long 살린_시각 = System.nanoTime();
        int 이미_반쯤 = 서킷().getState() == CircuitBreaker.State.HALF_OPEN ? 1 : 0;
        남은_프로브_실패.set(살린_뒤_실패시킬_프로브);
        멎었다.set(false);
        boolean 닫혔다;
        try {
            Awaitility.await().pollInterval(Duration.ofMillis(20)).atMost(기다림)
                    .until(() -> 서킷().getState() == CircuitBreaker.State.CLOSED);
            닫혔다 = true;
        } catch (ConditionTimeoutException e) {
            닫혔다 = false;
        }
        return new Recovery(닫혔다, 시도_수(살린_시각) + 이미_반쯤);
    }

    @Test
    @DisplayName("C8c_뒷단이_살아나면_회복_시도_두_번_안에_닫힌다")
    void C8c_뒷단이_살아나면_회복_시도_두_번_안에_닫힌다() {
        long[] 센_시작 = new long[1];
        int[] 장애중_시도 = new int[1];
        Recovery[] 회복 = new Recovery[1];

        ChaosScenario.named("C8c 프로브 회복")
                .baseline(() -> 발급을_시도한다(2))
                .inject(() -> 센_시작[0] = 연다())
                .duringFault(() -> {
                    // **실패한 회복 시도를 한 번은 본다.** 프로브가 멎은 뒷단에서 실패해 다시 열려야
                    // half-open 이 두 번째로 온다. 첫 half-open 에 곧바로 살리면 실패 경로를 안 밟는다.
                    try {
                        Awaitility.await().atMost(기다림).until(() -> 시도_수(센_시작[0]) >= 2);
                    } catch (ConditionTimeoutException e) {
                        // 판정이 이유를 남긴다.
                    }
                    장애중_시도[0] = 시도_수(센_시작[0]);
                })
                .recover(() -> {
                })
                .afterRecovery(() -> 회복[0] = 살리고_닫히기를_기다린다(0))
                .assertEntry(ChaosScenario.Verdict.none())
                .assertDuring(() -> RecoveryCriteria.violations(
                        장애중_시도[0] >= 2 ? Optional.empty()
                                : Optional.of("전제 — 장애 중에 실패한 회복 시도가 없었다: half-open %d 번"
                                        .formatted(장애중_시도[0]))))
                .assertRecovery(() -> RecoveryCriteria.violations(회복을_판정한다(회복[0])))
                .run();
    }

    private Optional<String> 회복을_판정한다(Recovery 회복) {
        if (회복 == null || !회복.닫혔다()) {
            return Optional.of("뒷단을 살렸는데 %s 안에 서킷이 안 닫혔다".formatted(기다림));
        }
        // 닫히려면 half-open 을 한 번은 지난다. 0 이면 계수가 안 돈 것이다.
        return 회복.시도() >= 1 && 회복.시도() <= 시도_한계 ? Optional.empty()
                : Optional.of("G8.12 회복 시도가 %d 번이다 (한계 %d)".formatted(회복.시도(), 시도_한계));
    }

    /**
     * <b>대조 판 하나 — 계수가 센다.</b> 뒷단을 끝내 안 살리면 같은 계수가 한계를 넘어 쌓여야 한다. 안 쌓이면
     * 위 판정은 시도를 못 세는 채로 초록이다 — 옛 판정이 그 모양이었다.
     */
    @Test
    @DisplayName("C8c_뒷단이_안_살아나면_회복_시도가_한계를_넘어_쌓인다")
    void C8c_뒷단이_안_살아나면_회복_시도가_한계를_넘어_쌓인다() {
        long 센_시작 = 연다();

        Awaitility.await().during(대조_창).atMost(대조_창.plusSeconds(2))
                .until(() -> 서킷().getState() != CircuitBreaker.State.CLOSED);

        assertThat(시도_수(센_시작)).as("안 살아난 뒷단에서 %s 동안 half-open 진입", 대조_창)
                .isGreaterThan(시도_한계);
    }

    /**
     * <b>대조 판 둘 — 한계가 빨개진다.</b> 살아난 뒤에도 프로브가 두 회차를 실패하면 결국 닫히되 시도가 셋이다.
     * 판정기가 이 판을 위반으로 읽어야 "두 번 이하" 가 늘 참인 조건이 아니다.
     */
    @Test
    @DisplayName("C8c_살아난_뒤에도_프로브가_거듭_실패하면_한계_판정이_빨개진다")
    void C8c_살아난_뒤에도_프로브가_거듭_실패하면_한계_판정이_빨개진다() {
        연다();
        Awaitility.await().atMost(기다림)
                .until(() -> 서킷().getState() == CircuitBreaker.State.OPEN);

        Recovery 회복 = 살리고_닫히기를_기다린다(허가 * 시도_한계);

        assertThat(회복.닫혔다()).as("결국 닫힌다").isTrue();
        assertThat(회복을_판정한다(회복)).as("한계를 넘은 회복을 위반으로 읽는다")
                .hasValueSatisfying(위반 -> assertThat(위반).contains("G8.12"));
    }
}

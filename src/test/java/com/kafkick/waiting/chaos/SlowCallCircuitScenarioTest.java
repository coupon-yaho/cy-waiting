package com.kafkick.waiting.chaos;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

/**
 * C8b — 뒷단이 느리지만 답한다 → 느린 호출로 서킷 오픈 (CY-833).
 *
 * <p>C8 은 응답 상한이 느림 임계보다 짧아 전부 타임아웃으로 열린다. 여기서는 상한 안에 늦게
 * 답하게 해 <b>느린 호출 비율만으로</b> 열리는지 잰다.
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(SlowCallCircuitScenarioTest.IdleCoupon.class)
class SlowCallCircuitScenarioTest {

    private static final Instant 지금 = Instant.parse("2026-09-14T00:00:00Z");
    private static final String COUPON = "c8b";

    /** 느림 임계. 지연은 이것보다 넉넉히 길고 응답 상한보다 넉넉히 짧아야 한다. */
    private static final Duration 느림_임계 = Duration.ofMillis(200);
    private static final Duration 지연 = Duration.ofMillis(500);
    private static final Duration 응답_상한 = Duration.ofSeconds(2);

    /** 표본 하한. 빠른 것과 느린 것을 반씩 채우면 비율이 정확히 문턱이다. */
    private static final int 표본_하한 = 4;

    /** 뒷단이 느린가. 켜진 동안 짝수 회원만 늦게 답한다. */
    private static final AtomicBoolean 느리다 = new AtomicBoolean();

    private static final BackendStub 뒷단 = BackendStub.늦게_답한다(
            회원 -> 느리다.get() && Long.parseLong(회원) % 2 == 0, 지연);

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("spring.data.redis.url", faults::주소);
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("waiting.backend.response-timeout", () -> 응답_상한);
        registry.add("waiting.backend.circuit.minimum-number-of-calls", () -> 표본_하한);
        registry.add("waiting.backend.circuit.sliding-window-size", () -> "10s");
        registry.add("waiting.backend.circuit.slow-call-duration-threshold", () -> 느림_임계);
        registry.add("waiting.backend.circuit.wait-duration-in-open-state", () -> "5s");
    }

    @AfterAll
    static void 내린다() {
        뒷단.close();
        if (faults != null) {
            faults.close();
        }
    }

    @TestConfiguration
    static class IdleCoupon {

        @Bean
        @Primary
        Clock 고정_시계() {
            return Clock.fixed(지금, ZoneOffset.UTC);
        }

        /** 한산한 쿠폰. 요청이 뒷단까지 가야 서킷이 표본을 얻는다. */
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

    /** 회원 번호. 요청마다 새로 뽑는다 — 고정 시계라 같은 회원은 초당 상한에 걸린다. */
    private final AtomicInteger 회원 = new AtomicInteger(80_000);

    private final AtomicInteger 느림_초과 = new AtomicInteger();
    private final AtomicInteger 실패_초과 = new AtomicInteger();

    private int 발급을_시도한다() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build()
                .post()
                .uri("/api/v1/coupons/" + COUPON + "/issue")
                .header("X-Member-Id", String.valueOf(회원.getAndIncrement()))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }

    private List<Integer> 여러_번_시도한다(int 횟수) {
        List<Integer> 상태 = new ArrayList<>();
        for (int i = 0; i < 횟수; i++) {
            상태.add(발급을_시도한다());
        }
        return 상태;
    }

    private CircuitBreaker 서킷() {
        return circuits.find("backend").orElseThrow(
                () -> new IllegalStateException("서킷이 없다 — 이름이 바뀌었는지 본다"));
    }

    @Test
    @DisplayName("C8b_느린_호출_비율만으로_서킷이_열린다")
    void C8b_느린_호출_비율만으로_서킷이_열린다() {
        List<Integer> 정상_상태 = new ArrayList<>();
        List<Integer> 여는_상태 = new ArrayList<>();
        List<Integer> 유지_상태 = new ArrayList<>();
        long[] 유지중_유입 = new long[1];
        int[] 열린_때_실패 = {-1};
        long[] 느린_회원_응답 = {-1};

        ChaosScenario.named("C8b 뒷단 느림 → 서킷 오픈")
                .baseline(() -> {
                    정상_상태.addAll(여러_번_시도한다(2));
                    서킷().getEventPublisher()
                            .onSlowCallRateExceeded(e -> 느림_초과.incrementAndGet())
                            .onFailureRateExceeded(e -> 실패_초과.incrementAndGet());
                    // **창을 비운다.** 정상 구간의 빠른 호출이 남으면 비율이 문턱 아래로 묽어져
                    // 몇 건째에 열렸는지로 문턱을 못 잰다.
                    서킷().reset();
                })
                .inject(() -> 느리다.set(true))
                .duringFault(() -> {
                    // 회원 번호가 짝수부터라 느린 것과 빠른 것이 번갈아 간다.
                    회원.set(회원.get() + 회원.get() % 2);
                    여는_상태.addAll(여러_번_시도한다(표본_하한));
                    // 열린 순간 바로 잰다. 대기가 끝나 half-open 으로 가면 지표가 새로 시작한다.
                    Awaitility.await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(10))
                            .atMost(Duration.ofSeconds(2))
                            .until(() -> 서킷().getState() == CircuitBreaker.State.OPEN);
                    열린_때_실패[0] = 서킷().getMetrics().getNumberOfFailedCalls();
                    long 열린_뒤 = 뒷단.받은_수();
                    유지_상태.addAll(여러_번_시도한다(3));
                    유지중_유입[0] = 뒷단.받은_수() - 열린_뒤;
                })
                .recover(() -> 느리다.set(false))
                .afterRecovery(() -> {
                    Awaitility.await().atMost(Duration.ofSeconds(10))
                            .until(() -> 서킷().getState() != CircuitBreaker.State.OPEN);
                    느린_회원_응답[0] = 뒷단에_직접_묻는다();
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        다_통과했다("정상", 정상_상태)))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // **실패 없이 열렸는가.** 상한 안에 답했으니 5xx 도 타임아웃도 없어야
                        // 한다. 섞이면 느린 호출 배선이 죽어도 열린다.
                        다_통과했다("여는 구간", 여는_상태),
                        느린_호출로_열렸다(열린_때_실패[0]),
                        유입이_멎었다(유지중_유입[0]),
                        줄에_세웠다(유지_상태)))
                // **CLOSED 까지는 여기서 못 잰다** (CY-813). half-open 이면 판정이 전원을 줄에
                // 세우는데 스케줄러를 껐다. 대기가 지나 열린 채로 안 굳는지까지만 본다 —
                // 이것은 뒷단이 느린 채여도 참이라, 느림을 걷었는지는 하네스 확인으로 따로 본다.
                .assertRecovery(() -> RecoveryCriteria.violations(
                        열린_채로_안_굳었다(),
                        느림이_걷혔다(느린_회원_응답[0]),
                        뒷단.중복_수신이_없다()))
                .run();
    }

    private Optional<String> 다_통과했다(String 구간, List<Integer> 상태) {
        return !상태.isEmpty() && 상태.stream().allMatch(s -> s == 200) ? Optional.empty()
                : Optional.of("%s — 전부 200 이어야 한다: %s".formatted(구간, 상태));
    }

    /** 느린 호출 비율이 문턱을 넘어 열렸고, 실패율로 연 것이 아니다. */
    private Optional<String> 느린_호출로_열렸다(int 열린_때_실패) {
        return 느림_초과.get() == 1 && 실패_초과.get() == 0 && 열린_때_실패 == 0
                ? Optional.empty()
                : Optional.of("느린 호출로 안 열렸다 — 느림 초과 %d, 실패 초과 %d, 열린 때 실패 %d"
                        .formatted(느림_초과.get(), 실패_초과.get(), 열린_때_실패));
    }

    /** 하네스 확인이다. 느리던 짝수 회원으로 스텁을 직접 불러, 늦게 답하는 갈래를 탄 수를 센다. */
    private long 뒷단에_직접_묻는다() {
        long 전 = 뒷단.늦게_답한_수();
        WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + 뒷단.port())
                .responseTimeout(Duration.ofSeconds(2))
                .build()
                .get().uri("/probe").header("X-Member-Id", "2")
                .exchange().expectStatus().isOk();
        return 뒷단.늦게_답한_수() - 전;
    }

    private Optional<String> 느림이_걷혔다(long 늦게_답한_증가) {
        return 늦게_답한_증가 == 0 ? Optional.empty()
                : Optional.of("하네스 — 느림을 걷었는데 스텁이 늦게 답하는 갈래를 탔다");
    }

    private Optional<String> 유입이_멎었다(long 유입) {
        return 유입 == 0 ? Optional.empty()
                : Optional.of("서킷이 열렸는데 뒷단이 %d 건을 더 받았다".formatted(유입));
    }

    /** 202 만 줄에 선 것이다. 5xx 만 거르면 폴백의 503 도 통과한다. */
    private Optional<String> 줄에_세웠다(List<Integer> 상태) {
        return 상태.stream().allMatch(s -> s == 202) ? Optional.empty()
                : Optional.of("유지 — 줄에 못 선 것이 있다: %s".formatted(상태));
    }

    private Optional<String> 열린_채로_안_굳었다() {
        return 서킷().getState() != CircuitBreaker.State.OPEN ? Optional.empty()
                : Optional.of("대기 시간이 지났는데 서킷이 열린 채로 굳었다");
    }
}

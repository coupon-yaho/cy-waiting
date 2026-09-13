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
import java.util.function.Supplier;
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
 * C9b — 뒷단 5xx → 서킷 오픈 → half-open 회복 (CY-926).
 *
 * <p>5xx 가 서킷 실패로 세어진 뒤(CY-853) 그 서킷이 <b>열린 채로 안 굳는지</b>를 C8 과 같은
 * 판정으로 잰다. C8 은 응답이 아예 안 오고, 여기는 오긴 오는데 500 이다. 실시간으로 도는
 * 통합 확인이고, 전이 시점은 {@code BackendCircuitTransitionTest} 가 시계를 쥐고 잰다.
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(ServerErrorCircuitRecoveryScenarioTest.IdleCoupon.class)
class ServerErrorCircuitRecoveryScenarioTest {

    private static final Instant 지금 = Instant.parse("2026-09-14T00:00:00Z");
    private static final String COUPON = "c9b";

    /** 뒷단이 5xx 를 내는가. 이 스위치로 장애를 넣고 걷는다. */
    private static final AtomicBoolean 실패한다 = new AtomicBoolean();

    private static final BackendStub 뒷단 = BackendStub.실패할_수_있다(실패한다::get);

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        // **레디스를 띄운다.** 안 띄우면 줄 등록이 안 돼 전량이 fail-open 으로 새고,
        // 뒷단 유입 0 이 "줄로 보냈다" 와 "라우트가 끊었다" 를 못 가른다.
        faults = RedisFaults.시작한다();
        registry.add("spring.data.redis.url", faults::주소);
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        // 표본 하한을 낮춘다. 운영값 20 건을 여기서 채우면 몇 초가 걸린다.
        registry.add("waiting.backend.circuit.minimum-number-of-calls", () -> 3);
        registry.add("waiting.backend.circuit.sliding-window-size", () -> "2s");
        registry.add("waiting.backend.circuit.wait-duration-in-open-state", () -> "1s");
        registry.add("waiting.backend.circuit.permitted-number-of-calls-in-half-open-state",
                () -> 2);
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

    /** 열린 횟수. */
    private final AtomicInteger 열린_횟수 = new AtomicInteger();

    /** 회복을 기다리는 동안 볼 시계. 판정이 보는 시계는 고정이라 여기 못 쓴다. */
    private final Supplier<Instant> 벽시계 = Instant::now;

    /** 회복을 포기하는 한계. 서킷의 대기 시간보다 넉넉해야 한다. */
    private static final Duration 회복_한계 = Duration.ofSeconds(20);

    /** 회원 번호. 요청마다 새로 뽑는다 — 고정 시계라 같은 회원은 초당 상한에 걸린다. */
    private final AtomicInteger 회원 = new AtomicInteger(90_000);

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build();
    }

    private int 발급을_시도한다() {
        return 클라이언트().post()
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
    @DisplayName("C9b_5xx_로_연_서킷이_열린_채로_안_굳는다")
    void C9b_5xx_로_연_서킷이_열린_채로_안_굳는다() {
        long[] 유지중_유입 = new long[1];
        List<Integer> 정상_상태 = new ArrayList<>();
        long[] 정상_유입 = new long[1];
        List<Integer> 열기_전_상태 = new ArrayList<>();
        long[] 열기_전_유입 = new long[1];
        List<Integer> 장애중_상태 = new ArrayList<>();
        long[] 회복_유입 = new long[1];

        ChaosScenario.named("C9b 뒷단 5xx → 서킷 오픈")
                .baseline(() -> {
                    // **서킷은 첫 요청이 만든다.** 그 전에 잡으려 하면 없다.
                    정상_상태.addAll(여러_번_시도한다(3));
                    정상_유입[0] = 뒷단.받은_수();
                    서킷().getEventPublisher().onStateTransition(e -> {
                        if (e.getStateTransition().getToState() == CircuitBreaker.State.OPEN) {
                            열린_횟수.incrementAndGet();
                        }
                    });
                })
                .inject(() -> 실패한다.set(true))
                .duringFault(() -> {
                    long 전 = 뒷단.받은_수();
                    열기_전_상태.addAll(여러_번_시도한다(5));
                    열기_전_유입[0] = 뒷단.받은_수() - 전;
                    // **열린 뒤부터 잰다.** 창이 시간 단위라 호출이 느리면 다섯 건으로 표본이 안
                    // 차고, 그때 유지 구간의 요청이 뒷단에 가 판정이 빨개진다.
                    Awaitility.await().atMost(Duration.ofSeconds(10))
                            .until(() -> {
                                여러_번_시도한다(1);
                                return 서킷().getState() == CircuitBreaker.State.OPEN;
                            });
                    long 열린_뒤 = 뒷단.받은_수();
                    장애중_상태.addAll(여러_번_시도한다(5));
                    유지중_유입[0] = 뒷단.받은_수() - 열린_뒤;
                })
                .recover(() -> 실패한다.set(false))
                .afterRecovery(() -> {
                    닫힐_때까지_두드린다();
                    회복_유입[0] = 뒷단이_직접_답한다() ? 1 : 0;
                })
                // **뒷단 배선이 살아 있는가.** 연결이 안 되는 배선에서도 서킷은 열려 아래
                // 판정이 다 초록이 된다 — 그때는 5xx 로 연 것이 아니다.
                .assertEntry(() -> RecoveryCriteria.violations(
                        다_통과했다(정상_상태, 정상_유입[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        오백이_뒷단에서_왔다(열기_전_상태, 열기_전_유입[0]),
                        서킷이_열렸다(),
                        유입이_멎었다(유지중_유입[0]),
                        // **막는 것과 줄에 세우는 것은 다르다.** 뒷단 유입 0 은 둘 다에서
                        // 나온다. 5xx 가 폴백으로 바뀌어 503 이 되는 것도 여기서 갈린다.
                        줄에_세웠다(장애중_상태)))
                // **닫히는 것까지는 여기서 못 잰다** (CY-813). HALF_OPEN 이면 판정이 전원을
                // 줄에 세우므로 뒷단으로 가는 유일한 길이 배분이 준 차례인데 스케줄러를 껐다.
                // C8 과 같은 한계라, 대기 시간이 지나 시도에 들어갔는지까지 잰다.
                .assertRecovery(() -> RecoveryCriteria.violations(
                        시도에_들어갔다(),
                        // 하네스 확인이다. 스텁을 직접 찔러 스위치가 걷혔는지만 본다.
                        스위치가_걷혔다(회복_유입[0]),
                        뒷단.중복_수신이_없다()))
                .run();
    }

    /** 열린 상태를 벗어날 때까지 눌러 본다. 횟수가 아니라 시각으로 끊는다. */
    private void 닫힐_때까지_두드린다() {
        Instant 시작 = 벽시계.get();
        while (서킷().getState() == CircuitBreaker.State.OPEN
                && Duration.between(시작, 벽시계.get()).compareTo(회복_한계) < 0) {
            여러_번_시도한다(2);
        }
    }

    private Optional<String> 서킷이_열렸다() {
        CircuitBreaker.State 지금_상태 = 서킷().getState();
        return 지금_상태 == CircuitBreaker.State.OPEN || 열린_횟수.get() > 0
                ? Optional.empty()
                : Optional.of("장애 구간인데 서킷이 안 열렸다 — 상태 %s".formatted(지금_상태));
    }

    /** 서킷이 열렸으면 뒷단으로 가는 것이 거의 없어야 한다. */
    private Optional<String> 유입이_멎었다(long 유입) {
        return 유입 > 1
                ? Optional.of("서킷이 열렸는데 뒷단이 %d 건을 더 받았다".formatted(유입))
                : Optional.empty();
    }

    /** 대기 시간이 지나 회복을 시도하는 상태로 들어갔는가. */
    private Optional<String> 시도에_들어갔다() {
        return 서킷().getState() == CircuitBreaker.State.OPEN
                ? Optional.of("대기 시간이 지났는데 서킷이 열린 채로 굳었다")
                : Optional.empty();
    }

    /** 202 만 줄에 선 것이다. 5xx 만 거르면 429·409 도 "큐 등록 정상" 을 만족시킨다. */
    private Optional<String> 줄에_세웠다(List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("유지 — 보낸 것이 없다");
        }
        long 못_선_것 = 상태.stream().filter(status -> status != 202).count();
        return 못_선_것 == 0 ? Optional.empty()
                : Optional.of("유지 — %d 건이 줄에 못 섰다 (보낸 %d): %s"
                        .formatted(못_선_것, 상태.size(), 상태));
    }

    /** 뒷단을 직접 찔러 살아났는지 본다. 서킷 상태는 뒷단이 죽은 채로도 바뀐다. */
    private boolean 뒷단이_직접_답한다() {
        try {
            return WebTestClient.bindToServer()
                    .baseUrl("http://localhost:" + 뒷단.port())
                    .responseTimeout(Duration.ofSeconds(2))
                    .build()
                    .get().uri("/probe").exchange()
                    .returnResult(Void.class).getStatus().is2xxSuccessful();
        } catch (RuntimeException e) {
            return false;
        }
    }

    private Optional<String> 스위치가_걷혔다(long 회복_유입) {
        return 회복_유입 > 0 ? Optional.empty()
                : Optional.of("하네스 — 스위치를 걷었는데 스텁이 직접 물어도 200 이 아니다");
    }

    private Optional<String> 다_통과했다(List<Integer> 상태, long 유입) {
        return 상태.size() == 3 && 상태.stream().allMatch(s -> s == 200) && 유입 == 3
                ? Optional.empty()
                : Optional.of("전제 — 정상 구간이 뒷단까지 다 안 갔다: 상태 %s, 뒷단 수신 %d"
                        .formatted(상태, 유입));
    }

    /** 서킷을 연 것이 뒷단의 5xx 인가. 뒷단이 받았고 사용자가 5xx 계열을 받았어야 한다. */
    private Optional<String> 오백이_뒷단에서_왔다(List<Integer> 상태, long 유입) {
        return 유입 > 0 && 상태.stream().anyMatch(s -> s >= 500) ? Optional.empty()
                : Optional.of("전제 — 5xx 가 뒷단에서 안 왔다: 상태 %s, 뒷단 수신 %d"
                        .formatted(상태, 유입));
    }
}

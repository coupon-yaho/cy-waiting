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
 * C9c — 쿠폰 하나만 5xx 를 낼 때 서킷이 막는 범위 (CY-926).
 *
 * <p><b>서킷은 뒷단 전체 하나다.</b> 쿠폰 하나의 5xx 로 열리면 무관한 한산한 쿠폰까지 줄에 선다.
 * 받아들인 범위라 사실로 적는다 — 범위를 좁히는 날(인스턴스·쿠폰별 서킷) 이 판정이 뒤집힌다.
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(ServerErrorScopeScenarioTest.TwoIdleCoupons.class)
class ServerErrorScopeScenarioTest {

    private static final Instant 지금 = Instant.parse("2026-09-14T00:00:00Z");

    /** 5xx 를 내는 쿠폰. */
    private static final String 아픈_쿠폰 = "c9c-sick";

    /** 멀쩡한 한산한 쿠폰. 뒷단은 이 쿠폰에 늘 200 을 낸다. */
    private static final String 멀쩡한_쿠폰 = "c9c-well";

    private static final AtomicBoolean 실패한다 = new AtomicBoolean();

    private static final BackendStub 뒷단 =
            BackendStub.쿠폰만_실패한다(couponId -> 실패한다.get() && 아픈_쿠폰.equals(couponId));

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        // 레디스가 없으면 줄 등록이 안 돼 fail-open 으로 새고, "줄에 섰다" 를 못 잰다.
        faults = RedisFaults.시작한다();
        registry.add("spring.data.redis.url", faults::주소);
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
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
    static class TwoIdleCoupons {

        @Bean
        @Primary
        Clock 고정_시계() {
            return Clock.fixed(지금, ZoneOffset.UTC);
        }

        /** 둘 다 한산하다. 줄에 설 이유가 없는 쿠폰이라야 서킷 탓인지가 갈린다. */
        @Bean
        @Primary
        SnapshotSource 한산한_재료() {
            Map<String, String> 재료 = SnapshotCodec.create().encode(
                    new GatewaySnapshot(Map.of(
                            아픈_쿠폰, CouponStates.idle(1_000_000),
                            멀쩡한_쿠폰, CouponStates.idle(1_000_000)),
                            new SnapshotMeta(10_000, 1), 지금),
                    CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
            return () -> Mono.just(재료);
        }
    }

    @LocalServerPort
    private int port;

    @Autowired
    private CircuitBreakerRegistry circuits;

    /** 회원 번호. 고정 시계라 같은 회원은 초당 상한에 걸려 요청마다 새로 뽑는다. */
    private final AtomicInteger 회원 = new AtomicInteger(95_000);

    private int 발급을_시도한다(String couponId) {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build()
                .post()
                .uri("/api/v1/coupons/" + couponId + "/issue")
                .header("X-Member-Id", String.valueOf(회원.getAndIncrement()))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }

    private List<Integer> 여러_번_시도한다(String couponId, int 횟수) {
        List<Integer> 상태 = new ArrayList<>();
        for (int i = 0; i < 횟수; i++) {
            상태.add(발급을_시도한다(couponId));
        }
        return 상태;
    }

    private CircuitBreaker 서킷() {
        return circuits.find("backend").orElseThrow(
                () -> new IllegalStateException("서킷이 없다 — 이름이 바뀌었는지 본다"));
    }

    @Test
    @DisplayName("C9c_쿠폰_하나의_5xx_가_멀쩡한_쿠폰까지_줄에_세운다")
    void C9c_쿠폰_하나의_5xx_가_멀쩡한_쿠폰까지_줄에_세운다() {
        List<Integer> 정상_상태 = new ArrayList<>();
        List<Integer> 멀쩡한_쿠폰_상태 = new ArrayList<>();
        long[] 멀쩡한_쿠폰_유입 = new long[1];
        CircuitBreaker.State[] 열린_뒤_상태 = new CircuitBreaker.State[1];
        CircuitBreaker.State[] 회복_뒤_상태 = new CircuitBreaker.State[1];

        ChaosScenario.named("C9c 쿠폰 하나의 5xx")
                .baseline(() -> 정상_상태.addAll(여러_번_시도한다(멀쩡한_쿠폰, 3)))
                .inject(() -> 실패한다.set(true))
                .duringFault(() -> {
                    Awaitility.await().atMost(Duration.ofSeconds(10))
                            .until(() -> {
                                여러_번_시도한다(아픈_쿠폰, 1);
                                return 서킷().getState() == CircuitBreaker.State.OPEN;
                            });
                    열린_뒤_상태[0] = 서킷().getState();
                    long 전 = 뒷단.받은_수(멀쩡한_쿠폰);
                    멀쩡한_쿠폰_상태.addAll(여러_번_시도한다(멀쩡한_쿠폰, 5));
                    멀쩡한_쿠폰_유입[0] = 뒷단.받은_수(멀쩡한_쿠폰) - 전;
                })
                .recover(() -> 실패한다.set(false))
                .afterRecovery(() -> {
                    Awaitility.await().atMost(Duration.ofSeconds(20))
                            .until(() -> 서킷().getState() != CircuitBreaker.State.OPEN);
                    회복_뒤_상태[0] = 서킷().getState();
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        다_통과했다("정상", 정상_상태)))
                .assertDuring(() -> RecoveryCriteria.violations(
                        열렸다(열린_뒤_상태[0]),
                        // **넓어진 범위를 사실로 적는다.** 멀쩡한 쿠폰은 뒷단이 200 을 낼 것인데도
                        // 서킷이 전체 하나라 판정이 줄에 세운다. 이 값이 뒤집히는 날이 범위를
                        // 좁힌 날이다.
                        멀쩡한_쿠폰도_줄에_섰다(멀쩡한_쿠폰_상태),
                        멀쩡한_쿠폰이_뒷단에_안_갔다(멀쩡한_쿠폰_유입[0])))
                // 닫히는 것까지는 못 잰다 — HALF_OPEN 의 유일한 길이 배분인데 스케줄러를 껐다
                // (CY-813). 열린 채로 안 굳는지까지 본다.
                .assertRecovery(() -> RecoveryCriteria.violations(
                        안_굳었다(회복_뒤_상태[0]),
                        뒷단.중복_수신이_없다()))
                .run();
    }

    private Optional<String> 다_통과했다(String 구간, List<Integer> 상태) {
        return !상태.isEmpty() && 상태.stream().allMatch(s -> s == 200) ? Optional.empty()
                : Optional.of("전제 — %s 구간에 멀쩡한 쿠폰이 다 통과하지 않았다: %s".formatted(구간, 상태));
    }

    private Optional<String> 열렸다(CircuitBreaker.State 상태) {
        return 상태 == CircuitBreaker.State.OPEN ? Optional.empty()
                : Optional.of("전제 — 아픈 쿠폰의 5xx 로 서킷이 안 열렸다: %s".formatted(상태));
    }

    private Optional<String> 멀쩡한_쿠폰도_줄에_섰다(List<Integer> 상태) {
        return !상태.isEmpty() && 상태.stream().allMatch(s -> s == 202) ? Optional.empty()
                : Optional.of("유지 — 멀쩡한 쿠폰이 줄에 안 섰다: %s. 서킷 범위가 바뀌었는지 보고 "
                        + "이 사실 판정과 계획서를 같이 고친다".formatted(상태));
    }

    private Optional<String> 멀쩡한_쿠폰이_뒷단에_안_갔다(long 유입) {
        return 유입 == 0 ? Optional.empty()
                : Optional.of("유지 — 서킷이 열렸는데 멀쩡한 쿠폰이 뒷단에 %d 건 갔다".formatted(유입));
    }

    private Optional<String> 안_굳었다(CircuitBreaker.State 상태) {
        return 상태 != CircuitBreaker.State.OPEN ? Optional.empty()
                : Optional.of("회복 — 대기 시간이 지났는데 서킷이 열린 채로 굳었다");
    }
}

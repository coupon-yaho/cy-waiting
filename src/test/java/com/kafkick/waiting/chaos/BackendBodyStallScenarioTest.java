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
import io.micrometer.core.instrument.MeterRegistry;
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
import org.springframework.test.web.reactive.server.FluxExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

/**
 * C18-b — 헤더 200 은 오고 본문이 안 끝난다 (CY-714).
 *
 * <p>본문 상한이 커넥션을 끊어 매달리지는 않는다. 다만 <b>사용자는 잘린 200 을 받고 서킷은 성공으로
 * 센다</b> — 열린 결함이다 (CY-710·CY-713). 지금 동작을 못 박아, 고치는 날 여기가 뒤집힌다.
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(BackendBodyStallScenarioTest.IdleCoupon.class)
class BackendBodyStallScenarioTest {

    private static final Instant 지금 = Instant.parse("2026-09-14T00:00:00Z");
    private static final String COUPON = "c18b";

    /** 응답 상한. 본문 상한은 이것의 두 배로 배선된다. */
    private static final Duration 응답_상한 = Duration.ofMillis(500);
    private static final Duration 본문_상한 = 응답_상한.multipliedBy(2);

    /** 끊긴 뒤 클라이언트에 닿기까지의 여유. 상한 배선이 달라지면 이 폭 밖으로 나가야 한다. */
    private static final Duration 여유 = Duration.ofMillis(400);

    private static final int 보낼_수 = 3;

    private static final AtomicBoolean 본문이_멎었다 = new AtomicBoolean();

    private static final BackendStub 뒷단 = BackendStub.본문을_안_끝낼_수_있다(본문이_멎었다::get);

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("spring.data.redis.url", faults::주소);
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("waiting.backend.response-timeout", () -> 응답_상한);
        registry.add("waiting.backend.connect-timeout", () -> Duration.ofMillis(250));
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

        /** 한산한 쿠폰. 요청이 뒷단까지 가야 본문이 멎는 갈래를 밟는다. */
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

    /** 한 요청의 끝. 상태, 본문이 잘렸는지, 끝나기까지 걸린 시간. */
    private record Outcome(int status, boolean 잘림, Duration 걸림) {
    }

    @LocalServerPort
    private int port;

    @Autowired
    private CircuitBreakerRegistry circuits;

    @Autowired
    private MeterRegistry meters;

    /** 회원 번호. 요청마다 새로 뽑는다 — 고정 시계라 같은 회원은 초당 상한에 걸린다. */
    private final AtomicInteger 회원 = new AtomicInteger(70_000);

    private Outcome 발급을_시도한다() {
        long 시작 = System.nanoTime();
        FluxExchangeResult<String> 응답 = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build()
                .post()
                .uri("/api/v1/coupons/" + COUPON + "/issue")
                .header("X-Member-Id", String.valueOf(회원.getAndIncrement()))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(String.class);
        boolean 잘림;
        try {
            응답.getResponseBody().then().block(Duration.ofSeconds(10));
            잘림 = false;
        } catch (RuntimeException e) {
            잘림 = true;
        }
        return new Outcome(응답.getStatus().value(), 잘림, Duration.ofNanos(System.nanoTime() - 시작));
    }

    private List<Outcome> 여러_번_시도한다(int 횟수) {
        List<Outcome> 결과들 = new ArrayList<>();
        for (int i = 0; i < 횟수; i++) {
            결과들.add(발급을_시도한다());
        }
        return 결과들;
    }

    private CircuitBreaker 서킷() {
        return circuits.find("backend").orElseThrow(
                () -> new IllegalStateException("서킷이 없다 — 이름이 바뀌었는지 본다"));
    }

    private double 끊은_수() {
        return meters.counter("waiting.backend.body.cut").count();
    }

    @Test
    @DisplayName("C18b_끝나지_않는_본문은_상한에서_잘린_200_으로_끝난다")
    void C18b_끝나지_않는_본문은_상한에서_잘린_200_으로_끝난다() {
        List<Outcome> 정상 = new ArrayList<>();
        List<Outcome> 장애중 = new ArrayList<>();
        List<Outcome> 회복 = new ArrayList<>();
        double[] 끊은_증가 = new double[1];
        int[] 서킷_실패 = new int[1];
        int[] 서킷_성공_증가 = new int[1];

        ChaosScenario.named("C18-b 뒷단 본문 멎음")
                .baseline(() -> 정상.addAll(여러_번_시도한다(2)))
                .inject(() -> 본문이_멎었다.set(true))
                .duringFault(() -> {
                    double 전 = 끊은_수();
                    int 성공_전 = 서킷().getMetrics().getNumberOfSuccessfulCalls();
                    장애중.addAll(여러_번_시도한다(보낼_수));
                    끊은_증가[0] = 끊은_수() - 전;
                    서킷_실패[0] = 서킷().getMetrics().getNumberOfFailedCalls();
                    서킷_성공_증가[0] = 서킷().getMetrics().getNumberOfSuccessfulCalls() - 성공_전;
                })
                .recover(() -> 본문이_멎었다.set(false))
                .afterRecovery(() -> 회복.addAll(여러_번_시도한다(2)))
                .assertEntry(() -> RecoveryCriteria.violations(온전히_받았다("정상", 정상)))
                .assertDuring(() -> RecoveryCriteria.violations(
                        상한에서_끝났다(장애중),
                        상한이_끊었다(끊은_증가[0]),
                        // **결함을 못 박는다.** 봉투가 갈리고(CY-713) 끊는 자리가 서킷 바깥이라
                        // 성공으로 쌓인다(CY-710). 고치면 여기가 빨개지고, 그때 판정을 뒤집는다.
                        잘린_200_이_나갔다(장애중),
                        서킷이_성공으로_셌다(서킷_실패[0], 서킷_성공_증가[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        온전히_받았다("회복", 회복),
                        뒷단.중복_수신이_없다()))
                .run();
    }

    private Optional<String> 온전히_받았다(String 구간, List<Outcome> 결과들) {
        return !결과들.isEmpty() && 결과들.stream().allMatch(r -> r.status() == 200 && !r.잘림())
                ? Optional.empty()
                : Optional.of("%s — 전부 온전한 200 이어야 한다: %s".formatted(구간, 결과들));
    }

    /** 본문 상한에서 끝났는가. 아래로도 묶어야 상한이 응답 상한의 두 배로 배선된 것이 드러난다. */
    private Optional<String> 상한에서_끝났다(List<Outcome> 결과들) {
        Duration 하한 = 본문_상한.minus(Duration.ofMillis(50));
        Duration 한계 = 본문_상한.plus(여유);
        return 결과들.size() == 보낼_수 && 결과들.stream().allMatch(
                r -> r.걸림().compareTo(하한) >= 0 && r.걸림().compareTo(한계) < 0)
                ? Optional.empty()
                : Optional.of("본문 상한 %s~%s 에서 안 끝났다: %s".formatted(하한, 한계, 결과들));
    }

    private Optional<String> 상한이_끊었다(double 증가) {
        return 증가 == 보낼_수 ? Optional.empty()
                : Optional.of("본문 상한이 %s 건을 끊었다 (보낸 %d)".formatted(증가, 보낼_수));
    }

    private Optional<String> 잘린_200_이_나갔다(List<Outcome> 결과들) {
        return 결과들.stream().allMatch(r -> r.status() == 200 && r.잘림()) ? Optional.empty()
                : Optional.of("결함이 바뀌었다(CY-713) — 잘린 200 이 아니다: %s".formatted(결과들));
    }

    /** 끊긴 호출이 실패가 아니라 성공으로 쌓였다. 실패 0 만 보면 아예 안 센 경우도 초록이다. */
    private Optional<String> 서킷이_성공으로_셌다(int 실패, int 성공_증가) {
        return 실패 == 0 && 성공_증가 == 보낼_수 ? Optional.empty()
                : Optional.of("결함이 바뀌었다(CY-710) — 서킷 실패 %d, 성공 증가 %d (보낸 %d)"
                        .formatted(실패, 성공_증가, 보낼_수));
    }
}

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
 * C18-b — 헤더 200 은 오고 본문이 안 끝난다 (CY-870).
 *
 * <p>본문 상한이 커넥션을 끊어 매달리지는 않는다. 다만 헤더가 이미 나가 <b>사용자는 잘린 200 을
 * 받고, 서킷은 성공으로 센다</b>. 받아들인 한계라 사실로 못 박는다 — 바뀌는 날 이 판정이 뒤집힌다.
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

    /** 끊긴 뒤 클라이언트에 닿기까지의 여유. */
    private static final Duration 여유 = Duration.ofSeconds(2);

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
    private record 결과(int status, boolean 잘림, Duration 걸림) {
    }

    @LocalServerPort
    private int port;

    @Autowired
    private CircuitBreakerRegistry circuits;

    @Autowired
    private MeterRegistry meters;

    /** 회원 번호. 요청마다 새로 뽑는다 — 고정 시계라 같은 회원은 초당 상한에 걸린다. */
    private final AtomicInteger 회원 = new AtomicInteger(70_000);

    private 결과 발급을_시도한다() {
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
        return new 결과(응답.getStatus().value(), 잘림, Duration.ofNanos(System.nanoTime() - 시작));
    }

    private List<결과> 여러_번_시도한다(int 횟수) {
        List<결과> 결과들 = new ArrayList<>();
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
        List<결과> 정상 = new ArrayList<>();
        List<결과> 장애중 = new ArrayList<>();
        List<결과> 회복 = new ArrayList<>();
        double[] 끊은_증가 = new double[1];
        int[] 서킷_실패 = new int[1];

        ChaosScenario.named("C18-b 뒷단 본문 멎음")
                .baseline(() -> 정상.addAll(여러_번_시도한다(2)))
                .inject(() -> 본문이_멎었다.set(true))
                .duringFault(() -> {
                    double 전 = 끊은_수();
                    장애중.addAll(여러_번_시도한다(보낼_수));
                    끊은_증가[0] = 끊은_수() - 전;
                    서킷_실패[0] = 서킷().getMetrics().getNumberOfFailedCalls();
                })
                .recover(() -> 본문이_멎었다.set(false))
                .afterRecovery(() -> 회복.addAll(여러_번_시도한다(2)))
                .assertEntry(() -> RecoveryCriteria.violations(온전히_받았다("정상", 정상)))
                .assertDuring(() -> RecoveryCriteria.violations(
                        상한에서_끝났다(장애중),
                        상한이_끊었다(끊은_증가[0]),
                        // **사실이다.** 헤더가 나간 뒤라 503 으로 못 바꾸고, 끊는 자리가 서킷
                        // 바깥이라 실패로 안 쌓인다. 둘 중 하나라도 바뀌면 여기가 빨개진다.
                        잘린_200_이_나갔다(장애중),
                        서킷은_못_봤다(서킷_실패[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        온전히_받았다("회복", 회복),
                        뒷단.중복_수신이_없다()))
                .run();
    }

    private Optional<String> 온전히_받았다(String 구간, List<결과> 결과들) {
        return !결과들.isEmpty() && 결과들.stream().allMatch(r -> r.status() == 200 && !r.잘림())
                ? Optional.empty()
                : Optional.of("%s — 전부 온전한 200 이어야 한다: %s".formatted(구간, 결과들));
    }

    /** 매달리지 않았는가. 본문 상한에 끊겨 클라이언트까지 닿는 시간 안에 끝나야 한다. */
    private Optional<String> 상한에서_끝났다(List<결과> 결과들) {
        Duration 한계 = 본문_상한.plus(여유);
        return 결과들.size() == 보낼_수
                && 결과들.stream().allMatch(r -> r.걸림().compareTo(한계) < 0)
                ? Optional.empty()
                : Optional.of("본문 상한 %s 안에 안 끝났다: %s".formatted(한계, 결과들));
    }

    private Optional<String> 상한이_끊었다(double 증가) {
        return 증가 == 보낼_수 ? Optional.empty()
                : Optional.of("본문 상한이 %s 건을 끊었다 (보낸 %d)".formatted(증가, 보낼_수));
    }

    private Optional<String> 잘린_200_이_나갔다(List<결과> 결과들) {
        return 결과들.stream().allMatch(r -> r.status() == 200 && r.잘림()) ? Optional.empty()
                : Optional.of("사실이 바뀌었다 — 잘린 200 이 아니다: %s".formatted(결과들));
    }

    private Optional<String> 서킷은_못_봤다(int 실패) {
        return 실패 == 0 ? Optional.empty()
                : Optional.of("사실이 바뀌었다 — 서킷이 끊긴 본문을 실패 %d 건으로 셌다".formatted(실패));
    }
}

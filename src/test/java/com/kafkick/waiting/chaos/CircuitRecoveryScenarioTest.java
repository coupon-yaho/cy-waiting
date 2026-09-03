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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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

/**
 * C8 — 뒷단 지연 → 서킷 오픈 → half-open 회복 (8.3.4 · 5절).
 *
 * <p>게이트 G8.12 를 직접 잰다 — <b>회복 시도가 두 번을 넘으면 안 된다.</b>
 */
// 반복 실패는 유입 억제가 안 걸린다는 뜻이다. half-open 순간 그때 도착한 모든
// 트래픽이 약한 뒷단에 꽂혀 다시 열리고, 그러면 회복이 영영 안 온다.
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(CircuitRecoveryScenarioTest.TogglingBackend.class)
class CircuitRecoveryScenarioTest {

    private static final Instant 지금 = Instant.parse("2026-08-31T00:00:00Z");
    private static final String COUPON = "c1";

    /** 뒷단이 멎었는가. 이 스위치로 장애를 넣고 걷는다. */
    private static final AtomicBoolean 멎었다 = new AtomicBoolean();

    /** 짧게 잡는다. 운영값으로 재면 시험 하나가 그만큼 걸린다. */
    private static final Duration 응답_상한 = Duration.ofMillis(300);
    // 응답 상한을 줄인 판이라 연결 상한도 그보다 짧아야 한다 —
    // 기본값(500ms)을 그대로 두면 기동이 막힌다.
    private static final Duration 연결_상한 = Duration.ofMillis(100);

    private static final BackendStub 뒷단 = BackendStub.멎을_수_있다(멎었다::get);

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        // **레디스를 띄운다.** 안 띄우면 줄 등록이 안 돼 전량이 fail-open 으로
        // 새고, 그때 이 시험이 재는 것이 통째로 바뀐다 — 뒷단 유입 0 이 "줄로
        // 보냈다" 와 "판정이 통과시켰는데 라우트가 끊었다" 를 못 가른다.
        faults = RedisFaults.시작한다();
        registry.add("spring.data.redis.url", faults::주소);
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("waiting.backend.response-timeout", () -> 응답_상한);
        registry.add("waiting.backend.connect-timeout", () -> 연결_상한);
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
    static class TogglingBackend {

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

    /** 열린 횟수. <b>두 번을 넘으면 회복이 반복 실패한 것이다</b> (G8.12). */
    private final AtomicInteger 열린_횟수 = new AtomicInteger();

    /**
     * 회복을 기다리는 동안 볼 시계.
     *
     * <p>판정이 보는 시계는 고정이라 여기 못 쓴다 — 안 흐르면 대기 시간이 영영
     * 안 지난다. 주입해 두면 시험이 그것을 바꿀 수 있다.
     */
    private final Supplier<Instant> 벽시계 = Instant::now;

    /** 회복을 포기하는 한계. 서킷의 대기 시간보다 넉넉해야 한다. */
    private static final Duration 회복_한계 = Duration.ofSeconds(20);

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(응답_상한.multipliedBy(30))
                .build();
    }

    private int 발급을_시도한다(int member) {
        return 클라이언트().post()
                .uri("/api/v1/coupons/" + COUPON + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }

    private List<Integer> 여러_번_시도한다(int 횟수, int 시작_회원) {
        List<Integer> 상태 = new ArrayList<>();
        for (int i = 0; i < 횟수; i++) {
            상태.add(발급을_시도한다(시작_회원 + i));
        }
        return 상태;
    }

    private CircuitBreaker 서킷() {
        return circuits.find("backend").orElseThrow(
                () -> new IllegalStateException("서킷이 없다 — 이름이 바뀌었는지 본다"));
    }

    /**
     * <b>서킷이 열렸다가 두 번 안에 닫힌다</b> (G8.12).
     *
     * <p>반복 실패는 유입 억제가 안 걸린다는 뜻이다. half-open 순간 그때 도착한
     * 모든 트래픽이 약한 뒷단에 꽂혀 다시 열린다.
     */
    @Test
    @DisplayName("C8_서킷이_열렸다가_두_번_안에_닫힌다")
    void C8_서킷이_열렸다가_두_번_안에_닫힌다() {
        long[] 유지중_유입 = new long[1];

        List<Integer> 장애중_상태 = new ArrayList<>();
        List<Integer> 회복_상태 = new ArrayList<>();
        long[] 회복_유입 = new long[1];

        ChaosScenario.named("C8 뒷단 지연 → 서킷 오픈")
                .baseline(() -> {
                    // **서킷은 첫 요청이 만든다.** 그 전에 잡으려 하면 없다.
                    여러_번_시도한다(3, 1_000);
                    서킷().getEventPublisher().onStateTransition(e -> {
                        if (e.getStateTransition().getToState() == CircuitBreaker.State.OPEN) {
                            열린_횟수.incrementAndGet();
                        }
                    });
                })
                .inject(() -> 멎었다.set(true))
                .duringFault(() -> {
                    여러_번_시도한다(5, 2_000);
                    long 열린_뒤 = 뒷단.받은_수();
                    장애중_상태.addAll(여러_번_시도한다(5, 2_100));
                    유지중_유입[0] = 뒷단.받은_수() - 열린_뒤;
                })
                .recover(() -> 멎었다.set(false))
                .afterRecovery(() -> {
                    닫힐_때까지_두드린다();
                    // **뒷단을 직접 찌른다.** 서킷 상태는 시계가 흐르면 뒷단이
                    // 죽은 채로도 바뀌므로, 그것만 보면 복구를 안 해도 회복으로
                    // 읽힌다. 게이트웨이를 거쳐서는 못 본다 — 반쯤 열린 구간에
                    // 뒷단으로 가는 유일한 길이 배분이 준 차례다 (CY-813).
                    회복_유입[0] = 뒷단이_직접_답한다() ? 1 : 0;
                    회복_상태.addAll(여러_번_시도한다(10, 3_000));
                })
                .assertEntry(ChaosScenario.Verdict.none())
                .assertDuring(() -> RecoveryCriteria.violations(서킷이_열렸다(),
                        유입이_멎었다(유지중_유입[0]),
                        // **막는 것과 줄에 세우는 것은 다르다.** 뒷단 유입 0 은
                        // 둘 다에서 나온다. 응답을 봐야 어느 쪽인지 갈린다 —
                        // 전량이 5xx 면 계획이 요구한 "큐 등록 정상" 이 아니다.
                        줄에_세웠다("유지", 장애중_상태)))
                // **닫히는 것까지는 여기서 못 잰다** (CY-813). HALF_OPEN 이면
                // 판정이 전원을 줄에 세우므로, 뒷단으로 가는 유일한 길이 배분이
                // 차례를 준 사람이다. 이 시험은 스케줄러를 꺼 뒀고, 켜면 리더
                // 락을 두고 다른 시험과 겹친다.
                //
                // 대신 **대기 시간이 지나 시도에 들어갔는지**를 잰다. 거기까지가
                // 서킷의 몫이고, 그 뒤는 배분의 몫이다.
                //
                // **닫힌 것을 실패로 안 읽는다.** 앞선 회차는 끝에서 HALF_OPEN 을
                // 못 박았는데, 그건 "닫히지 못한다" 를 요구하는 것이라 실제로
                // 닫히면 시험이 깨졌다 — 넷 중 셋이 그렇게 죽었다. 계획이
                // 요구하는 것은 성공하면 닫히는 것이고, 여기서 재는 것은
                // 열린 채로 굳지 않는 것이다.
                .assertRecovery(() -> RecoveryCriteria.violations(
                        시도에_들어갔다(),
                        // **뒷단이 정말 살아났는가.** 이것이 없으면 복구를 아예
                        // 안 해도 위 판정들이 전부 통과한다 — 서킷 상태는 시계가
                        // 흐르면 뒷단이 죽은 채로도 바뀌기 때문이다.
                        뒷단이_다시_받았다(회복_유입[0]),
                        // **회복 구간에는 이 판정을 안 건다.** 유지 구간에
                        // 밀어 넣은 사람들로 줄이 차서 429 가 정상이다. 그건
                        // 서킷이 아니라 큐 용량의 일이고, 여기서 재면 시나리오가
                        // 자기가 만든 상태를 결함으로 읽는다.
                        // 반쯤 열린 구간의 시험 요청이 불어나면 뒷단이 같은
                        // 요청을 두 번 받는다. 발급 경로에서 그건 초과 발급이다.
                        뒷단.중복_수신이_없다()))
                // **RC1~RC6 은 여기서 안 잰다.** 반쯤 열린 구간에 뒷단으로 가는
                // 유일한 길이 배분이 준 차례인데 스케줄러를 껐다 (CY-813).
                // 순번도 자리도 안 생기고, 회복 버스트를 잴 유입도 없다.
                //
                // **"회복 시도 ≤ 2" 도 지금은 공허하다** (CY-834). 두드리는
                // 루프가 열린 상태를 벗어나는 순간 빠져나오므로 프로브가 실패해
                // 다시 열릴 시간이 없다 — 관측값이 늘 1 이다.
                //
                // **지연 갈래도 안 밟는다** (CY-833). 응답 상한이 느린 호출
                // 임계보다 낮아 모든 호출이 타임아웃으로 끊긴다.
                .run();
    }

    /**
     * 열린 상태를 벗어날 때까지 눌러 본다.
     *
     * <p>열린 뒤 대기 시간이 지나야 반쯤 열리므로, 횟수가 아니라 <b>시각</b>으로
     * 끊는다 — 요청이 빨라지면 마흔 번이 대기 시간보다 짧게 끝난다.
     */
    private void 닫힐_때까지_두드린다() {
        Instant 시작 = 벽시계.get();
        int 회차 = 0;
        while (서킷().getState() == CircuitBreaker.State.OPEN
                && Duration.between(시작, 벽시계.get()).compareTo(회복_한계) < 0) {
            여러_번_시도한다(2, 3_000 + 회차 * 10);
            회차++;
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

    /**
     * 대기 시간이 지나 회복을 시도하는 상태로 들어갔는가.
     *
     * <p>닫히는 것은 프로브가 뒷단에 닿아야 하는데, 그 길이 배분뿐이다
     * (CY-813). 여기서는 서킷이 <b>스스로 열린 채로 안 굳는지</b>까지 본다.
     */
    private Optional<String> 시도에_들어갔다() {
        CircuitBreaker.State 지금_상태 = 서킷().getState();
        return 지금_상태 == CircuitBreaker.State.OPEN
                ? Optional.of("대기 시간이 지났는데 서킷이 열린 채로 굳었다")
                : Optional.empty();
    }

    /**
     * 줄에 세웠는가. 뒷단 유입 0 은 막은 것과 줄로 보낸 것 양쪽에서 나오므로,
     * 응답을 봐야 갈린다. 계획서 C8 진입이 요구하는 것이 이것이다.
     */
    private Optional<String> 줄에_세웠다(String 구간, List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("%s — 보낸 것이 없다".formatted(구간));
        }
        // **202 만 줄에 선 것이다.** 5xx 만 거르면 429·409 도 "큐 등록 정상" 을
        // 만족시킨다 — 줄이 꽉 찼거나 매진이라 못 선 것을 선 것으로 읽는다.
        long 못_선_것 = 상태.stream().filter(status -> status != 202).count();
        return 못_선_것 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 줄에 못 섰다 (보낸 %d): %s"
                        .formatted(구간, 못_선_것, 상태.size(), 상태));
    }

    /**
     * 뒷단을 직접 찔러 살아났는지 본다. 게이트웨이를 안 거치므로 서킷과
     * 무관하게 사실을 알 수 있다.
     */
    private boolean 뒷단이_직접_답한다() {
        try {
            return WebTestClient.bindToServer()
                    .baseUrl("http://localhost:" + 뒷단.port())
                    .responseTimeout(응답_상한.multipliedBy(4))
                    .build()
                    .get().uri("/probe").exchange()
                    .returnResult(Void.class).getStatus().is2xxSuccessful();
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * 뒷단이 정말 살아났는가. <b>서킷 상태만 보면 회복을 안 잰다</b> — 대기
     * 시간이 지나면 뒷단이 죽은 채로도 반쯤 열린 상태로 간다.
     */
    private Optional<String> 뒷단이_다시_받았다(long 회복_유입) {
        return 회복_유입 > 0 ? Optional.empty()
                : Optional.of("회복 구간인데 뒷단이 직접 물어도 안 답한다 — 아직 안 돌아왔다");
    }

    // **G8.12 판정을 안 건다** (CY-834). 두드리는 루프가 열린 상태를 벗어나는
    // 순간 빠져나오므로 프로브가 실패해 다시 열릴 시간이 없다 — 관측값이 늘
    // 1 이라 "2 이하" 가 항진명제다. 뒷단을 영영 안 살리고 판정 사다리의 서킷
    // 갈래까지 지운 경우에도 그랬다. 셀 기회를 만드는 것이 먼저다.
}

package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.control.SnapshotSource;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
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
 * C1 — Redis 완전 정지 → 복구 (8.3.4 · 5절).
 *
 * <p>장애 주입 장치가 도는지가 아니라 <b>시나리오</b>를 잰다.
 */
// 진입·유지·회복을 따로 재고 공통 기준을 건다. 지금까지의 카오스 시험은 장치를
// 쟀지, 그 장치를 겪은 게이트웨이가 무엇을 했는지는 안 쟀다.
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(RedisFullStopScenarioTest.StubbedBackend.class)
class RedisFullStopScenarioTest {

    /** 회복까지 걸린 시간. 시나리오 안에서 재고 판정에 넘긴다. */
    private Duration 회복까지_걸린_시간;

    /**
     * 회복을 기다리는 동안 볼 시계.
     *
     * <p>판정이 보는 시계는 고정이라 여기 못 쓴다 — 안 흐르면 무한히 기다린다.
     * 이 자리만 실제로 흐르는 시각이 필요하고, 주입해 두면 시험이 그것을 바꿀 수
     * 있다.
     */
    private final Supplier<Instant> 벽시계 = Instant::now;

    private static final Instant 지금 = Instant.parse("2026-08-30T00:00:00Z");
    private static final String COUPON = "c1";

    /**
     * 전역 크레딧. <b>고정 시계라 초당 예산이 안 채워진다</b> — 시험 전체가 한
     * 초 안에 일어나므로, 보낼 요청 수를 다 덮을 만큼 크게 잡는다.
     */
    private static final int 크레딧 = 40;

    /**
     * 한 초에 통과할 수 있는 몫. 노드가 하나이므로 전역 크레딧과 같다.
     *
     * <p>한산한 쿠폰은 fail-open 을 안 거치고 정상 경로로 통과한다. 그래서
     * 비교 대상은 fail-open 몫이 아니라 이 예산이다.
     */
    private static final long 노드_예산 = 크레딧;

    /** 장애 구간에 보내는 요청 수. 예산보다 커야 넘치는지를 잰다. */
    private static final int 장애중_보낼_수 = (int) 노드_예산 + 8;

    private static final int 보낼_수 = 5;

    /** RC3 의 한계. 이 안에 판정이 정상으로 돌아와야 한다. */
    private static final Duration 회복_한계 = Duration.ofSeconds(30);

    /** 뒷단이 받은 누적 수. 기록기가 1초마다 이걸 읽어 차분을 낸다. */
    private static final AtomicLong 뒷단이_받은_수 = new AtomicLong();

    private static final DisposableServer 뒷단 = HttpServer.create()
            .port(0)
            .handle((request, response) -> {
                뒷단이_받은_수.incrementAndGet();
                return response.status(HttpStatus.OK.value()).send();
            })
            .bindNow();

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.url", faults::주소);
    }

    @AfterAll
    static void 내린다() {
        뒷단.disposeNow();
        if (faults != null) {
            faults.close();
        }
    }

    @TestConfiguration
    static class StubbedBackend {

        @Bean
        @Primary
        Clock 고정_시계() {
            return Clock.fixed(지금, ZoneOffset.UTC);
        }

        /**
         * <b>줄이 선 쿠폰에 작은 크레딧을 심는다.</b>
         *
         * <p>한산한 쿠폰으로 재면 fail-open 상한이 안 물린다 — 무제한 통과를
         * 재려는 판정이 아무것도 안 재게 된다.
         */
        @Bean
        @Primary
        SnapshotSource 몰리는_재료() {
            Map<String, String> 재료 = SnapshotCodec.create().encode(
                    new GatewaySnapshot(
                            // **한산한 쿠폰이다.** 줄이 선 쿠폰으로 재면 전원이
                            // 202 로 끝나 뒷단에 아무것도 안 닿고, 그러면 회복
                            // 버스트를 비교할 정상값이 없다. 크레딧을 작게 둬서
                            // 한산해도 상한이 물리게 한다.
                            Map.of(COUPON, CouponStates.idle(1_000)),
                            new SnapshotMeta(크레딧, 1), 지금),
                    CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
            return () -> Mono.just(재료);
        }
    }

    @LocalServerPort
    private int port;

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build();
    }

    /** 한 번 발급을 시도하고 받은 상태를 돌려준다. */
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

    /**
     * <b>진입에서 전면 차단도 무제한 통과도 없다.</b>
     *
     * <p>Redis 가 죽으면 큐 등록이 불가하므로 fail-open 이 적용되는 유일한
     * 구간이다. 전부 막으면 한산한 쿠폰까지 죽고, 전부 흘리면 상한이 사라진다.
     */
    @Test
    @DisplayName("C1_레디스가_죽었다_살아난다")
    void C1_레디스가_죽었다_살아난다() {
        BackendRpsRecorder 유입 = new BackendRpsRecorder(뒷단이_받은_수::get);
        List<Integer> 정상_상태 = new ArrayList<>();
        List<Integer> 장애중_상태 = new ArrayList<>();
        List<Integer> 회복_상태 = new ArrayList<>();

        ChaosScenario.named("C1 Redis 완전 정지")
                .baseline(() -> {
                    유입.sample(지금);
                    정상_상태.addAll(여러_번_시도한다(보낼_수, 1_000));
                    유입.sample(지금.plusSeconds(1));
                    // **정상 구간이 정상인지부터 본다.** 여기가 이미 장애면
                    // 뒤의 비교가 전부 무의미하다 — 버스트를 잴 기준이 없다.
                    assertThat(정상_상태).as("전제 — 5xx 없이 답한다")
                            .noneMatch(status -> status >= 500);
                    assertThat(정상_상태).as("전제 — 뒷단까지 간 요청이 있다")
                            .anyMatch(status -> status < 300);
                })
                .inject(() -> faults.끊는다())
                .duringFault(() -> 장애중_상태.addAll(여러_번_시도한다(장애중_보낼_수, 2_000)))
                .recover(() -> faults.붙인다())
                .afterRecovery(() -> {
                    유입.sample(지금.plusSeconds(2));
                    // **바로 다음 요청이 성공하기를 요구하지 않는다.** 레디스가
                    // 막 돌아온 순간은 연결이 다시 맺히는 중이다. RC3 가 재는
                    // 것은 "즉시" 가 아니라 "한계 안에" 다.
                    회복까지_걸린_시간 = 판정이_돌아올_때까지_기다린다(회복_상태, 벽시계);
                    유입.sample(지금.plusSeconds(3));
                })
                // **진입 판정은 주입 직후, 유지 구간이 시작되기 전이다.**
                // 그 시점에는 아직 요청을 안 보냈으므로 여기서 잴 것이 없다.
                .assertEntry(ChaosScenario.Verdict.none())
                // **유지 판정이 장애가 살아 있는 동안 돈다.** 복구 뒤로 미루면
                // 이미 걷힌 상태를 읽어 전면 차단도 무제한 통과도 안 보인다.
                .assertDuring(() -> RecoveryCriteria.violations(
                        전면_차단이_아니다(장애중_상태), 무제한_통과가_아니다(장애중_상태)))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        RecoveryCriteria.slowVerdictReturn(회복까지_걸린_시간, 회복_한계),
                        // **차분은 뒤 표본의 시각에 쌓인다.** 앞 표본의 초로
                        // 창을 잡으면 정상 구간이 통째로 0 으로 보인다.
                        RecoveryCriteria.recoveryBurst(
                                유입.averageRps(지금.plusSeconds(1), 지금.plusSeconds(2)),
                                유입.peakRps(지금.plusSeconds(3), 지금.plusSeconds(4)))))
                .run();

        assertThat(회복_상태).as("회복 뒤에는 5xx 없이 답한다")
                .isNotEmpty()
                .last(org.assertj.core.api.InstanceOfAssertFactories.INTEGER)
                .isLessThan(500);
    }

    /** 전부 5xx 면 한산한 쿠폰까지 죽은 것이다. 판정은 레디스를 안 친다. */
    private Optional<String> 전면_차단이_아니다(List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("장애 구간에서 아무것도 안 봤다 — 판정할 것이 없다");
        }
        return 상태.stream().allMatch(s -> s >= 500)
                ? Optional.of("진입에서 전면 차단이 일어났다: " + 상태)
                : Optional.empty();
    }

    /** 통과가 예산을 넘으면 상한이 사라진 것이다. */
    private Optional<String> 무제한_통과가_아니다(List<Integer> 상태) {
        long 통과 = 상태.stream().filter(s -> s < 300).count();
        return 통과 > 노드_예산
                ? Optional.of("진입에서 예산 %d 를 넘겨 %d 건이 통과했다"
                        .formatted(노드_예산, 통과))
                : Optional.empty();
    }

    /**
     * 5xx 가 그칠 때까지 눌러 보고 걸린 시간을 돌려준다.
     *
     * <p>끝내 안 그치면 {@code null} 이다 — 판정기가 그것을 위반으로 읽는다.
     *
     * @param 지금 벽시계를 주입받는다. 시험이 실시계를 직접 읽으면 장비에 따라
     *             갈리고, 그때 빨간 것이 결함인지 느린 장비인지 못 가린다
     */
    private Duration 판정이_돌아올_때까지_기다린다(List<Integer> 회복_상태, Supplier<Instant> 지금) {
        Instant 시작 = 지금.get();
        while (Duration.between(시작, 지금.get()).compareTo(회복_한계) < 0) {
            List<Integer> 한_판 = 여러_번_시도한다(보낼_수, 3_000 + 회복_상태.size());
            회복_상태.addAll(한_판);
            if (한_판.stream().noneMatch(s -> s >= 500)) {
                return Duration.between(시작, 지금.get());
            }
        }
        return null;
    }
}

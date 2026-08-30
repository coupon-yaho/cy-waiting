package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.domain.queue.QueueToken;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.control.SnapshotSource;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
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
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
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

    /** 장애 구간에 줄을 쳤을 때의 상태. 레디스가 정말 죽었는지의 증거다. */
    private int 줄을_친_결과;

    /** 장애 전후의 자리. RC5 가 이 둘을 비교한다. */
    private Map<String, Double> 장애_전_자리 = Map.of();

    private Map<String, Double> 회복_뒤_자리 = Map.of();

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

    /** 줄이 서는 쿠폰. RC2·RC5 는 줄에 사람이 있어야 잴 수 있다. */
    private static final String QUEUED = "c2";

    /** 장애 전에 줄을 세울 인원. */
    private static final int 줄_선_사람 = 5;

    /**
     * 전역 크레딧. <b>고정 시계라 초당 예산이 안 채워진다</b> — 시험 전체가 한
     * 초 안에 일어나므로, 보낼 요청 수를 다 덮을 만큼 크게 잡는다.
     */
    // **시험 전체가 보내는 것보다 훨씬 크게 잡는다.** 고정 시계라 초당 예산이
    // 한 번만 채워지므로, 예산이 물리면 그 뒤의 503 이 전부 "레디스를 기다렸다"
    // 로 읽힌다. 여기서 재려는 것은 예산이 아니라 판정이 레디스를 치는가다.
    private static final int 크레딧 = 10_000;

    /** 장애 구간에 보내는 요청 수. 예산 안이라 통과가 곧 정상이다. */
    private static final int 장애중_보낼_수 = 20;

    private static final int 보낼_수 = 5;

    /** 재고. 이 시나리오가 보내는 전체보다 커야 미달이 위반으로 안 읽힌다. */
    private static final long 재고 = 1_000;

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
                            Map.of(COUPON, CouponStates.idle(1_000),
                                    // **용량이 대기 인원보다 넉넉해야 한다.**
                                    // 용량은 크레딧에 비례하므로, 크레딧이 작으면
                                    // 줄이 꽉 찬 것으로 판정돼 전원이 429 를 받는다.
                                    // 배분 스케줄러는 꺼 뒀으므로 선 줄은 유지된다.
                                    QUEUED, CouponStates.queueing(100, 1_000, 101)),
                            new SnapshotMeta(크레딧, 1), 지금),
                    CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
            return () -> Mono.just(재료);
        }
    }

    @LocalServerPort
    private int port;

    /**
     * 순번 조회 토큰. <b>이 경로는 레디스를 친다</b> — 발급 경로가 안 치는 것과
     * 대조가 되어, 레디스가 정말 죽었는지·정말 돌아왔는지를 여기서 본다.
     */
    @Autowired
    private QueueToken tokens;

    /** 자리는 응답에 안 실린다. RC5 를 재려면 줄을 직접 봐야 한다. */
    @Autowired
    private ReactiveStringRedisTemplate redis;

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

    /** 줄에 세운다. 장애 전에 불러 자리를 만든다. */
    private void 줄에_세운다() {
        for (int i = 0; i < 줄_선_사람; i++) {
            클라이언트().post()
                    .uri("/api/v1/coupons/" + QUEUED + "/issue")
                    .header("X-Member-Id", String.valueOf(7_000 + i))
                    .header("X-Member-Grade", "GOLD")
                    .exchange()
                    .returnResult(Void.class);
        }
    }

    /** 줄에 선 사람들의 자리. 응답에 안 실리므로 줄을 직접 읽는다. */
    private Map<String, Double> 자리들() {
        String key = RedisKeys.queue(QUEUED, 1, 0);
        Map<String, Double> 본_것 = new LinkedHashMap<>();
        for (int i = 0; i < 줄_선_사람; i++) {
            String member = String.valueOf(7_000 + i);
            Double score = redis.opsForZSet().score(key, member).block(Duration.ofSeconds(5));
            if (score != null) {
                본_것.put(member, score);
            }
        }
        return 본_것;
    }

    /** 줄을 치는 요청. 레디스가 죽어 있으면 5xx 가 온다. */
    private int 순번을_묻는다(int member) {
        return 클라이언트().get()
                .uri("/api/v1/coupons/" + COUPON + "/queue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .header("Queue-Token", tokens.issue(COUPON, String.valueOf(member), 지금))
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
                    줄에_세운다();
                    장애_전_자리 = 자리들();
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
                .duringFault(() -> {
                    장애중_상태.addAll(여러_번_시도한다(장애중_보낼_수, 2_000));
                    // **레디스가 정말 죽었는지 확인한다.** 안 죽었으면 이 시나리오
                    // 전체가 아무것도 안 잰 것이다.
                    줄을_친_결과 = 순번을_묻는다(8_000);
                })
                .recover(() -> faults.붙인다())
                .afterRecovery(() -> {
                    // **바로 다음 요청이 성공하기를 요구하지 않는다.** 레디스가
                    // 막 돌아온 순간은 연결이 다시 맺히는 중이다. RC3 가 재는
                    // 것은 "즉시" 가 아니라 "한계 안에" 다.
                    회복까지_걸린_시간 = 판정이_돌아올_때까지_기다린다(회복_상태, 벽시계);
                    회복_뒤_자리 = 자리들();
                    // **버스트는 회복을 확인한 뒤에 잰다.** 대기 루프가 보낸
                    // 요청까지 세면, 회복이 늦을수록 시험이 더 많이 보내므로
                    // 버스트가 커진다 — 재는 도구가 재는 대상을 오염시킨다.
                    // 정상 구간과 같은 수만 보내고 그 창에서 봉우리를 본다.
                    유입.sample(지금.plusSeconds(2));
                    여러_번_시도한다(보낼_수, 5_000);
                    유입.sample(지금.plusSeconds(3));
                })
                // **진입 판정은 주입 직후, 유지 구간이 시작되기 전이다.**
                // 그 시점에는 아직 요청을 안 보냈으므로 여기서 잴 것이 없다.
                .assertEntry(ChaosScenario.Verdict.none())
                // **유지 판정이 장애가 살아 있는 동안 돈다.** 복구 뒤로 미루면
                // 이미 걷힌 상태를 읽어 전면 차단도 무제한 통과도 안 보인다.
                // **fail-open 상한은 여기서 안 잰다.** 한산한 쿠폰은 그 경로를
                // 안 거친다. 상한을 재려면 줄이 선 재료가 필요하고, 그러면
                // 전원이 202 로 끝나 RC4 를 못 잰다 — 재료가 둘 필요하다.
                .assertDuring(() -> RecoveryCriteria.violations(
                        오백이_안_샌다(장애중_상태), 레디스가_정말_죽었다()))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        RecoveryCriteria.slowVerdictReturn(회복까지_걸린_시간, 회복_한계),
                        // RC1 — 뒷단이 받은 수가 재고를 안 넘는다. 이 시나리오는
                        // 발급만 때리므로 수신 수가 곧 발급 시도다.
                        RecoveryCriteria.overIssued(유입.total(), 재고),
                        // RC6 — 회복 뒤 유입이 정상 수준으로 돌아온다.
                        RecoveryCriteria.notConverged("판정 통과 비율",
                                통과_비율(정상_상태), 통과_비율(회복_상태)),
                        // **차분은 표집 시각이 아니라 그것이 온 초에 쌓인다.**
                        // 표집과 표집 사이에 온 것이므로 앞 초의 몫이다.
                        RecoveryCriteria.recoveryBurst(
                                유입.averageRps(지금, 지금.plusSeconds(1)),
                                유입.peakRps(지금.plusSeconds(2), 지금.plusSeconds(3))),
                        // **RC5 는 여기서 못 잰다** (CY-809). 픽스처가
                        // `--appendonly no` 로 띄우므로 컨테이너를 끊었다 붙이면
                        // 줄이 통째로 사라진다. 계획서가 요구하는 "큐 순번 전원
                        // 유지" 를 구조적으로 만족할 수 없다.
                        //
                        // 대신 **줄이 정말 사라졌는지**를 못 박는다. 나중에
                        // 영속성을 켜면 이 단언이 빨개져 그때 RC5 로 바꾼다.
                        줄이_영속성_없이_사라졌다()))
                .run();

        assertThat(회복_상태).as("회복 뒤에는 5xx 없이 답한다")
                .isNotEmpty()
                .last(org.assertj.core.api.InstanceOfAssertFactories.INTEGER)
                .isLessThan(500);
    }

    /**
     * <b>503 이 하나도 새면 안 된다.</b>
     *
     * <p>한산한 쿠폰의 판정은 레디스를 안 치므로, 레디스가 죽어도 통과해야 한다.
     * 하나라도 5xx 면 그 요청은 레디스를 기다린 것이다. "전부 5xx 인가" 만 보면
     * 한 건이라도 성공하는 순간 임의의 유출이 통과한다.
     */
    private Optional<String> 오백이_안_샌다(List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("장애 구간에서 아무것도 안 봤다 — 판정할 것이 없다");
        }
        long 샌_것 = 상태.stream().filter(s -> s >= 500).count();
        return 샌_것 > 0
                ? Optional.of("장애 구간에서 503 이 %d 건 샜다 — 판정이 레디스를 기다렸다"
                        .formatted(샌_것))
                : Optional.empty();
    }

    /**
     * 장애 구간에 줄을 치면 실패해야 한다.
     *
     * <p>성공하면 레디스가 안 죽은 것이고, 그러면 이 시나리오 전체가 아무것도
     * 안 잰 것이다. 전제를 판정으로 못 박아 둔다.
     */
    private Optional<String> 레디스가_정말_죽었다() {
        return 줄을_친_결과 < 500
                ? Optional.of("장애 구간인데 줄 조회가 %d 로 성공했다 — 레디스가 안 죽었다"
                        .formatted(줄을_친_결과))
                : Optional.empty();
    }

    /**
     * 영속성이 없는 판에서 줄이 사라진 사실을 못 박는다 (CY-809).
     *
     * <p>이 시나리오는 RC5 를 아직 못 잰다. 그 사실을 주석으로만 두면 다음
     * 사람이 "재고 있다" 고 믿는다. 영속성을 켜면 여기가 빨개져 손이 간다.
     */
    private Optional<String> 줄이_영속성_없이_사라졌다() {
        if (장애_전_자리.isEmpty()) {
            return Optional.of("장애 전에 줄이 비어 있었다 — 전제가 안 섰다");
        }
        return 회복_뒤_자리.isEmpty() ? Optional.empty()
                : Optional.of("줄이 살아남았다 — 영속성이 켜졌다면 이제 RC5 로 잰다 (CY-809)");
    }

    /** 통과 비율. RC6 이 이것으로 판정 분포의 수렴을 본다. */
    private double 통과_비율(List<Integer> 상태) {
        return 상태.isEmpty() ? 0
                : (double) 상태.stream().filter(s -> s < 300).count() / 상태.size();
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
            // **줄을 치는 경로까지 돌아와야 회복이다.** 발급 경로는 레디스를
            // 안 치므로 죽은 채로도 통과한다 — 그것만 보면 복구를 안 해도
            // 회복으로 읽힌다.
            한_판.add(순번을_묻는다(9_000 + 회복_상태.size()));
            // **202 를 회복으로 안 읽는다.** 정상 구간이 통과였는데 회복 뒤에
            // 줄에 서기 시작했다면 판정 분포가 아직 안 돌아온 것이다.
            if (한_판.stream().noneMatch(s -> s >= 500)) {
                return Duration.between(시작, 지금.get());
            }
        }
        return null;
    }
}

package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.awaitility.Awaitility;
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
 * C2 — Redis 지연 500ms → 해제 (8.3.4 · 5절).
 *
 * <p><b>판정이 Redis 를 안 치므로 발급 지연이 변하면 안 된다.</b> 변한다면
 * 어딘가에서 치고 있다는 뜻이다 — 불변식 1 의 실전 검증이다.
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
@Import(RedisLatencyScenarioTest.StubbedBackend.class)
class RedisLatencyScenarioTest {

    private static final Instant 지금 = Instant.parse("2026-08-31T00:00:00Z");
    private static final String COUPON = "c1";

    /** 레디스 명령 상한. 이 위로 넣으면 지연이 아니라 정지가 된다. */
    private static final Duration 명령_상한 = Duration.ofMillis(500);

    /**
     * 주입할 지연. <b>상한의 0.6 배다.</b>
     *
     * <p>상한과 같은 값을 넣으면 러너가 조금만 느려져도 전건 타임아웃으로
     * 뒤집혀, 지연 시나리오가 말없이 정지 시나리오가 된다. 실측으로 520ms
     * 에서 이미 같은 배치 안에서 성공과 타임아웃이 갈렸다.
     */
    private static final Duration 지연 = Duration.ofMillis(300);

    /**
     * 판정이 늦어져도 되는 폭. <b>주입량에 묶는다.</b>
     *
     * <p>정상 대비 배수로 두면 문턱이 장비 소음 대역(수 ms)에 들어앉아, 잡아야
     * 할 신호(수백 ms)와 무관해진다.
     */
    private static final Duration 허용_증가 = 지연.dividedBy(2);

    /** 버리는 워밍업 라운드. 첫 요청은 커넥션 수립과 JIT 을 같이 먹는다. */
    private static final int 워밍업 = 5;

    private static final int 보낼_수 = 15;

    private static final AtomicLong 뒷단이_받은_수 = new AtomicLong();

    /**
     * 뒷단이 같은 회원을 두 번 받은 횟수.
     */
    // **비율로 중복을 재면 늘 여유가 생긴다.** 로컬에서 끝난 요청은 분모에만
    // 들어가고, 그만큼 중복이 숨는다. 요청을 짚어 세면 그 여유가 없다.
    private static final AtomicLong 뒷단이_두_번_받은_수 = new AtomicLong();

    private static final Set<String> 뒷단이_본_회원 = ConcurrentHashMap.newKeySet();

    private static final DisposableServer 뒷단 = HttpServer.create()
            .port(0)
            .handle((request, response) -> {
                뒷단이_받은_수.incrementAndGet();
                // 회원 번호는 시험 전체에서 안 겹치게 발급한다. 겹쳐 도착하면
                // 게이트웨이가 한 요청을 두 번 보낸 것이다.
                String member = request.requestHeaders().get("X-Member-Id");
                if (member != null && !뒷단이_본_회원.add(member)) {
                    뒷단이_두_번_받은_수.incrementAndGet();
                }
                return response.status(HttpStatus.OK.value()).send();
            })
            .bindNow();

    private static RedisWireFaults 선;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        선 = RedisWireFaults.시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.host", 선::호스트);
        registry.add("spring.data.redis.port", 선::포트);
    }

    @AfterAll
    static void 내린다() {
        뒷단.disposeNow();
        if (선 != null) {
            선.close();
        }
    }

    @TestConfiguration
    static class StubbedBackend {

        @Bean
        @Primary
        Clock 고정_시계() {
            return Clock.fixed(지금, ZoneOffset.UTC);
        }

        // **재료 스텁을 두지 않는다.** 두면 어댑터가 가려져 레디스 왕복이
        // 애초에 안 생기고, 그러면 "요청마다 재료를 다시 읽는다" 는 회귀가
        // — 불변식 1 이 무너지는 가장 그럴듯한 형태가 — 계측 밖에 남는다.
        // 재료는 실제 레디스에 심는다.
    }

    @LocalServerPort
    private int port;

    /** 카나리를 치는 자리이자 재료를 심는 자리. 앱과 같은 프록시를 지난다. */
    @Autowired
    private ReactiveStringRedisTemplate redis;

    /**
     * <b>재료를 실제 레디스에 심는다.</b>
     *
     * <p>앱은 이것을 자기 어댑터로 읽는다 — 그래야 판정 경로가 새로 왕복을
     * 내기 시작하는 순간 지연에 잡힌다.
     */
    private void 재료를_심는다() {
        Map<String, String> 재료 = SnapshotCodec.create().encode(
                new GatewaySnapshot(Map.of(COUPON, CouponStates.idle(1_000_000)),
                        new SnapshotMeta(10_000, 1), 지금),
                CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
        redis.opsForHash().putAll(RedisKeys.SNAPSHOT, 재료).block(명령_상한.multipliedBy(4));
    }

    /**
     * 재료가 앱에 닿을 때까지 기다린다. 안 닿았으면 전 구간이 거절이다.
     */
    // **폴마다 회원을 새로 쓴다.** 같은 번호로 두 번 치면 뒷단이 그것을 중복
    // 수신으로 세고, 시험 자신이 만든 중복이 판정을 빨갛게 만든다.
    //
    // **상태로 기다린다.** 지연 재는 함수로 폴하면 첫 프로브가 거절일 때 예외가
    // 그대로 터져 나가 기다리질 않는다 — 재료가 아직 안 실린 순간에 걸리면
    // 그 판이 죽고, 그건 결함이 아니라 시험이 너무 일찍 물어본 것이다.
    private void 재료가_닿기를_기다린다() {
        AtomicLong 회원 = new AtomicLong(800);
        Awaitility.await().atMost(Duration.ofSeconds(20))
                .until(() -> 발급_상태((int) 회원.incrementAndGet()) == 200);
    }

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(30))
                .build();
    }

    /**
     * 발급 한 번의 걸린 시간(ms). <b>상태가 200 이 아니면 안 센다.</b>
     *
     * <p>거절은 뒷단 홉을 건너뛰어 정상보다 빠르다. 상태를 안 보면 전 요청을
     * 거절하는 판이 "안 느려졌다" 로 기록된다.
     */
    // 여기서는 실시계를 쓴다. 재려는 것이 시각이 아니라 경과이고, 그것이 이
    // 시나리오의 관측 대상이다. 판정이 보는 시계는 여전히 고정이다.
    private long 발급_지연(int member) {
        long 시작 = System.nanoTime();
        int 상태 = 발급_상태(member);
        long 걸린_시간 = Duration.ofNanos(System.nanoTime() - 시작).toMillis();
        if (상태 != 200) {
            throw new IllegalStateException(
                    "발급이 %d 로 끝났다 — 지연을 잴 수 없다".formatted(상태));
        }
        return 걸린_시간;
    }

    /** 발급 한 번의 상태 코드. 기다리는 자리는 거절을 예외로 안 만든다. */
    private int 발급_상태(int member) {
        return 클라이언트().post()
                .uri("/api/v1/coupons/" + COUPON + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }

    /**
     * <b>꼬리로 판정한다.</b>
     *
     * <p>중앙값은 소수 요청만 레디스를 치는 결함에 눈이 먼다. 모르는 자리에서
     * 새로 치기 시작하는 형태는 본질적으로 소수 요청이다.
     */
    private long 최대_지연(int 시작_회원) {
        // 워밍업은 버린다. 콜드 표본이 분모를 키워 문턱을 관대하게 만든다.
        for (int i = 0; i < 워밍업; i++) {
            발급_지연(시작_회원 + i);
        }
        long 최대 = 0;
        for (int i = 0; i < 보낼_수; i++) {
            최대 = Math.max(최대, 발급_지연(시작_회원 + 워밍업 + i));
        }
        return 최대;
    }

    /**
     * 같은 프록시를 지나는 카나리. <b>주입이 정말 걸렸는지를 여기서 본다.</b>
     *
     * <p>이것이 없으면 "판정이 레디스를 안 친다" 와 "주입이 안 걸렸다" 를 영영
     * 못 가린다. 둘 다 발급 지연이 안 변한 그림으로 나온다.
     */
    private long 카나리_지연() {
        long 시작 = System.nanoTime();
        redis.opsForValue().get("chaos:canary").block(명령_상한.multipliedBy(4));
        return Duration.ofNanos(System.nanoTime() - 시작).toMillis();
    }

    /**
     * <b>지연을 넣어도 판정이 안 느려진다</b> (불변식 1).
     *
     * <p>느려진다면 통과 경로 어딘가가 레디스를 치고 있다. 이 게이트웨이의
     * 존재 이유가 그 왕복을 없앤 것이라, 그때는 설계가 무너진 것이다.
     */
    @Test
    @DisplayName("C2_지연을_넣어도_판정이_안_느려진다")
    void C2_지연을_넣어도_판정이_안_느려진다() {
        long[] 정상 = new long[1];
        long[] 장애중 = new long[1];
        long[] 회복 = new long[1];
        long[] 카나리 = new long[3];
        long[] 도착 = new long[3];
        BackendRpsRecorder 유입 = new BackendRpsRecorder(뒷단이_받은_수::get);

        ChaosScenario.named("C2 Redis 지연 %s".formatted(지연))
                .baseline(() -> {
                    재료를_심는다();
                    재료가_닿기를_기다린다();
                    유입.sample(지금);
                    카나리[0] = 카나리_지연();
                    도착[0] = 잰다(() -> 정상[0] = 최대_지연(1_000));
                    유입.sample(지금.plusSeconds(1));
                })
                .inject(() -> 지연을_넣는다(지연))
                .duringFault(() -> {
                    // **주입이 걸렸는지 먼저 본다.** 이것이 안 느려졌으면 뒤의
                    // 모든 판정이 아무것도 안 잰 것이다.
                    카나리[1] = 카나리_지연();
                    도착[1] = 잰다(() -> 장애중[0] = 최대_지연(2_000));
                })
                .recover(this::지연을_걷는다)
                .afterRecovery(() -> {
                    유입.sample(지금.plusSeconds(2));
                    // **지연이 걷혔는지도 카나리로 본다.** 발급 경로는 레디스를
                    // 안 치므로 안 걷혀도 안 느려진다 — 그것만 보면 복구를 안
                    // 해도 회복으로 읽힌다.
                    카나리[2] = 카나리_지연();
                    도착[2] = 잰다(() -> 회복[0] = 최대_지연(3_000));
                    유입.sample(지금.plusSeconds(3));
                })
                .assertEntry(ChaosScenario.Verdict.none())
                .assertDuring(() -> RecoveryCriteria.violations(
                        주입이_걸렸다(카나리[0], 카나리[1]),
                        판정이_안_느려졌다("장애 중", 정상[0], 장애중[0]),
                        다_뒷단까지_갔다("유지", 도착[1])))
                // **"갱신 루프 정지 0" 은 여기서 못 잰다** (CY-827). 고정
                // 시계라 재료 나이가 안 자라 낡음이 영영 안 뜨고, 유지 구간이
                // 표본 도는 시간뿐이라 배경 루프가 그 창에 한 번도 안 들어온다.
                //
                // **RC1·RC2·RC5·RC6 도 없다.** 이 시나리오는 줄이 안 서는
                // 한산한 쿠폰만 때린다 — 순번도 자리도 생기지 않는다.
                .assertRecovery(() -> RecoveryCriteria.violations(
                        지연이_걷혔다(카나리[0], 카나리[2]),
                        판정이_안_느려졌다("회복 뒤", 정상[0], 회복[0]),
                        // **RC4 의 버스트 쪽은 이 하네스로 못 잰다** (CY-817).
                        // 부하 생성기가 닫힌 루프라 발신 속도가 게이트웨이
                        // 지연으로 정해진다 — 구간마다 같은 수를 보내므로
                        // 비율이 구조적으로 1.00 이다. 대신 아래 둘이 실제로
                        // 잡는 것을 본다.
                        RecoveryCriteria.amplified((long) 워밍업 + 보낼_수, 도착[2]),
                        다_뒷단까지_갔다("회복", 도착[2]),
                        // **중복은 짚어서 센다.** 지연이 걷히는 순간 재전송이
                        // 겹치면 발급 요청이 불어나는데, 비율로는 로컬에서 끝난
                        // 요청만큼 여유가 생겨 그 안에 숨는다.
                        중복_수신이_없다()))
                .run();

    }

    /** 뒷단이 같은 요청을 두 번 받았는가. 발급 경로에서 그건 초과 발급이다. */
    private Optional<String> 중복_수신이_없다() {
        long 중복 = 뒷단이_두_번_받은_수.get();
        return 중복 == 0 ? Optional.empty()
                : Optional.of("RC4 뒷단이 같은 요청을 %d 건 두 번 받았다".formatted(중복));
    }

    /**
     * <b>판정이 살아 있는지를 시험이 스스로 확인한다.</b>
     */
    // 이 시나리오의 판정은 전부 "안 변했다" 를 본다. 그런 판정은 아무것도 안
    // 재도 통과하므로, 변한 값을 넣어 실제로 우는지를 따로 본다.
    @Test
    @DisplayName("판정이_변화를_실제로_잡는다")
    void 판정이_변화를_실제로_잡는다() {
        long 문턱 = 허용_증가.toMillis();
        assertThat(판정이_안_느려졌다("가짜", 10, 10 + 문턱)).as("문턱까지는 통과")
                .isEmpty();
        assertThat(판정이_안_느려졌다("가짜", 10, 10 + 문턱 + 1))
                .hasValueSatisfying(v -> assertThat(v).contains("레디스를 친다"));

        assertThat(주입이_걸렸다(10, 10 + 지연.dividedBy(2).toMillis())).as("문턱까지는 통과")
                .isEmpty();
        assertThat(주입이_걸렸다(10, 11))
                .hasValueSatisfying(v -> assertThat(v).contains("카나리"));

        assertThat(다_뒷단까지_갔다("가짜", (long) 워밍업 + 보낼_수)).isEmpty();
        assertThat(다_뒷단까지_갔다("가짜", 워밍업 + 보낼_수 - 1L))
                .hasValueSatisfying(v -> assertThat(v).contains("뒷단에 갔다"));
    }

    /** 한 배치가 뒷단에 몇 건 닿았는지 잰다. 가짜 성공은 홉을 건너뛴다. */
    private long 잰다(Runnable 배치) {
        long 전 = 뒷단이_받은_수.get();
        배치.run();
        return 뒷단이_받은_수.get() - 전;
    }

    /**
     * <b>다 뒷단까지 갔는가.</b>
     *
     * <p>상태 코드만 보면 게이트웨이가 로컬에서 낸 가짜 200 을 못 가린다.
     * 그것도 뒷단 홉을 건너뛰므로 정상보다 빠르고, 그러면 "안 느려졌다" 로
     * 기록된다 — 서킷 폴백 회귀가 실제로 취하는 모양이다.
     */
    private Optional<String> 다_뒷단까지_갔다(String 구간, long 실제) {
        long 보낸_수 = (long) 워밍업 + 보낼_수;
        return 실제 == 보낸_수 ? Optional.empty()
                : Optional.of("%s — %d 건만 뒷단에 갔다 (보낸 %d)"
                        .formatted(구간, 실제, 보낸_수));
    }

    private void 지연을_넣는다(Duration 만큼) {
        try {
            선.느리게(만큼);
        } catch (IOException e) {
            throw new IllegalStateException("지연을 못 넣었다", e);
        }
    }

    private void 지연을_걷는다() {
        try {
            선.걷는다();
        } catch (IOException e) {
            throw new IllegalStateException("지연을 못 걷었다", e);
        }
    }

    /**
     * 주입이 실제로 걸렸는가.
     *
     * <p>이것이 없으면 "판정이 레디스를 안 친다" 와 "주입이 안 걸렸다" 를 영영
     * 못 가린다. 둘 다 발급 지연이 안 변한 그림으로 나온다.
     */
    private Optional<String> 주입이_걸렸다(long 정상_카나리, long 장애중_카나리) {
        long 늘어난_것 = 장애중_카나리 - 정상_카나리;
        return 늘어난_것 >= 허용_증가.toMillis() ? Optional.empty()
                : Optional.of("카나리가 %dms 밖에 안 느려졌다 (%dms → %dms) — 주입이 안 걸렸다"
                        .formatted(늘어난_것, 정상_카나리, 장애중_카나리));
    }

    /**
     * 지연이 실제로 걷혔는가.
     *
     * <p>발급 경로는 레디스를 안 치므로 안 걷혀도 안 느려진다. 그것만 보면
     * 복구를 안 해도 회복으로 읽힌다.
     */
    private Optional<String> 지연이_걷혔다(long 정상_카나리, long 회복_카나리) {
        long 남은_것 = 회복_카나리 - 정상_카나리;
        return 남은_것 <= 허용_증가.toMillis() ? Optional.empty()
                : Optional.of("카나리가 아직 %dms 느리다 (%dms → %dms) — 지연이 안 걷혔다"
                        .formatted(남은_것, 정상_카나리, 회복_카나리));
    }

    /**
     * 판정 지연의 증가가 주입량의 절반을 안 넘는지 본다.
     *
     * <p>배수가 아니라 절대 증가분이다. 배수로 두면 문턱이 장비 소음 대역에
     * 들어앉아 잡아야 할 신호와 무관해진다.
     */
    private Optional<String> 판정이_안_느려졌다(String 구간, long 정상, long 지금_지연) {
        long 늘어난_것 = 지금_지연 - 정상;
        return 늘어난_것 <= 허용_증가.toMillis() ? Optional.empty()
                : Optional.of("%s 판정이 %dms 느려졌다 (%dms → %dms) — 통과 경로가 레디스를 친다"
                        .formatted(구간, 늘어난_것, 정상, 지금_지연));
    }
}

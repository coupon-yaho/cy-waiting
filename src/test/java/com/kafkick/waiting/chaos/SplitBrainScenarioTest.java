package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * C5 — 리더가 갈라져 둘이 된다 (8.3.4 · 5절).
 *
 * <p>펜싱 토큰을 안 쓰는 근거의 실전 검증이다. 갈라진 동안 크레딧이 두 배로
 * 발행될 수 있지만 <b>초과 발급은 0</b> 이어야 한다 — 재고 차감이 발급 계층의
 * 원자 연산이기 때문이다. 한 건이라도 나오면 그 근거가 무너진 것이다.
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class SplitBrainScenarioTest {

    private static final String COUPON = "c5-queued";

    /**
     * 줄이 없는 쿠폰. <b>양성 대조다.</b> 줄이 선 쿠폰은 뒷단에 아예 안 가므로
     * 도착 0 이 "추월 안 함" 과 "전면 차단" 을 구분하지 못한다.
     */
    private static final String 한산한_쿠폰 = "c5-idle";

    private static final int 한산한_보낼_수 = 2;

    /** 갈라져 나간 쪽. 락을 쥐지만 우리 노드는 그것을 모른다. */
    private static final String 갈라진_리더 = "c5-partitioned";

    private static final Duration 기다림 = Duration.ofSeconds(20);

    private static final int 줄_선_사람 = 5;

    private static final int 보낼_수 = 10;

    /**
     * 강등을 확인하기까지의 한계. <b>리스(2초)보다 짧게 잡는다</b> — 리스와 같게
     * 두면 만료를 기다려 내려온 것도 통과해, 사실 기반 강등이 있으나 마나가
     * 된다. 연장은 100ms 마다 돌고 한 판이 300ms 안에 끝나므로 여유는 있다.
     */
    private static final Duration 강등_한계 = Duration.ofSeconds(1);

    private static final BackendStub 뒷단 = BackendStub.항상_받는다();

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.url", faults::주소);
    }

    @AfterAll
    static void 내린다() {
        뒷단.close();
        if (faults != null) {
            faults.close();
        }
    }

    @LocalServerPort
    private int port;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private Leadership leadership;

    @Autowired
    private SnapshotHolder holder;

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build();
    }

    private int 발급_상태(String couponId, int member) {
        return 클라이언트().post()
                .uri("/api/v1/coupons/" + couponId + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }

    private List<Integer> 여러_번_시도한다(String couponId, int 횟수, int 시작_회원) {
        List<Integer> 상태 = new ArrayList<>();
        for (int i = 0; i < 횟수; i++) {
            상태.add(발급_상태(couponId, 시작_회원 + i));
        }
        return 상태;
    }

    private long 뒷단까지_센다(Runnable 배치) {
        long 전 = 뒷단.받은_수();
        배치.run();
        return 뒷단.받은_수() - 전;
    }

    private void 재료를_심는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON, 한산한_쿠폰).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "50").block(기다림);
        redis.opsForValue().set(RedisKeys.stock(한산한_쿠폰), "100000").block(기다림);
        for (int i = 0; i < 줄_선_사람; i++) {
            redis.opsForZSet().add(RedisKeys.queue(COUPON, 1, 0), "q" + i, 100 + i)
                    .block(기다림);
        }
    }

    /** 줄에 선 사람들의 자리. 이름으로 짚어야 같은 값을 가진 둘이 안 섞인다. */
    private Map<String, Double> 자리들() {
        Map<String, Double> 자리 = new LinkedHashMap<>();
        for (int i = 0; i < 줄_선_사람; i++) {
            String member = "q" + i;
            Double score = redis.opsForZSet()
                    .score(RedisKeys.queue(COUPON, 1, 0), member).block(기다림);
            if (score != null) {
                자리.put(member, score);
            }
        }
        return 자리;
    }

    /** 판정이 멈추지 않는다. 분단은 제어 평면의 일이지 이 노드 응답의 일이 아니다. */
    private Optional<String> 판정이_멈추지_않았다(String 구간, List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("%s — 보낸 것이 없다".formatted(구간));
        }
        long 멈춘_것 = 상태.stream().filter(status -> status >= 500).count();
        return 멈춘_것 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 5xx 다 (보낸 %d)".formatted(구간, 멈춘_것, 상태.size()));
    }

    @Test
    @DisplayName("C5_리더가_갈라졌다_돌아온다")
    void C5_리더가_갈라졌다_돌아온다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        try {
            LeaderFaults 락 = LeaderFaults.of(연결);
            List<Integer> 정상_상태 = new ArrayList<>();
            List<Integer> 장애중_상태 = new ArrayList<>();
            List<Integer> 회복_상태 = new ArrayList<>();
            List<Integer> 회복_줄_상태 = new ArrayList<>();
            List<Integer> 정상_줄_상태 = new ArrayList<>();
            long[] 펜스 = new long[3];
            long[] 줄_도착 = new long[2];
            long[] 한산한_도착 = new long[3];
            List<Integer> 한산한_장애중 = new ArrayList<>();
            Map<String, Double> 장애_전_자리 = new LinkedHashMap<>();
            Map<String, Double> 회복_뒤_자리 = new LinkedHashMap<>();

            ChaosScenario.named("C5 리더 분단")
                    .baseline(() -> {
                        재료를_심는다();
                        Awaitility.await().atMost(기다림).until(leadership::isLeader);
                        // 재료가 닿아야 판정이 선다. 안 기다리면 전 구간이
                        // 거절이고 그러면 아무것도 못 잰다.
                        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                        펜스[0] = leadership.fence();
                        장애_전_자리.putAll(자리들());
                        한산한_도착[0] = 뒷단까지_센다(() -> 정상_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 1_100)));
                        정상_줄_상태.addAll(여러_번_시도한다(COUPON, 보낼_수, 1_000));
                    })
                    .inject(() -> {
                        // **갈라진 쪽이 락을 가져간다.** 우리 노드는 아직 자기가
                        // 리더라고 믿는다 — 그 창이 스플릿 브레인이다.
                        assertThat(락.죽은_리더가_넘겨받는다(갈라진_리더,
                                Duration.ofSeconds(30))).isTrue();
                    })
                    .duringFault(() -> {
                        // **강등을 기다린다.** 다음 갱신이 실패해야 이 노드가
                        // 리더가 아님을 안다. 기회를 봐서 읽으면 갱신 주기와
                        // 요청 속도의 경합이라, 빠른 판에서는 아직 리더로 읽힌다.
                        Awaitility.await().atMost(강등_한계).until(() -> !leadership.isLeader());
                        펜스[1] = leadership.fence();
                        // **분단을 낡음 너머로 끌고 간다.** 200ms 만에 걷으면
                        // 배분 틱 하나도 안 놓치고 낡음도 안 열려, 줄을 심어 둔
                        // 것이 판정에 아무 역할을 못 한다 — fail-open 이 줄을
                        // 추월하는 회귀가 그 구간에 아예 없다.
                        Awaitility.await().atMost(기다림).until(holder::isDataStale);
                        한산한_도착[1] = 뒷단까지_센다(() -> 한산한_장애중.addAll(
                                여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 2_100)));
                        줄_도착[0] = 뒷단까지_센다(() -> 장애중_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 2_000)));
                    })
                    .recover(() -> 락.lease를_만료시킨다(Duration.ofMillis(1)))
                    .afterRecovery(() -> {
                        Awaitility.await().atMost(기다림).until(leadership::isLeader);
                        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                        펜스[2] = leadership.fence();
                        한산한_도착[2] = 뒷단까지_센다(() -> 회복_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 3_100)));
                        줄_도착[1] = 뒷단까지_센다(() -> 회복_줄_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 3_000)));
                        회복_뒤_자리.putAll(자리들());
                    })
                    .assertEntry(ChaosScenario.Verdict.none())
                    .assertDuring(() -> RecoveryCriteria.violations(
                            // 양성 대조 — 한산한 쪽이 계속 통과해야 아래
                            // "추월 0" 이 무언가를 잰 것이 된다.
                            열려_있었다("유지", 한산한_도착[1], 한산한_도착[0]),
                            판정이_멈추지_않았다("유지 한산", 한산한_장애중),
                            판정이_멈추지_않았다("유지", 장애중_상태),
                            // 5xx 만 보면 전원이 429 로 거절돼도 통과한다.
                            줄에_세웠다("유지", 장애중_상태),
                            // **줄이 있으면 추월 금지** (불변식 4). 갈라진 동안
                            // 크레딧이 두 배여도 줄 선 사람을 건너뛰면 안 된다.
                            줄을_추월하지_않았다("유지", 줄_도착[0])))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            열려_있었다("회복", 한산한_도착[2], 한산한_도착[0]),
                            판정이_멈추지_않았다("회복", 회복_상태),
                            // RC5 — 유령 리더의 늦은 정리가 줄을 지우면 안 된다.
                            줄에_세웠다("회복", 회복_줄_상태),
                            RecoveryCriteria.seatLost(장애_전_자리, 회복_뒤_자리),
                            줄을_추월하지_않았다("회복", 줄_도착[1]),
                            // **틱 번호가 역행하지 않는다.** 구 리더가 물러난 뒤
                            // 다시 잡으면 번호가 앞선 값 이상이어야 한다.
                            리더십이_실제로_갈라졌다(펜스[0], 펜스[1], 펜스[2]),
                            뒷단.중복_수신이_없다()))
                    .run();

            assertThat(판정이_멈추지_않았다("정상", 정상_상태)).isEmpty();
            assertThat(줄에_세웠다("정상", 정상_줄_상태)).isEmpty();
            assertThat(한산한_도착[0]).as("전제 — 한산한 쿠폰은 평시에 뒷단까지 간다")
                    .isEqualTo(한산한_보낼_수);
        } finally {
            연결.sync().del(RedisKeys.LEADER);
            연결.close();
        }
    }

    /** 줄이 선 쿠폰에 새로 온 사람이 뒷단까지 갔다면 줄을 건너뛴 것이다. */
    /** 전면 차단이 아니다. 대조군이 막히면 아래 추월 판정이 아무것도 안 잰다. */
    private Optional<String> 열려_있었다(String 구간, long 도착, long 평시) {
        return 도착 == 평시 ? Optional.empty()
                : Optional.of("%s — 한산한 쿠폰이 %d 건만 갔다 (평시 %d)"
                        .formatted(구간, 도착, 평시));
    }

    /** 줄이 선 쿠폰이면 202 로 자리를 받아야 한다. */
    private Optional<String> 줄에_세웠다(String 구간, List<Integer> 상태) {
        long 못_선_것 = 상태.stream().filter(status -> status != 202).count();
        return 못_선_것 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 줄에 못 섰다 (보낸 %d): %s"
                        .formatted(구간, 못_선_것, 상태.size(), 상태));
    }

    private Optional<String> 줄을_추월하지_않았다(String 구간, long 도착) {
        return 도착 == 0 ? Optional.empty()
                : Optional.of("%s — 줄이 선 쿠폰에서 %d 건이 뒷단까지 갔다".formatted(구간, 도착));
    }

    /**
     * <b>분단이 실제로 걸렸는지를 펜스로 본다.</b> 갈라진 동안 이 노드는 리더가
     * 아니어야 하고(펜스 0), 돌아온 뒤 번호는 앞선 것보다 커야 한다. 같으면
     * 락을 한 번도 안 놓은 것이라 이 시나리오가 아무것도 안 겪은 것이다.
     */
    private Optional<String> 리더십이_실제로_갈라졌다(long 정상, long 장애중, long 회복) {
        if (정상 <= 0) {
            return Optional.of("전제 — 정상 구간에 리더가 아니었다");
        }
        if (장애중 != 0) {
            return Optional.of("갈라졌는데 이 노드가 아직 리더다 — 주입이 안 걸렸다");
        }
        if (회복 <= 0) {
            return Optional.of("회복 뒤에도 리더가 아니다");
        }
        return 회복 > 정상 ? Optional.empty()
                : Optional.of("펜스가 %d 에서 %d 로 안 늘었다 — 락을 안 놓았다"
                        .formatted(정상, 회복));
    }
}

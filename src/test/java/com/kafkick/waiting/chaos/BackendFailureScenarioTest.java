package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.SnapshotHolder;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * C9 — 뒷단이 5xx 를 낸다 (8.3.4 · 5절).
 *
 * <p>C8 과 갈리는 자리다. 저쪽은 응답이 아예 안 오고 여기는 오긴 오는데
 * 실패다 — 서킷이 여는 근거가 다르다. <b>줄에 선 사람의 자리는 레디스에 있으니
 * 뒷단이 아무리 망가져도 그대로여야 한다.</b>
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class BackendFailureScenarioTest {

    private static final String COUPON = "c9-queued";

    /**
     * 줄이 없는 쿠폰. <b>대조군이자 양성 대조다.</b> 줄이 선 쿠폰은 뒷단에
     * 아예 안 가므로 장애가 있든 없든 관측이 똑같다 — 그것만 보면 주입을 안
     * 해도 통과한다.
     */
    private static final String 한산한_쿠폰 = "c9-idle";

    /** 대조군에 보내는 수. 크레딧 안쪽이어야 줄이 안 선다. */
    private static final int 한산한_보낼_수 = 2;

    private static final int 줄_선_사람 = 5;

    private static final int 보낼_수 = 10;

    private static final Duration 기다림 = Duration.ofSeconds(20);

    /** 뒷단이 5xx 를 내는가. 이 스위치로 장애를 넣고 걷는다. */
    private static final AtomicBoolean 실패한다 = new AtomicBoolean();

    private static final BackendStub 뒷단 = BackendStub.실패할_수_있다(실패한다::get);

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

    /**
     * 뒷단을 직접 찔러 상태를 본다. 게이트웨이를 안 거치므로 서킷과 무관하게
     * 사실을 알 수 있다 — 주입이 정말 걸렸는지의 유일한 증거다.
     */
    private int 뒷단이_직접_답한다() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + 뒷단.port())
                .responseTimeout(Duration.ofSeconds(5))
                .build()
                .get().uri("/probe").exchange()
                .returnResult(Void.class).getStatus().value();
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

    /** 판정이 멈추지 않는다. 뒷단이 망가진 것이 이 노드 응답의 일이 아니다. */
    private Optional<String> 판정이_멈추지_않았다(String 구간, List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("%s — 보낸 것이 없다".formatted(구간));
        }
        long 멈춘_것 = 상태.stream().filter(status -> status >= 500).count();
        return 멈춘_것 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 5xx 다 (보낸 %d)".formatted(구간, 멈춘_것, 상태.size()));
    }

    @Test
    @DisplayName("C9_뒷단이_오백을_내도_순번이_남는다")
    void C9_뒷단이_오백을_내도_순번이_남는다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        try {
            List<Integer> 정상_상태 = new ArrayList<>();
            List<Integer> 장애중_상태 = new ArrayList<>();
            List<Integer> 회복_상태 = new ArrayList<>();
            Map<String, Double> 장애_전_자리 = new LinkedHashMap<>();
            Map<String, Double> 회복_뒤_자리 = new LinkedHashMap<>();
            long[] 도착 = new long[2];
            long[] 한산한_도착 = new long[3];
            int[] 뒷단_상태 = new int[3];
            List<Integer> 한산한_장애중 = new ArrayList<>();

            ChaosScenario.named("C9 뒷단 5xx")
                    .baseline(() -> {
                        재료를_심는다();
                        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                        장애_전_자리.putAll(자리들());
                        뒷단_상태[0] = 뒷단이_직접_답한다();
                        한산한_도착[0] = 뒷단까지_센다(() -> 정상_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 1_100)));
                        여러_번_시도한다(COUPON, 보낼_수, 1_000);
                    })
                    .inject(() -> 실패한다.set(true))
                    .duringFault(() -> {
                        뒷단_상태[1] = 뒷단이_직접_답한다();
                        한산한_도착[1] = 뒷단까지_센다(() -> 한산한_장애중.addAll(
                                여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 2_100)));
                        도착[0] = 뒷단까지_센다(() -> 장애중_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 2_000)));
                    })
                    .recover(() -> 실패한다.set(false))
                    .afterRecovery(() -> {
                        뒷단_상태[2] = 뒷단이_직접_답한다();
                        한산한_도착[2] = 뒷단까지_센다(() -> 회복_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 3_100)));
                        도착[1] = 뒷단까지_센다(
                                () -> 여러_번_시도한다(COUPON, 보낼_수, 3_000));
                        회복_뒤_자리.putAll(자리들());
                    })
                    .assertEntry(ChaosScenario.Verdict.none())
                    .assertDuring(() -> RecoveryCriteria.violations(
                            // **주입이 정말 걸렸는가.** 없으면 주입을 안 해도
                            // 전 판정이 통과한다 — 줄이 선 쿠폰은 뒷단에 아예
                            // 안 가므로 장애 유무로 관측이 안 갈린다.
                            뒷단이_망가졌다(뒷단_상태[0], 뒷단_상태[1]),
                            // **대조군의 통과는 여기서 안 잰다.** 뒷단이 5xx 를
                            // 내면 서킷이 열려 통과가 끊기는 것이 맞는 동작이고,
                            // 실제로 절반이 5xx 로 나갔다. 그건 C8 이 재는
                            // 자리라 여기서 걸면 같은 것을 두 번 재면서 이
                            // 시나리오의 초점을 흐린다 (CY-841).
                            판정이_멈추지_않았다("유지", 장애중_상태),
                            // **줄에 세웠는가.** 5xx 만 보면 전원이 429 로
                            // 거절돼도 통과한다 — 아무도 자리를 못 받은 판과
                            // 모두가 받은 판이 같은 초록이 된다.
                            줄에_세웠다("유지", 장애중_상태),
                            // **줄이 선 쿠폰이라 뒷단까지 가면 추월이다.**
                            줄을_추월하지_않았다("유지", 도착[0])))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            뒷단이_돌아왔다(뒷단_상태[2]),
                            판정이_멈추지_않았다("회복", 회복_상태),
                            줄에_세웠다("회복", 회복_상태),
                            줄을_추월하지_않았다("회복", 도착[1]),
                            // RC5 — 뒷단이 망가져도 자리는 레디스에 있다.
                            RecoveryCriteria.seatLost(장애_전_자리, 회복_뒤_자리),
                            뒷단.중복_수신이_없다()))
                    .run();

            assertThat(판정이_멈추지_않았다("정상", 정상_상태)).isEmpty();
            assertThat(장애_전_자리).as("전제 — 줄이 서 있었다").hasSize(줄_선_사람);
            assertThat(한산한_도착[0]).as("전제 — 한산한 쿠폰은 평시에 뒷단까지 간다")
                    .isEqualTo(한산한_보낼_수);
        } finally {
            실패한다.set(false);
            연결.close();
        }
    }

    private long 뒷단까지_센다(Runnable 배치) {
        long 전 = 뒷단.받은_수();
        배치.run();
        return 뒷단.받은_수() - 전;
    }

    /** 주입이 정말 걸렸는가. 뒷단이 직접 물어도 실패해야 장애다. */
    private Optional<String> 뒷단이_망가졌다(int 정상, int 장애중) {
        if (정상 != 200) {
            return Optional.of("전제 — 정상 구간에 뒷단이 %d 를 냈다".formatted(정상));
        }
        return 장애중 >= 500 ? Optional.empty()
                : Optional.of("장애 구간인데 뒷단이 %d 를 낸다 — 주입이 안 걸렸다"
                        .formatted(장애중));
    }

    /** 복구가 정말 됐는가. */
    private Optional<String> 뒷단이_돌아왔다(int 회복) {
        return 회복 == 200 ? Optional.empty()
                : Optional.of("회복 구간인데 뒷단이 %d 를 낸다 — 아직 안 돌아왔다"
                        .formatted(회복));
    }

    /** 줄이 선 쿠폰이면 202 로 자리를 받아야 한다. 그 밖은 못 선 것이다. */
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
}

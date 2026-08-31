package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.GatewayRegistry;
import com.kafkick.waiting.control.SnapshotHolder;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
 * C6 — 게이트웨이 노드 한 대가 빠지고 돌아온다 (8.3.4 · 5절).
 *
 * <p>남은 노드는 <b>큰 분모</b>를 쓴다. 죽은 노드의 몫까지 자기가 쓰면 총합이
 * 상한을 넘으므로, 과소 통과가 안전한 방향이다. 돌아오면 분모가 바로 반영돼야
 * 한다 — 늦으면 그 구간에 총합이 상한을 넘는다 (F5).
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class NodeLossScenarioTest {

    private static final String COUPON = "c6-idle";

    /** 이 노드 말고 심을 이웃. 하나가 빠지고 돌아오는 것을 이 위에서 본다. */
    private static final List<String> 이웃 = List.of("c6-node-a", "c6-node-b");

    private static final Duration 기다림 = Duration.ofSeconds(20);

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
    private GatewayRegistry registry;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private SnapshotHolder holder;

    /** 재료를 심는다. 안 심으면 전 구간이 거절이라 아무것도 못 잰다. */
    private void 재료를_심는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "100000").block(기다림);
    }

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build();
    }

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

    private List<Integer> 여러_번_시도한다(int 횟수, int 시작_회원) {
        List<Integer> 상태 = new ArrayList<>();
        for (int i = 0; i < 횟수; i++) {
            상태.add(발급_상태(시작_회원 + i));
        }
        return 상태;
    }

    /** 분모가 그 수가 될 때까지 기다린다. 배분 틱이 읽어 가야 반영된다. */
    private void 분모가_된다(int 기대) {
        Awaitility.await().atMost(기다림).until(() -> registry.count() == 기대);
    }

    /**
     * <b>판정이 멈추지 않는다.</b> 이웃이 빠지는 것은 이 노드의 일이 아니다 —
     * 5xx 가 하나라도 나면 남의 사정이 내 응답으로 샌 것이다.
     */
    private Optional<String> 오백이_안_샌다(String 구간, List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("%s — 보낸 것이 없다".formatted(구간));
        }
        long 샌_것 = 상태.stream().filter(status -> status >= 500).count();
        return 샌_것 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 5xx 다 (보낸 %d)"
                        .formatted(구간, 샌_것, 상태.size()));
    }

    @Test
    @DisplayName("C6_노드가_빠졌다_돌아온다")
    void C6_노드가_빠졌다_돌아온다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        try {
            GatewayNodes 노드 = new GatewayNodes(연결, Duration.ofSeconds(60));
            List<Integer> 정상_상태 = new ArrayList<>();
            List<Integer> 장애중_상태 = new ArrayList<>();
            List<Integer> 회복_상태 = new ArrayList<>();
            int[] 분모 = new int[3];

            ChaosScenario.named("C6 노드 소실")
                    .baseline(() -> {
                        재료를_심는다();
                        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                        Awaitility.await().atMost(기다림).until(() -> 발급_상태(900) == 200);
                        살아있는_이웃.addAll(이웃);
                        뛴다.scheduleAtFixedRate(
                                () -> 살아있는_이웃.forEach(노드::등록한다),
                                0, 500, TimeUnit.MILLISECONDS);
                        // 이 노드까지 셋이다. 자기 하트비트는 앱이 넣는다.
                        분모가_된다(이웃.size() + 1);
                        분모[0] = registry.count();
                        정상_상태.addAll(여러_번_시도한다(보낼_수, 1_000));
                    })
                    .inject(() -> {
                        // **뛰는 것을 멈추고 지운다.** 지우기만 하면 다음 갱신이
                        // 되살려 장애가 성립하지 않는다.
                        살아있는_이웃.remove(이웃.get(0));
                        노드.해제한다(이웃.get(0));
                    })
                    .duringFault(() -> {
                        분모가_된다(이웃.size());
                        분모[1] = registry.count();
                        장애중_상태.addAll(여러_번_시도한다(보낼_수, 2_000));
                    })
                    .recover(() -> 살아있는_이웃.add(이웃.get(0)))
                    .afterRecovery(() -> {
                        분모가_된다(이웃.size() + 1);
                        분모[2] = registry.count();
                        회복_상태.addAll(여러_번_시도한다(보낼_수, 3_000));
                    })
                    .assertEntry(ChaosScenario.Verdict.none())
                    .assertDuring(() -> RecoveryCriteria.violations(
                            오백이_안_샌다("유지", 장애중_상태),
                            분모가_줄었다(분모[0], 분모[1])))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            오백이_안_샌다("회복", 회복_상태),
                            분모가_돌아왔다(분모[0], 분모[2]),
                            뒷단.중복_수신이_없다()))
                    .run();

            assertThat(오백이_안_샌다("정상", 정상_상태)).isEmpty();
        } finally {
            뛴다.shutdownNow();
            이웃.forEach(id -> 연결.sync().hdel(GatewayNodes.KEY, id));
            연결.close();
        }
    }

    private static final int 보낼_수 = 5;

    /**
     * 살아 있는 이웃. <b>계속 뛰어야 산 것이다</b> — 한 번 심고 두면 하트비트
     * 창이 지나 같이 걷히고, 그러면 재려던 한 대가 아니라 둘이 빠진다.
     */
    private final Set<String> 살아있는_이웃 = ConcurrentHashMap.newKeySet();

    private final ScheduledExecutorService 뛴다 = Executors.newSingleThreadScheduledExecutor();

    /** 빠진 만큼 분모가 줄어야 한다. 안 줄면 죽은 노드의 몫이 허공에 남는다. */
    private Optional<String> 분모가_줄었다(int 정상, int 장애중) {
        return 장애중 == 정상 - 1 ? Optional.empty()
                : Optional.of("분모가 %d 에서 %d 로 갔다 (기대 %d)"
                        .formatted(정상, 장애중, 정상 - 1));
    }

    /** 돌아오면 바로 반영돼야 한다. 늦으면 그 구간에 총합이 상한을 넘는다 (F5). */
    private Optional<String> 분모가_돌아왔다(int 정상, int 회복) {
        return 회복 == 정상 ? Optional.empty()
                : Optional.of("분모가 %d 로 안 돌아왔다: %d".formatted(정상, 회복));
    }
}

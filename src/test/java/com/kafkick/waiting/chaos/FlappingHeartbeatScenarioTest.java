package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.WaitingApplication;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.GatewayRegistry;
import com.kafkick.waiting.control.SnapshotHolder;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import org.springframework.http.HttpMethod;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * C6d — 하트비트가 흔들린다 (CY-931).
 *
 * <p>간헐적으로 놓치는 것과 노드가 없어진 것은 다르다. 놓침으로 분모를 내리면 남은 노드가 큰 몫을 써서
 * 초과 발급 방향으로 간다 — 그래서 감소는 연속 관측 뒤에만 확정한다. 그 경계를 여기서 잰다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class FlappingHeartbeatScenarioTest {

    private static final String COUPON = "c6d-idle";

    private static final Duration 기다림 = Duration.ofSeconds(30);

    private static final Duration 분모_한계 = Duration.ofSeconds(20);

    private static final int 보낼_수 = 12;

    private static final BackendStub 뒷단 = BackendStub.항상_받는다();

    private static RedisWireFaults 선;

    private static RedisWireFaults.Gate 둘째_문;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        선 = RedisWireFaults.시작한다();
        둘째_문 = 선.문을_하나_더();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.url", 선::주소);
    }

    @AfterAll
    static void 내린다() {
        뒷단.close();
        if (선 != null) {
            선.close();
        }
    }

    @LocalServerPort
    private int port;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private SnapshotHolder holder;

    @Autowired
    private GatewayRegistry 노드_수;

    /**
     * <b>흔들리는 동안에는 분모를 안 내린다</b> (CY-931). 하트비트를 간헐적으로 놓치는 것과 노드가 없어진
     * 것은 다르다 — 놓침으로 분모를 내리면 남은 노드가 큰 몫을 써서 초과 발급 방향으로 간다.
     */
    @Test
    @DisplayName("C6d_하트비트가_흔들리는_동안은_분모를_안_내린다")
    void C6d_하트비트가_흔들리는_동안은_분모를_안_내린다() throws Exception {
        StatefulRedisConnection<String, String> 연결 = 선.연결한다();
        try (SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, 둘째_문.주소(),
                "http://localhost:" + 뒷단.port(), false)) {
            int[] 정상_분모 = new int[1];
            int[] 흔들리는_동안_최소 = {Integer.MAX_VALUE};
            boolean[] 멎은_뒤_내려왔다 = new boolean[1];
            long[] 크레딧 = new long[1];
            long[] 도착 = new long[1];
            long[] 걸린 = new long[1];
            List<Integer> 흔들리는_동안 = new ArrayList<>();

            ChaosScenario.named("C6d 하트비트 흔들림")
                    .baseline(() -> {
                        재료를_심는다();
                        BackendReports 보고기 = BackendReports.실시계로(연결, Duration.ofSeconds(30));
                        보고기.보고한다("c6d-be", 1_000);
                        Awaitility.await().alias("첫 스냅샷이 닿는다").atMost(기다림)
                                .until(() -> !holder.isDataStale());
                        Awaitility.await().alias("둘째가 등록된다").atMost(기다림)
                                .until(() -> 노드_수.count() >= 2);
                        정상_분모[0] = 노드_수.count();
                        크레딧[0] = holder.view().snapshot().meta().globalCredit();
                    })
                    .inject(() -> 끊는다(둘째_문))
                    .duringFault(() -> {
                        // **끊었다 붙였다를 되풀이한다.** 하트비트가 가끔 닿으면 관측이 끊겨, 감소를
                        // 확정하는 연속 셈이 매번 0 으로 돌아가야 한다.
                        for (int i = 0; i < 6; i++) {
                            흔들리는_동안_최소[0] = Math.min(흔들리는_동안_최소[0], 노드_수.count());
                            쉰다(700);
                            걷는다(둘째_문);
                            쉰다(700);
                            흔들리는_동안_최소[0] = Math.min(흔들리는_동안_최소[0], 노드_수.count());
                            끊는다(둘째_문);
                        }
                        long 시작 = System.nanoTime();
                        도착[0] = 뒷단까지_센다(() -> {
                            흔들리는_동안.addAll(여러_번_시도한다(port, 보낼_수, 5_000));
                            흔들리는_동안.addAll(여러_번_시도한다(둘째.port(), 보낼_수, 6_000));
                        });
                        걸린[0] = System.nanoTime() - 시작;
                        // **멎은 뒤에는 내려와야 한다.** 흔들림과 소실을 같은 값으로 두면 죽은 노드가
                        // 영영 분모에 남아 전 노드가 작은 몫을 쓴다.
                        try {
                            Awaitility.await().alias("멎은 뒤 감소가 확정된다")
                                    .atMost(분모_한계).pollInterval(Duration.ofMillis(200))
                                    .until(() -> 노드_수.count() < 정상_분모[0]);
                            멎은_뒤_내려왔다[0] = true;
                        } catch (RuntimeException e) {
                            멎은_뒤_내려왔다[0] = false;
                        }
                    })
                    .recover(() -> 걷는다(둘째_문))
                    .afterRecovery(() -> Awaitility.await().alias("분모가 돌아온다")
                            .atMost(분모_한계).pollInterval(Duration.ofMillis(200))
                            .until(() -> 노드_수.count() >= 정상_분모[0]))
                    .assertEntry(() -> RecoveryCriteria.violations(
                            정상_분모[0] >= 2 ? Optional.empty()
                                    : Optional.of("전제 — 분모가 2 가 아니다: %d".formatted(정상_분모[0]))))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            흔들리는_동안_최소[0] >= 정상_분모[0] ? Optional.empty()
                                    : Optional.of("흔들리는 동안 분모가 내려갔다 — 최소 %d"
                                            .formatted(흔들리는_동안_최소[0])),
                            멎은_뒤_내려왔다[0] ? Optional.empty()
                                    : Optional.of("멎은 뒤에도 분모가 그대로다 — %d"
                                            .formatted(노드_수.count())),
                            전면_차단이_아니다("흔들리는 동안", 흔들리는_동안),
                            도착[0] <= 허용(크레딧[0], 걸린[0]) ? Optional.empty()
                                    : Optional.of("흔들리는 창에서 뒷단에 %d 건 닿았다 — 허용 %d (크레딧 %d)"
                                            .formatted(도착[0], 허용(크레딧[0], 걸린[0]), 크레딧[0]))))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            노드_수.count() >= 정상_분모[0] ? Optional.empty()
                                    : Optional.of("걷었는데 분모가 안 돌아왔다 — %d"
                                            .formatted(노드_수.count()))))
                    .run();
        } finally {
            연결.close();
        }
    }

    /**
     * 그 창에서 허용되는 최대 도착. <b>크레딧은 초당 예산이다</b> — 창이 길면 그만큼 버킷이 열리므로,
     * 창 길이로 안 나누면 정상 유입도 위반으로 읽는다. 초 경계를 걸칠 수 있어 한 칸을 더 준다.
     */
    private long 허용(long 크레딧, long 걸린_나노) {
        long 초 = Math.max(1, (걸린_나노 + 999_999_999L) / 1_000_000_000L);
        return 크레딧 * (초 + 1);
    }

    /** 흔들림의 간격. 벽시계로 둔다 — 재는 것이 실제 하트비트와 관측의 경합이다. */
    private void 쉰다(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("흔들림이 끊겼다", e);
        }
    }

    /** 검사 예외를 여기서 받는다. 시나리오 단계는 던지는 자리가 아니다. */
    private void 끊는다(RedisWireFaults.Gate 문) {
        try {
            문.끊는다();
        } catch (IOException e) {
            throw new IllegalStateException("문을 못 끊었다", e);
        }
    }

    private void 걷는다(RedisWireFaults.Gate 문) {
        try {
            문.걷는다();
        } catch (IOException e) {
            throw new IllegalStateException("문을 못 걷었다", e);
        }
    }

    /** 전원이 5xx 는 아닌가. 끊긴 노드도 판정은 내야 한다 — 전부 막히면 그 구간이 통째로 안 보인다. */
    private Optional<String> 전면_차단이_아니다(String 이름, List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("%s — 보낸 것이 없다".formatted(이름));
        }
        long 답한_것 = 상태.stream().filter(status -> status < 500).count();
        return 답한_것 > 0 ? Optional.empty()
                : Optional.of("%s — 전원이 5xx 다 (보낸 %d)".formatted(이름, 상태.size()));
    }

    /** 5xx 가 섞였는가. 끊긴 노드도 판정은 내야 한다 — 못 내면 그 구간이 통째로 안 보인다. */
    private Optional<String> 멎지_않았다(String 이름, List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("%s — 보낸 것이 없다".formatted(이름));
        }
        long 멎은_것 = 상태.stream().filter(status -> status >= 500).count();
        return 멎은_것 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 5xx 다 (보낸 %d)"
                        .formatted(이름, 멎은_것, 상태.size()));
    }

    private void 재료를_심는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "100000").block(기다림);
    }

    private long 뒷단까지_센다(Runnable 배치) {
        long 전 = 뒷단.받은_수(COUPON);
        배치.run();
        return 뒷단.받은_수(COUPON) - 전;
    }

    private List<Integer> 여러_번_시도한다(int 노드_포트, int 횟수, int 시작_회원) {
        List<Integer> 상태 = new ArrayList<>();
        for (int i = 0; i < 횟수; i++) {
            상태.add(발급_상태(노드_포트, 시작_회원 + i));
        }
        return 상태;
    }

    private int 발급_상태(int 노드_포트, int member) {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + 노드_포트)
                .responseTimeout(Duration.ofSeconds(10))
                .build()
                .method(HttpMethod.POST)
                .uri("/api/v1/coupons/" + COUPON + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }
}

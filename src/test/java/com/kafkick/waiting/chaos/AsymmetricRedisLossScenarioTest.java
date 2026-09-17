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
 * C6c — 리더는 정상인데 동료만 레디스를 못 쓴다 (CY-931).
 *
 * <p>전 노드가 같이 끊기는 판은 분모가 안 움직인다 — 관측 실패는 직전 값을 지키기 때문이다. 한쪽만 끊기면
 * 다르다: 리더의 하트비트 스크립트가 살아 있는 노드를 적게 세고, 그 값이 연속으로 오면 분모가 내려간다.
 * 그동안 끊긴 동료는 낡은 재료로 <b>제 몫을 계속 통과시킨다</b> — 두 값이 겹치는 창이 여기서만 생긴다.
 *
 * <p>재는 것은 하나다. <b>그 창에서 뒷단에 닿은 총합이 전역 크레딧을 안 넘는다</b> (불변식 2의 앞단).
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class AsymmetricRedisLossScenarioTest {

    private static final String COUPON = "c6c-idle";

    private static final Duration 기다림 = Duration.ofSeconds(30);

    /** 분모가 내려오기를 기다리는 한계. 감소는 연속 관측 뒤라 틱 여럿이 든다. */
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

    @Test
    @DisplayName("C6c_동료만_레디스를_못_써도_총합이_크레딧을_안_넘는다")
    void C6c_동료만_레디스를_못_써도_총합이_크레딧을_안_넘는다() throws Exception {
        StatefulRedisConnection<String, String> 연결 = 선.연결한다();
        try (SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, 둘째_문.주소(),
                "http://localhost:" + 뒷단.port(), false)) {
            List<Integer> 정상_상태 = new ArrayList<>();
            List<Integer> 끊긴_뒤_첫_노드 = new ArrayList<>();
            List<Integer> 끊긴_뒤_둘째 = new ArrayList<>();
            int[] 정상_분모 = new int[1];
            int[] 내려간_분모 = new int[1];
            long[] 크레딧 = new long[1];
            long[] 도착 = new long[2];
            long[] 걸린 = new long[1];
            boolean[] 분모가_내려왔다 = new boolean[1];

            ChaosScenario.named("C6c 비대칭 레디스 소실")
                    .baseline(() -> {
                        재료를_심는다();
                        BackendReports 보고기 = BackendReports.실시계로(연결, Duration.ofSeconds(30));
                        보고기.보고한다("c6c-be", 1_000);
                        Awaitility.await().alias("첫 스냅샷이 닿는다").atMost(기다림)
                                .until(() -> !holder.isDataStale());
                        // 둘이 다 등록돼야 분모가 2 다. 아니면 이 시나리오가 재는 창이 안 생긴다.
                        Awaitility.await().alias("둘째가 등록된다").atMost(기다림)
                                .until(() -> 노드_수.count() >= 2);
                        정상_분모[0] = 노드_수.count();
                        크레딧[0] = holder.view().snapshot().meta().globalCredit();
                        도착[0] = 뒷단까지_센다(() -> {
                            정상_상태.addAll(여러_번_시도한다(port, 보낼_수, 1_000));
                            정상_상태.addAll(여러_번_시도한다(둘째.port(), 보낼_수, 2_000));
                        });
                    })
                    .inject(() -> 끊는다(둘째_문))
                    .duringFault(() -> {
                        // **분모가 실제로 내려와야 이 시나리오가 무언가를 잰다.** 안 내려오면
                        // 두 노드가 같은 분모를 쓰는 평시와 다르지 않다.
                        try {
                            Awaitility.await().alias("리더가 적게 센 값을 확정한다")
                                    .atMost(분모_한계).pollInterval(Duration.ofMillis(200))
                                    .until(() -> 노드_수.count() < 정상_분모[0]);
                            분모가_내려왔다[0] = true;
                        } catch (RuntimeException e) {
                            분모가_내려왔다[0] = false;
                        }
                        내려간_분모[0] = 노드_수.count();
                        long 시작 = System.nanoTime();
                        도착[1] = 뒷단까지_센다(() -> {
                            끊긴_뒤_첫_노드.addAll(여러_번_시도한다(port, 보낼_수, 3_000));
                            끊긴_뒤_둘째.addAll(여러_번_시도한다(둘째.port(), 보낼_수, 4_000));
                        });
                        걸린[0] = System.nanoTime() - 시작;
                    })
                    .recover(() -> 걷는다(둘째_문))
                    .afterRecovery(() -> Awaitility.await().alias("분모가 돌아온다")
                            .atMost(분모_한계).pollInterval(Duration.ofMillis(200))
                            .until(() -> 노드_수.count() >= 정상_분모[0]))
                    .assertEntry(() -> RecoveryCriteria.violations(
                            정상_분모[0] >= 2 ? Optional.empty()
                                    : Optional.of("전제 — 분모가 2 가 아니다: %d".formatted(정상_분모[0])),
                            도착[0] > 0 ? Optional.empty()
                                    : Optional.of("전제 — 평시에 뒷단까지 간 요청이 없다")))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            분모가_내려왔다[0] ? Optional.empty()
                                    : Optional.of("한쪽만 끊었는데 분모가 그대로다 — %d"
                                            .formatted(내려간_분모[0])),
                            // **끊긴 노드는 전면 차단만 아니면 된다.** 재료가 낡으면 fail-open 상한이
                            // 걸려 일부가 503 으로 나가는 것이 맞는 동작이다 — 상한이 없는 쪽이 사고다.
                            전면_차단이_아니다("끊긴 노드", 끊긴_뒤_둘째),
                            // **붙어 있는 노드는 멎지 않는다.** 여기서 5xx 가 나오면 한쪽 장애가
                            // 멀쩡한 노드로 번진 것이다.
                            멎지_않았다("첫 노드", 끊긴_뒤_첫_노드),
                            // **두 값이 겹치는 창의 총합.** 리더는 작은 분모로 제 몫을 키우고,
                            // 끊긴 동료는 낡은 재료로 옛 몫을 계속 쓴다.
                            도착[1] <= 허용(크레딧[0], 걸린[0]) ? Optional.empty()
                                    : Optional.of("끊긴 창에서 뒷단에 %d 건 닿았다 — 허용 %d (크레딧 %d)"
                                            .formatted(도착[1], 허용(크레딧[0], 걸린[0]), 크레딧[0]))))
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

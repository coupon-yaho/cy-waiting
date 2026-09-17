package com.kafkick.waiting.chaos;


import com.kafkick.waiting.WaitingApplication;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.GatewayRegistry;
import com.kafkick.waiting.control.SnapshotHolder;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
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

    /** 예산 판정에 쓸 발신 수. 허용보다 많이 보내야 그 부등식이 무언가를 잰다. */
    private static final int 부하_수 = 90;

    /** 뒷단이 보고할 여유. 크게 잡으면 허용이 발신보다 훨씬 커서 총합 초과를 못 본다. */
    private static final int 가용량 = 20;

    /**
     * 끊어 두는 창. <b>보고 신선도(3초)보다 길어야 한다</b> — 짧으면 동료의 마지막 하트비트가 늘 신선해
     * 리더가 적게 세는 관측을 한 번도 안 한다. 그러면 연속 셈이 0 으로 돌아가는지를 아예 안 밟는다.
     */
    private static final long 끊는_창_밀리 = 4_200;

    /**
     * 붙여 두는 창. <b>재연결과 하트비트 한 번이 확실히 들어가야 한다</b> — 짧으면 적게 센 관측이
     * 창을 건너 이어져, 흔들림인데도 연속 셈이 차고 감소가 확정된다. 4.2초/1.5초로 재 보니 세 번째
     * 창에서 1 로 떨어졌고, 3.5초는 동료가 낡음에 안 들어가 전제가 깨졌다. 끊는 창은 낡음 문턱
     * 위여야 하고, 붙이는 창은 연속 셈이 창을 건너 이어지지 않을 만큼이어야 한다.
     */
    private static final long 붙이는_창_밀리 = 2_500;

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
        String[] 원인 = new String[1];
        AtomicInteger 뛰다_터진_수 = new AtomicInteger();
        ScheduledExecutorService 보고 = Executors.newSingleThreadScheduledExecutor();
        try (SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, 둘째_문.주소(),
                "http://localhost:" + 뒷단.port(), false)) {
            int[] 정상_분모 = new int[1];
            int[] 흔들리는_동안_최소 = {Integer.MAX_VALUE};
            boolean[] 멎은_뒤_내려왔다 = new boolean[1];
            long[] 크레딧 = new long[1];
            long[] 도착 = new long[1];
            long[] 걸린 = new long[1];
            List<Integer> 흔들리는_동안_첫_노드 = new ArrayList<>();
            List<Integer> 흔들리는_동안_둘째 = new ArrayList<>();
            long[] 못_읽은_시간 = new long[1];
            int[] 붙인_뒤_분모 = new int[3];
            List<Integer> 관측열 = Collections.synchronizedList(new ArrayList<>());
            SnapshotHolder 둘째_홀더 = 둘째.빈("snapshotHolder", SnapshotHolder.class);

            ChaosScenario.named("C6d 하트비트 흔들림")
                    .baseline(() -> {
                        재료를_심는다();
                        // **다시 심는다.** 신선도가 3초라 한 번만 심으면 유지 구간의 크레딧이
                        // 바닥값이고, 그러면 예산 판정이 통째로 도달 불가다.
                        BackendReports 보고기 = BackendReports.실시계로(연결, Duration.ofSeconds(3));
                        보고.scheduleAtFixedRate(() -> {
                            try {
                                보고기.보고한다("c6d-be", 가용량);
                            } catch (RuntimeException e) {
                                뛰다_터진_수.incrementAndGet();
                            }
                        }, 0, 500, TimeUnit.MILLISECONDS);
                        Awaitility.await().alias("첫 스냅샷이 닿는다").atMost(기다림)
                                .until(() -> !holder.isDataStale());
                        Awaitility.await().alias("둘째가 등록된다").atMost(기다림)
                                .until(() -> 노드_수.count() >= 2);
                        정상_분모[0] = 노드_수.count();
                    })
                    .inject(() -> 끊는다(둘째_문))
                    .duringFault(() -> {
                        // **끊었다 붙였다를 되풀이한다.** 하트비트가 가끔 닿으면 관측이 끊겨, 감소를
                        // 확정하는 연속 셈이 매번 0 으로 돌아가야 한다.
                        // **최소값은 이어서 본다.** 두 점만 찍으면 값이 떨어질 가능성이 가장 큰
                        // 순간(끊긴 창의 끝)을 건너뛰고, 증가는 즉시라 그 하강이 지워진다.
                        ScheduledExecutorService 표집 = Executors.newSingleThreadScheduledExecutor();
                        표집.scheduleAtFixedRate(() -> {
                            int 지금_값 = 노드_수.count();
                            흔들리는_동안_최소[0] = Math.min(흔들리는_동안_최소[0], 지금_값);
                            관측열.add(지금_값);
                        }, 0, 100, TimeUnit.MILLISECONDS);
                        try {
                            for (int i = 0; i < 3; i++) {
                                쉰다(끊는_창_밀리);
                                // **전제 — 끊김이 실제로 문다.** 동료가 레디스를 계속 읽고 있으면
                                // 흔들림이 흔들림이 아니고, 아래 판정은 잴 것이 없다.
                                못_읽은_시간[0] = Math.max(못_읽은_시간[0],
                                        둘째_홀더.fetchAge().toMillis());
                                걷는다(둘째_문);
                                쉰다(붙이는_창_밀리);
                                // **붙인 뒤에는 돌아와 있어야 한다.** 증가는 즉시라, 안 돌아오면
                                // 흔들리는 노드가 영영 분모 밖에 남는다는 뜻이다.
                                붙인_뒤_분모[i] = 노드_수.count();
                                끊는다(둘째_문);
                            }
                            걷는다(둘째_문);
                            쉰다(붙이는_창_밀리);

                            크레딧[0] = holder.view().snapshot().meta().globalCredit();
                            long 시작 = System.nanoTime();
                            도착[0] = 뒷단까지_센다(() -> {
                                흔들리는_동안_첫_노드.addAll(여러_번_시도한다(port, 보낼_수, 5_000));
                                흔들리는_동안_둘째.addAll(여러_번_시도한다(둘째.port(), 보낼_수, 6_000));
                                함께_시도한다(port, 부하_수, 50_000);
                                함께_시도한다(둘째.port(), 부하_수, 60_000);
                            });
                            걸린[0] = System.nanoTime() - 시작;
                        } finally {
                            표집.shutdownNow();
                        }
                        끊는다(둘째_문);
                        // **멎은 뒤에는 내려와야 한다.** 흔들림과 소실을 같은 값으로 두면 죽은 노드가
                        // 영영 분모에 남아 전 노드가 작은 몫을 쓴다.
                        try {
                            Awaitility.await().alias("멎은 뒤 감소가 확정된다")
                                    .atMost(분모_한계).pollInterval(Duration.ofMillis(200))
                                    .until(() -> 노드_수.count() < 정상_분모[0]);
                            멎은_뒤_내려왔다[0] = true;
                        } catch (ConditionTimeoutException e) {
                            // **시한 초과만 판정으로 바꾼다.** 레디스 오류까지 같이 삼키면
                            // 원인이 다른 실패가 "안 내려왔다" 한 문장으로 뭉개진다.
                            멎은_뒤_내려왔다[0] = false;
                            원인[0] = e.getMessage();
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
                            // **흔들림이 분모를 영구히 깎지 않는다.** 끊긴 창이 낡음 문턱을 넘으면
                            // 감소가 확정되는 것이 맞는 동작이다 — 그 아래는 제품이 아예 안 본다.
                            // 그래서 재는 것은 "한 번도 안 내려갔나" 가 아니라 "붙일 때마다 돌아오나" 다.
                            // 연속 셈이 0 으로 돌아가는 것 자체는 GatewayRegistryTest 가 결정론으로 문다.
                            돌아왔다(붙인_뒤_분모, 정상_분모[0], 눌러_적는다(관측열)),
                            못_읽은_시간[0] >= 끊는_창_밀리 / 2 ? Optional.empty()
                                    : Optional.of("전제 — 끊었는데 동료가 계속 읽었다 — 최대 %dms"
                                            .formatted(못_읽은_시간[0])),
                            멎은_뒤_내려왔다[0] ? Optional.empty()
                                    : Optional.of("멎은 뒤에도 분모가 그대로다 — %d (%s)"
                                            .formatted(노드_수.count(), 원인[0])),
                            // **끊긴 노드는 전면 차단만 아니면 된다.** 낡은 재료면 상한이 걸려 일부가
                            // 503 인 것이 맞는 동작이다.
                            NodeIssueProbe.전면_차단이_아니다("흔들리는 노드", 흔들리는_동안_둘째),
                            // **붙어 있는 노드는 멎지 않는다.** 둘을 한 목록에 합치면 멀쩡한 노드가
                            // 전부 5xx 여도 끊긴 쪽 한 건으로 초록이 된다.
                            NodeIssueProbe.멎지_않았다("첫 노드", 흔들리는_동안_첫_노드),
                            뛰다_터진_수.get() == 0 ? Optional.empty()
                                    : Optional.of("보고가 %d 번 터졌다 — 크레딧이 바닥값일 수 있다"
                                            .formatted(뛰다_터진_수.get())),
                            도착[0] <= NodeIssueProbe.허용(크레딧[0], 걸린[0]) ? Optional.empty()
                                    : Optional.of("흔들리는 창에서 뒷단에 %d 건 닿았다 — 허용 %d (크레딧 %d)"
                                            .formatted(도착[0], NodeIssueProbe.허용(크레딧[0], 걸린[0]), 크레딧[0]))))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            노드_수.count() >= 정상_분모[0] ? Optional.empty()
                                    : Optional.of("걷었는데 분모가 안 돌아왔다 — %d"
                                            .formatted(노드_수.count()))))
                    .run();
        } finally {
            보고.shutdownNow();
            연결.close();
        }
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


    /** 5xx 가 섞였는가. 끊긴 노드도 판정은 내야 한다 — 못 내면 그 구간이 통째로 안 보인다. */

    private void 재료를_심는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "100000").block(기다림);
    }

    private long 뒷단까지_센다(Runnable 배치) {
        long 전 = 뒷단.받은_수(COUPON);
        배치.run();
        return 뒷단.받은_수(COUPON) - 전;
    }

    /**
     * 한 노드를 <b>함께</b> 두드린다. 순차로 보내면 초당 도착이 응답 시간에 갇혀, 예산을 넘겨 보려는
     * 발신이 예산 근처에도 못 간다 — 그러면 총합 판정이 제품이 아니라 하네스의 속도를 잰다.
     */
    private void 함께_시도한다(int 노드_포트, int 횟수, int 시작_회원) {
        ExecutorService 일꾼 = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> 맡긴_것 = new ArrayList<>();
            for (int i = 0; i < 횟수; i++) {
                int member = 시작_회원 + i;
                맡긴_것.add(일꾼.submit(() -> 발급_상태(노드_포트, member)));
            }
            for (Future<?> 하나 : 맡긴_것) {
                하나.get(30, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("발신이 끊겼다", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("발신이 안 끝났다", e);
        } finally {
            일꾼.shutdownNow();
        }
    }

    /** 붙일 때마다 분모가 돌아왔는가. 하나라도 안 돌아왔으면 흔들림이 소실로 굳은 것이다. */
    private Optional<String> 돌아왔다(int[] 붙인_뒤_분모, int 정상, String 관측) {
        for (int i = 0; i < 붙인_뒤_분모.length; i++) {
            if (붙인_뒤_분모[i] < 정상) {
                return Optional.of("%d 번째로 붙인 뒤 분모가 %d 다 (정상 %d) · 관측 %s"
                        .formatted(i + 1, 붙인_뒤_분모[i], 정상, 관측));
            }
        }
        return Optional.empty();
    }

    /** 관측 열을 값:길이 로 눌러 적는다. 100ms 표집이라 그대로 찍으면 수백 줄이다. */
    private String 눌러_적는다(List<Integer> 관측열) {
        StringBuilder 적은_것 = new StringBuilder();
        int 앞 = Integer.MIN_VALUE;
        int 길이 = 0;
        for (int 값 : List.copyOf(관측열)) {
            if (값 == 앞) {
                길이++;
                continue;
            }
            if (앞 != Integer.MIN_VALUE) {
                적은_것.append(앞).append(':').append(길이).append(' ');
            }
            앞 = 값;
            길이 = 1;
        }
        return 적은_것.append(앞).append(':').append(길이).toString();
    }

    /** 한 노드를 여러 번 두드린다. 한 건만 보면 그 한 건이 어느 쪽이었는지에 판정이 걸린다. */
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

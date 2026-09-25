package com.kafkick.waiting.chaos;


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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
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

    /** 상태를 보는 표본 수. 어느 노드가 멎었는지는 적은 수로도 보인다. */
    private static final int 보낼_수 = 12;

    /**
     * 예산 판정에 쓸 발신 수. <b>허용보다 많이 보내야 판정이 무언가를 잰다</b> — 적게 보내면 도착이
     * 보낸 수에 갇혀, 두 노드가 각각 예산 전량을 열어도 부등식이 정의상 성립한다.
     */
    private static final int 부하_수 = 90;

    /**
     * 뒷단이 보고할 여유. <b>작게 잡는다</b> — 크게 잡으면 허용이 보낼 수보다 훨씬 커서, 총합 초과가
     * 실제로 일어나도 판정이 못 본다.
     */
    private static final int 가용량 = 20;

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
        String[] 원인 = new String[1];
        AtomicInteger 뛰다_터진_수 = new AtomicInteger();
        ScheduledExecutorService 보고 = Executors.newSingleThreadScheduledExecutor();
        try (SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, 둘째_문.주소(),
                "http://localhost:" + 뒷단.port(), false)) {
            List<Integer> 정상_상태 = new ArrayList<>();
            List<Integer> 끊긴_뒤_첫_노드 = new ArrayList<>();
            List<Integer> 끊긴_뒤_둘째 = new ArrayList<>();
            int[] 정상_분모 = new int[1];
            int[] 내려간_분모 = new int[1];
            long[] 크레딧 = new long[1];
            long[] 도착 = new long[3];
            List<Integer> 걷은_뒤_첫_노드 = new ArrayList<>();
            List<Integer> 걷은_뒤_둘째 = new ArrayList<>();
            long[] 걸린 = new long[1];
            boolean[] 분모가_내려왔다 = new boolean[1];
            long[] 못_읽은_시간 = new long[1];
            SnapshotHolder 둘째_홀더 = 둘째.빈("snapshotHolder", SnapshotHolder.class);

            ChaosScenario.named("C6c 비대칭 레디스 소실")
                    .baseline(() -> {
                        재료를_심는다();
                        // **다시 심는다.** 보고 신선도가 3초라 한 번만 심으면 유지 구간에 들어갈 때
                        // 크레딧이 이미 바닥값이고, 그러면 예산 판정이 통째로 도달 불가다 (C6 와 같다).
                        BackendReports 보고기 = BackendReports.실시계로(연결, Duration.ofSeconds(3));
                        보고.scheduleAtFixedRate(() -> {
                            try {
                                보고기.보고한다("c6c-be", 가용량);
                            } catch (RuntimeException e) {
                                뛰다_터진_수.incrementAndGet();
                            }
                        }, 0, 500, TimeUnit.MILLISECONDS);
                        Awaitility.await().alias("첫 스냅샷이 닿는다").atMost(기다림)
                                .until(() -> !holder.isDataStale());
                        // 둘이 다 등록돼야 분모가 2 다. 아니면 이 시나리오가 재는 창이 안 생긴다.
                        Awaitility.await().alias("둘째가 등록된다").atMost(기다림)
                                .until(() -> 노드_수.count() >= 2);
                        정상_분모[0] = 노드_수.count();
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
                        } catch (ConditionTimeoutException e) {
                            // **시한 초과만 판정으로 바꾼다.** 레디스 오류까지 같이 삼키면
                            // 원인이 다른 실패가 "안 내려왔다" 한 문장으로 뭉개진다.
                            분모가_내려왔다[0] = false;
                            원인[0] = e.getMessage();
                        }
                        내려간_분모[0] = 노드_수.count();
                        // **전제 — 끊김이 실제로 문다.** 동료가 레디스를 계속 읽고 있으면 "두 값이
                        // 겹치는 창" 이 애초에 안 생기고, 아래 총합 판정은 잴 것이 없다. 낡음 플래그가
                        // 아니라 못 읽은 시간을 본다 — 낡음 문턱은 재료 나이라 창 길이와 눈금이 다르다.
                        못_읽은_시간[0] = 둘째_홀더.fetchAge().toMillis();
                        // **여기서 읽는다.** 기준선에서 읽으면 분모가 내려오기를 기다린 시간만큼
                        // 관측 시점과 판정 시점이 어긋난다.
                        크레딧[0] = holder.view().snapshot().meta().globalCredit();
                        long 시작 = System.nanoTime();
                        도착[1] = 뒷단까지_센다(() -> {
                            끊긴_뒤_첫_노드.addAll(여러_번_시도한다(port, 보낼_수, 3_000));
                            끊긴_뒤_둘째.addAll(여러_번_시도한다(둘째.port(), 보낼_수, 4_000));
                            // 예산을 넘겨 본다. 통과 상한이 실제로 걸리는지는 이 발신이 정한다.
                            함께_시도한다(port, 부하_수, 30_000);
                            함께_시도한다(둘째.port(), 부하_수, 40_000);
                        });
                        걸린[0] = System.nanoTime() - 시작;
                    })
                    .recover(() -> 걷는다(둘째_문))
                    .afterRecovery(() -> {
                        Awaitility.await().alias("분모가 돌아온다")
                                .atMost(분모_한계).pollInterval(Duration.ofMillis(200))
                                .until(() -> 노드_수.count() >= 정상_분모[0]);
                        // **걷은 뒤에도 두드린다.** 숫자 하나만 보면 장애는 견뎠는데 복구가 안 된
                        // 상태를 못 가른다 — 되살아난 노드가 계속 5xx 를 내도 분모는 돌아온다.
                        도착[2] = 뒷단까지_센다(() -> {
                            걷은_뒤_첫_노드.addAll(여러_번_시도한다(port, 보낼_수, 7_000));
                            걷은_뒤_둘째.addAll(여러_번_시도한다(둘째.port(), 보낼_수, 8_000));
                        });
                    })
                    .assertEntry(() -> RecoveryCriteria.violations(
                            정상_분모[0] >= 2 ? Optional.empty()
                                    : Optional.of("전제 — 분모가 2 가 아니다: %d".formatted(정상_분모[0])),
                            도착[0] > 0 ? Optional.empty()
                                    : Optional.of("전제 — 평시에 뒷단까지 간 요청이 없다")))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            분모가_내려왔다[0] ? Optional.empty()
                                    : Optional.of("한쪽만 끊었는데 분모가 그대로다 — %d (%s)"
                                            .formatted(내려간_분모[0], 원인[0])),
                            // **보고가 끊기면 크레딧이 바닥값으로 접힌다.** 그러면 예산 판정이
                            // 통과하는 이유가 제품이 아니라 하네스가 멈춘 것이 된다.
                            뛰다_터진_수.get() == 0 ? Optional.empty()
                                    : Optional.of("보고가 %d 번 터졌다 — 크레딧이 바닥값일 수 있다"
                                            .formatted(뛰다_터진_수.get())),
                            // **끊긴 노드는 열지 않는다** (CY-1006). 재료가 낡아 줄을 모르고 레디스도
                            // 못 써 줄에 못 세운다 — 열면 동료 노드가 세운 줄을 앞지른다.
                            못_읽은_시간[0] >= 2_000 ? Optional.empty()
                                    : Optional.of("전제 — 끊었는데 동료가 계속 읽었다 — %dms"
                                            .formatted(못_읽은_시간[0])),
                            NodeIssueProbe.되돌려_보냈다("끊긴 노드", 끊긴_뒤_둘째),
                            // **붙어 있는 노드는 멎지 않는다.** 여기서 5xx 가 나오면 한쪽 장애가
                            // 멀쩡한 노드로 번진 것이다.
                            NodeIssueProbe.멎지_않았다("첫 노드", 끊긴_뒤_첫_노드),
                            // **두 값이 겹치는 창의 총합.** 리더는 작은 분모로 제 몫을 키우고,
                            // 끊긴 동료는 적응형을 안 열지만 꺼진 쿠폰은 낡은 재료의 옛 몫으로 연다.
                            도착[1] <= NodeIssueProbe.허용(크레딧[0], 걸린[0]) ? Optional.empty()
                                    : Optional.of("끊긴 창에서 뒷단에 %d 건 닿았다 — 허용 %d (크레딧 %d)"
                                            .formatted(도착[1], NodeIssueProbe.허용(크레딧[0], 걸린[0]), 크레딧[0]))))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            NodeIssueProbe.멎지_않았다("걷은 뒤 첫 노드", 걷은_뒤_첫_노드),
                            // **뒷단 도착을 요구하지 않는다.** 유지 창에서 예산을 넘겨 봤으므로 줄이
                            // 서 있고, 그 뒤 신규가 줄로 가는 것이 맞는 동작이다 (불변식 4). 여기서
                            // 도착을 요구하면 추월을 요구하는 판정이 된다.
                            NodeIssueProbe.멎지_않았다("걷은 뒤 되살아난 노드", 걷은_뒤_둘째)))
                    .run();
        } finally {
            보고.shutdownNow();
            연결.close();
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

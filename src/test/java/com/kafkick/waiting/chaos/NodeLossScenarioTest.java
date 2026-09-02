package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.ControlPlaneProperties;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
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
// 램프를 짧게 잡는 배선은 C7 과 같다. 값만 다를 뿐 프로덕션이 도달하는 상태다.
@Tag("chaos")
// **컨텍스트를 캐시에 남기지 않는다.** 스케줄러를 켜고 띄우므로 제어 평면 루프가
// 계속 도는데, 뒷정리가 자원을 내린 뒤에도 캐시된 컨텍스트는 살아 있다 — 그 루프가
// 사라진 자원을 치면서 뒤 시험을 흔든다.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
@Import(NodeLossScenarioTest.ShortRamp.class)
class NodeLossScenarioTest {

    /** 램프를 짧게 잡는다. 운영값 60초로 재면 크레딧이 시험 동안 안 올라온다. */
    @TestConfiguration
    static class ShortRamp {

        @Bean
        @Primary
        ControlPlaneProperties 속성() {
            ControlPlaneProperties 기본 = ControlPlaneProperties.defaults();
            ControlPlaneProperties.Capacity c = 기본.capacity();
            return new ControlPlaneProperties(기본.scheduler(), 기본.leader(),
                    new ControlPlaneProperties.Capacity(Duration.ofSeconds(2), c.freshness(),
                            c.floor(), c.perInstanceCap(), c.rampDownTicks(), c.expectedNodes()));
        }
    }

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

    /**
     * 재료를 심는다. <b>가용량도 같이 보고한다</b> — 안 하면 크레딧이 바닥값
     * 이라 정상 구간에서 줄이 서고, 그 뒤로는 전 구간이 큐 등록이라 뒷단
     * 도착이 0 이 된다. 그러면 여기 걸린 판정이 전부 도달 불가가 된다.
     */
    private void 재료를_심는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "1000000").block(기다림);
    }

    /** 스냅샷이 싣고 있는 분모. <b>요청이 실제로 나누는 값이다.</b> */
    private int 스냅샷_분모() {
        return holder.view().snapshot().meta().gatewayCount();
    }

    /**
     * <b>뒷단까지 갔는가.</b> 이 구간이 0 이면 5xx 판정도 중복 판정도 도달
     * 불가라, 통과가 아무 뜻이 없다.
     */
    private Optional<String> 뒷단까지_갔다(String 구간, long 도착) {
        return 도착 > 0 ? Optional.empty()
                : Optional.of("%s — 뒷단에 한 건도 안 갔다 — 이 구간은 아무것도 안 쟀다"
                        .formatted(구간));
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

    /**
     * 스냅샷 분모가 그 수가 될 때까지 기다린다.
     *
     * <p><b>등록부가 아니라 스냅샷을 본다.</b> 요청이 나누는 값은 리더가 발행해
     * 폴러가 받아 온 쪽이다. 등록부만 보면 그 값이 데이터 평면에 안 닿아도
     * 통과한다.
     */
    private void 분모가_된다(int 기대, Duration 예산) {
        Awaitility.await().atMost(예산).until(() -> 스냅샷_분모() == 기대);
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
            long[] 도착 = new long[3];

            ChaosScenario.named("C6 노드 소실")
                    .baseline(() -> {
                        재료를_심는다();
                        // **뒷단 여유를 보고한다.** 안 하면 크레딧이 바닥값이라
                        // 정상 구간에서 줄이 서고, 그 뒤 전 구간이 큐 등록이
                        // 되어 뒷단 도착이 0 이 된다 — 그러면 판정이 전부
                        // 도달 불가다.
                        BackendReports 보고서 = BackendReports.실시계로(연결,
                                Duration.ofSeconds(3));
                        보고.scheduleAtFixedRate(() -> {
                            try {
                                보고서.보고한다("c6-be", 가용량);
                                살아있는_이웃.forEach(노드::등록한다);
                            } catch (RuntimeException e) {
                                // **조용히 죽지 않는다.** 한 번 던지면 그 뒤로
                                // 영영 안 뛰고, 그 침묵이 F5 결함으로 읽힌다.
                                뛰다_터진_수.incrementAndGet();
                            }
                        }, 0, 500, TimeUnit.MILLISECONDS);
                        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                        Awaitility.await().atMost(기다림).until(() -> 발급_상태(900) == 200);
                        살아있는_이웃.addAll(이웃);
                        // 이 노드까지 셋이다. 자기 하트비트는 앱이 넣는다.
                        분모가_된다(이웃.size() + 1, 기다림);
                        // **노드당 몫으로 센다.** 전역 크레딧을 보면 분모와
                        // 한산 비율이 빠져, 셋으로 나눈 뒤 0.7 을 곱한 값이
                        // 보낼 수보다 작아도 통과한다 — 그때 절반이 줄에 서고
                        // 뒷단 도착이 한 건이라 양성 대조도 통과한다.
                        Awaitility.await().atMost(기다림)
                                .until(() -> 한산한_몫() >= 보낼_수);
                        분모[0] = 스냅샷_분모();
                        도착[0] = 뒷단까지_센다(() -> 정상_상태.addAll(
                                여러_번_시도한다(보낼_수, 1_000)));
                    })
                    .inject(() -> {
                        // **뛰는 것을 멈추고 지운다.** 지우기만 하면 다음 갱신이
                        // 되살려 장애가 성립하지 않는다.
                        살아있는_이웃.remove(이웃.get(0));
                        노드.해제한다(이웃.get(0));
                    })
                    .duringFault(() -> {
                        // **감소는 즉시가 아니다** (F5). 연속 관측을 채우기 전에
                        // 떨어지면 죽은 줄 알았던 노드가 살아 있을 때 남은
                        // 노드가 몫을 키운다 — 그게 초과 발급 방향이다.
                        // **등록부에서 잰다.** 지연은 거기서 일어나고, 스냅샷은
                        // 발행·폴링 왕복만큼 늦어 그 창을 못 가른다.
                        Awaitility.await().pollDelay(하트비트.multipliedBy(감소_확정_틱 - 1))
                                .atMost(기다림).until(() -> true);
                        assertThat(registry.count())
                                .as("관측 %d 번까지는 안 줄어야 한다".formatted(감소_확정_틱 - 1))
                                .isEqualTo(이웃.size() + 1);
                        분모가_된다(이웃.size(), 기다림);
                        분모[1] = 스냅샷_분모();
                        도착[1] = 뒷단까지_센다(() -> 장애중_상태.addAll(
                                여러_번_시도한다(보낼_수, 2_000)));
                    })
                    .recover(() -> 살아있는_이웃.add(이웃.get(0)))
                    .afterRecovery(() -> {
                        // **즉시성을 시간으로 못 박는다** (F5). 20초 창으로 두면
                        // 증가를 몇 틱 지연시키는 회귀가 그냥 삼켜진다.
                        분모가_된다(이웃.size() + 1, 즉시_예산);
                        분모[2] = 스냅샷_분모();
                        도착[2] = 뒷단까지_센다(() -> 회복_상태.addAll(
                                여러_번_시도한다(보낼_수, 3_000)));
                    })
                    .assertEntry(ChaosScenario.Verdict.none())
                    .assertDuring(() -> RecoveryCriteria.violations(
                            오백이_안_샌다("유지", 장애중_상태),
                            // 양성 대조 — 도착이 0 이면 위 판정이 도달 불가다.
                            뒷단까지_갔다("유지", 도착[1]),
                            분모가_줄었다(분모[0], 분모[1])))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            오백이_안_샌다("회복", 회복_상태),
                            뒷단까지_갔다("회복", 도착[2]),
                            분모가_돌아왔다(분모[0], 분모[2]),
                            뒷단.중복_수신이_없다()))
                    // **RC1~RC6 은 여기서 안 잰다.** 이웃이 가짜라 예산을 안
                    // 쓰므로 "총 통과 ≤ 크레딧" 이 항진명제이고, 노드가 하나라
                    // 순번도 자리도 안 생긴다 (CY-838). G9.12 의 유입 비율은
                    // Phase 9 라우팅이라 여기서 불가다.
                    .run();

            assertThat(오백이_안_샌다("정상", 정상_상태)).isEmpty();
            assertThat(뒷단까지_갔다("정상", 도착[0])).isEmpty();
            assertThat(뛰다_터진_수.get()).as("하트비트가 조용히 멈추지 않았다").isZero();
        } finally {
            보고.shutdownNow();
            이웃.forEach(id -> 연결.sync().hdel(GatewayNodes.KEY, id));
            연결.close();
        }
    }

    private static final int 보낼_수 = 5;

    /** 하트비트 주기. 배분 틱과 같다 — 배분이 매 틱 분모를 읽는다. */
    private static final Duration 하트비트 =
            ControlPlaneProperties.defaults().scheduler().tick();

    /** 감소를 확정하기까지의 연속 관측 수. 설정에서 끌어온다 — 따로 적으면 갈린다. */
    private static final int 감소_확정_틱 =
            ControlPlaneProperties.defaults().capacity().rampDownTicks();

    /**
     * 한산한 쿠폰의 노드당 몫. 요청이 실제로 걸리는 상한이다.
     */
    private long 한산한_몫() {
        var meta = holder.view().snapshot().meta();
        return (long) (meta.globalCredit() / Math.max(1, meta.gatewayCount()) * 0.7);
    }

    /** 늘어난 분모가 닿기까지의 예산. 배분 한 틱과 재료 받아 오기가 든다. */
    private static final Duration 즉시_예산 = Duration.ofSeconds(3);

    /** 한 배치가 뒷단에 몇 건 닿았는지 센다. */
    private long 뒷단까지_센다(Runnable 배치) {
        long 전 = 뒷단.받은_수();
        배치.run();
        return 뒷단.받은_수() - 전;
    }

    /**
     * 살아 있는 이웃. <b>계속 뛰어야 산 것이다</b> — 한 번 심고 두면 하트비트
     * 창이 지나 같이 걷히고, 그러면 재려던 한 대가 아니라 둘이 빠진다.
     */
    private final Set<String> 살아있는_이웃 = ConcurrentHashMap.newKeySet();

    private final ScheduledExecutorService 보고 = Executors.newSingleThreadScheduledExecutor();

    /** 하트비트가 터진 횟수. 조용히 멈추면 그 침묵이 결함으로 읽힌다. */
    private final AtomicLong 뛰다_터진_수 = new AtomicLong();

    /** 뒷단이 부르는 여유. 크레딧을 바닥값 위로 올려 줄이 안 서게 한다. */
    private static final long 가용량 = 2_000;

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

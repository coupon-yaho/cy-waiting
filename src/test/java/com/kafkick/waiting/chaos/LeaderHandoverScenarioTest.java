package com.kafkick.waiting.chaos;

import com.kafkick.waiting.WaitingApplication;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.ControlPlaneLifecycle;
import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static org.assertj.core.api.Assertions.assertThat;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * C4b — 리더가 물러나고 <b>다른 노드가 이어받는다</b> (8.3.4 · 5절).
 *
 * <p>C4 와 갈리는 자리다. 저쪽은 대기 노드가 없어 리더 부재 구간을 재고, 여기는
 * 둘째가 실제로 이어받는다 — <b>낡음이 열리지 않는 것이 설계 의도</b>이고
 * (리스 2초 &lt; 낡음 문턱 5초) 그것을 못 박는 것이 이 시나리오다 (CY-821).
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class LeaderHandoverScenarioTest {

    private static final String COUPON = "c4b-queued";

    /** 줄이 없는 대조 쿠폰. 구간마다 따로 둬야 초당 예산이 구간을 안 넘는다. */
    private static final String[] 한산한_쿠폰 = {"c4b-idle-normal", "c4b-idle-fault",
            "c4b-idle-recovered"};

    private static final int 한산한_보낼_수 = 2;

    private static final int 줄_선_사람 = 5;

    private static final int 보낼_수 = 10;

    private static final Duration 기다림 = Duration.ofSeconds(60);

    private static final Duration 생존_수명 = Duration.ofMinutes(5);

    private static final long 가용량 = 2_000;

    private static final BackendStub 뒷단 = BackendStub.항상_받는다();

    private static final ScheduledExecutorService 보고 =
            Executors.newSingleThreadScheduledExecutor();

    private static final AtomicLong 뛰다_터진_수 = new AtomicLong();

    private static final Map<String, Double> 심은_자리 = new LinkedHashMap<>();

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.url", faults::주소);
    }

    @AfterAll
    static void 내린다() {
        보고.shutdownNow();
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

    @Autowired
    private Leadership 첫_노드;

    @Autowired
    private ControlPlaneProperties 제어;

    @Autowired
    private ControlPlaneLifecycle 수명;

    /** 승계 구간 내내 두 노드의 낡음을 긁는다. 한 점만 보면 가장 얕은 순간을 본다. */
    private final StaleWatch 낡음_감시 = new StaleWatch();

    /** 둘째의 스냅샷 보관자. 설계 주장이 걸린 곳은 이어받은 노드의 재료다. */
    private SecondNode 둘째_노드;

    private SnapshotHolder 둘째_홀더() {
        return 둘째_노드 == null ? null : 둘째_노드.빈("snapshotHolder", SnapshotHolder.class);
    }

    /**
     * 승계 구간의 낡음을 창으로 본다.
     *
     * <p>나이는 새 리더의 첫 발행 직전에 가장 크다. 그 시점은 회복 단계 안이라
     * 한 점 표본으로는 절대 안 잡힌다.
     */
    private static final class StaleWatch {

        private final Map<String, Supplier<SnapshotHolder>> 볼_곳 = new LinkedHashMap<>();

        private ScheduledExecutorService 폴러;

        private final Set<String> 낡았던 = ConcurrentHashMap.newKeySet();

        private final AtomicLong 본_판 = new AtomicLong();

        void 본다(String 이름, Supplier<SnapshotHolder> 홀더) {
            볼_곳.put(이름, 홀더);
        }

        void 시작한다() {
            폴러 = Executors.newSingleThreadScheduledExecutor();
            폴러.scheduleAtFixedRate(() -> {
                본_판.incrementAndGet();
                볼_곳.forEach((이름, 홀더) -> {
                    SnapshotHolder 지금 = 홀더.get();
                    if (지금 != null && 지금.isDataStale()) {
                        낡았던.add(이름);
                    }
                });
            }, 0, 50, TimeUnit.MILLISECONDS);
        }

        void 멈춘다() {
            if (폴러 != null) {
                폴러.shutdownNow();
            }
        }

        long 본_판() {
            return 본_판.get();
        }

        String 낡았던_노드() {
            return String.join("·", 낡았던);
        }
    }

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

    private long 뒷단까지_센다(String couponId, Runnable 배치) {
        long 전 = 뒷단.받은_수(couponId);
        배치.run();
        return 뒷단.받은_수(couponId) - 전;
    }

    private void 재료를_심는다(StatefulRedisConnection<String, String> 연결) {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "500").block(기다림);
        for (String 쿠폰 : 한산한_쿠폰) {
            redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, 쿠폰).block(기다림);
            redis.opsForValue().set(RedisKeys.stock(쿠폰), "100000").block(기다림);
        }
        심은_자리.putAll(QueueSeed.줄을_세운다(연결, COUPON, 줄_선_사람, 생존_수명));
    }

    @Test
    @DisplayName("C4b_리더가_물러나면_둘째가_이어받는다")
    void C4b_리더가_물러나면_둘째가_이어받는다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        try (SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                "http://localhost:" + 뒷단.port(), true)) {
            둘째_노드 = 둘째;
            낡음_감시.본다("첫 노드", () -> holder);
            낡음_감시.본다("둘째", this::둘째_홀더);
            List<Integer> 정상_상태 = new ArrayList<>();
            List<Integer> 정상_줄_상태 = new ArrayList<>();
            List<Integer> 승계중_상태 = new ArrayList<>();
            List<Integer> 승계중_줄_상태 = new ArrayList<>();
            List<Integer> 회복_상태 = new ArrayList<>();
            List<Integer> 회복_줄_상태 = new ArrayList<>();
            long[] 한산한_도착 = new long[3];
            long[] 줄_도착 = new long[3];
            long[] 판_번호 = new long[2];

            String[] 주인 = new String[2];
            BackendReports[] 보고기 = new BackendReports[1];
            GatewayNodes 노드 = new GatewayNodes(연결, Duration.ofSeconds(30));

            ChaosScenario.named("C4b 리더 승계")
                    .baseline(() -> {
                        재료를_심는다(연결);
                        보고기[0] = BackendReports.실시계로(연결, Duration.ofSeconds(3));
                        보고.scheduleAtFixedRate(() -> {
                            try {
                                보고기[0].보고한다("c4b-be", 가용량);
                            } catch (RuntimeException e) {
                                뛰다_터진_수.incrementAndGet();
                            }
                        }, 0, 500, TimeUnit.MILLISECONDS);
                        // **둘이 다 서 있어야 승계다.** 한 대만 뜬 채로 리더를
                        // 내리면 그건 C4 가 재는 리더 부재 구간이다.
                        Awaitility.await().alias("두 노드가 다 하트비트를 남긴다")
                                .atMost(기다림).until(() -> 노드.살아있는_수() >= 2);
                        // **첫 노드가 리더여야 한다.** 둘째가 쥔 채로 첫 노드의
                        // 제어 평면을 세우면 리더십에 아무 일도 안 일어나, 승계
                        // 없이 전 구간이 통과한다. 아니면 락을 비워 다시 뽑는다.
                        Awaitility.await().alias("첫 노드가 리더를 쥔다")
                                .atMost(기다림).pollInterval(Duration.ofMillis(300))
                                .until(() -> {
                                    if (첫_노드.isLeader()) {
                                        return true;
                                    }
                                    연결.sync().del(RedisKeys.LEADER);
                                    return false;
                                });
                        Awaitility.await().alias("첫 스냅샷이 닿아 재료가 신선해진다")
                                .atMost(기다림).until(() -> !holder.isDataStale());
                        // **스냅샷이 이 쿠폰을 줄 선 상태로 볼 때까지 기다린다.**
                        // 신선하기만 하면 심기 전에 발행된 판으로도 만족되고,
                        // 그 창에 걸리면 아직 한산해서 200 이 나간다.
                        Awaitility.await().alias("스냅샷이 이 쿠폰의 줄을 본다")
                                .atMost(기다림)
                                .until(() -> 발급_상태(COUPON, 999_000) == 202);
                        판_번호[0] = 판_번호를_읽는다();
                        주인[0] = 주인을_읽는다();
                        한산한_도착[0] = 뒷단까지_센다(한산한_쿠폰[0], () -> 정상_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰[0], 한산한_보낼_수, 100)));
                        줄_도착[0] = 뒷단까지_센다(COUPON, () -> 정상_줄_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 1_000)));
                    })
                    .inject(() -> {
                        // **첫 노드의 제어 평면을 세운다. 락은 안 놓는다.**
                        // 그래서 락이 리스 만료까지 남고, 그 구간이 프로덕션
                        // 리스가 정하는 승계 지연 그대로다. 유령을 얹어
                        // 만료시키면 그 길이를 시험이 정하게 되어, 리스가 낡음
                        // 문턱을 지키는지가 안 재진다.
                        //
                        // 후보가 둘째뿐이라 누가 이길지도 결정적이다. 락을
                        // 비워 두고 다투게 하면 두 연장 루프의 위상이 고정돼
                        // 늘 같은 쪽이 먼저 닿는다 — 실측으로 그랬다.
                        낡음_감시.시작한다();
                        수명.stop();
                    })
                    .duringFault(() -> {
                        한산한_도착[1] = 뒷단까지_센다(한산한_쿠폰[1], () -> 승계중_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰[1], 한산한_보낼_수, 200)));
                        줄_도착[1] = 뒷단까지_센다(COUPON, () -> 승계중_줄_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 2_000)));
                    })
                    .recover(() -> Awaitility.await().alias("둘째가 리더를 이어받는다")
                            .atMost(기다림).until(둘째::리더인가))
                    .afterRecovery(() -> {
                        Awaitility.await().alias("가용량 보고가 다시 닿는다").atMost(기다림)
                                .until(() -> 보고기[0].신선한_보고().containsKey("c4b-be"));
                        Awaitility.await().alias("두 노드의 스냅샷이 다 신선하다")
                                .atMost(기다림)
                                .until(() -> !holder.isDataStale() && !둘째가_낡았나());
                        낡음_감시.멈춘다();
                        판_번호[1] = 판_번호를_읽는다();
                        주인[1] = 주인을_읽는다();
                        한산한_도착[2] = 뒷단까지_센다(한산한_쿠폰[2], () -> 회복_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰[2], 한산한_보낼_수, 300)));
                        줄_도착[2] = 뒷단까지_센다(COUPON, () -> 회복_줄_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 3_000)));
                    })
                    .assertEntry(() -> RecoveryCriteria.violations(
                            대조군이_받았다("정상", 정상_상태),
                            대조군이_뒷단까지_갔다("정상", 한산한_도착[0]),
                            줄에_세웠다("정상", 정상_줄_상태, 보낼_수),
                            줄을_추월하지_않았다("정상", 줄_도착[0]),
                            노드가_둘이었다(노드),
                            // **둘째가 이어받을 수 있는 노드인가.** 하트비트만
                            // 세면 제어 평면이 없는 노드도 둘로 세어져, 이어받을
                            // 수 없는 판을 승계로 읽는다.
                            둘째가_이어받을_수_있다(둘째)))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            // **승계 중에도 판정이 선다.** 리더가 바뀌는 것이
                            // 판정 경로의 일이 아니다 — 그 경로는 레디스를
                            // 안 치고 로컬 스냅샷만 본다 (G5.1).
                            대조군이_받았다("승계", 승계중_상태),
                            대조군이_뒷단까지_갔다("승계", 한산한_도착[1]),
                            줄에_세웠다("승계", 승계중_줄_상태, 보낼_수),
                            줄을_추월하지_않았다("승계", 줄_도착[1])))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            대조군이_받았다("회복", 회복_상태),
                            대조군이_뒷단까지_갔다("회복", 한산한_도착[2]),
                            줄에_세웠다("회복", 회복_줄_상태, 보낼_수),
                            줄을_추월하지_않았다("회복", 줄_도착[2]),
                            // **판 번호는 앞선다.** 같으면 승계가 없었던 것이다.
                            판_번호가_앞섰다(판_번호[0], 판_번호[1]),
                            // **둘째가 이어받았는가.** 후보가 하나뿐이라
                            // 결정적이다 — 첫 노드가 다시 잡았으면 제어 평면을
                            // 세운 것이 안 먹은 것이다.
                            둘째가_이어받았다(주인[1], 둘째),
                            // **창이 닫힌 뒤에 본다.** 유지 판정에 걸면 나이가
                            // 가장 큰 구간 — 새 리더의 첫 발행 직전 — 이 아직
                            // 안 지난 채로 돌아, 가장 얕은 순간을 보게 된다.
                            낡음이_안_열렸다(),
                            보고가_안_터졌다(),
                            // **RC5 는 여기서 깨질 수 없다** (CY-844). 앞줄
                            // 제거가 재개 유예에 걸려 시험 수명 안에 안 돈다.
                            // 초록이지만 증거가 아니라, 재는 척하는 자리다.
                            RecoveryCriteria.seatLost(심은_자리,
                                    QueueSeed.자리들(연결, COUPON, 줄_선_사람)),
                            뒷단.중복_수신이_없다()))
                    .run();
        } finally {
            // **보고를 먼저 멈춘다.** 연결을 닫은 뒤에도 500ms 틱이 한 번 더
            // 뛰면 반드시 터지고, 그러면 터진 수 판정이 제 손으로 깨진다.
            보고.shutdownNow();
            낡음_감시.멈춘다();
            연결.sync().del(RedisKeys.LEADER);
            연결.close();
        }
    }

    /** 지금 락에 적힌 판 번호. 없으면 0 이다. */
    private long 판_번호를_읽는다() {
        String 값 = redis.opsForValue().get(RedisKeys.LEADER).block(기다림);
        if (값 == null) {
            return 0;
        }
        int 구분 = 값.indexOf('|');
        return 구분 < 0 ? 0 : Long.parseLong(값.substring(0, 구분));
    }

    /** 지금 락의 주인. 값의 형식은 판 번호와 주인을 구분자로 이은 것이다. */
    private String 주인을_읽는다() {
        String 값 = redis.opsForValue().get(RedisKeys.LEADER).block(기다림);
        if (값 == null) {
            return "";
        }
        int 구분 = 값.indexOf('|');
        return 구분 < 0 ? 값 : 값.substring(구분 + 1);
    }

    /** 둘째가 이어받았는가. 후보가 하나뿐이라 결정적이다. */
    private Optional<String> 둘째가_이어받았다(String 주인, SecondNode 둘째) {
        if (주인.isEmpty()) {
            return Optional.of("회복 뒤에 락 주인이 없다 — 아무도 안 이어받았다");
        }
        return 주인.equals(둘째.ownerId()) ? Optional.empty()
                : Optional.of("주인이 %s 다 — 둘째(%s)가 못 이어받았다"
                        .formatted(주인, 둘째.ownerId()));
    }

    /** 보고가 터지면 크레딧이 달라져 구간끼리 비교가 성립하지 않는다. */
    private Optional<String> 보고가_안_터졌다() {
        long 터진 = 뛰다_터진_수.get();
        return 터진 == 0 ? Optional.empty()
                : Optional.of("가용량 보고가 %d 판 터졌다 — 크레딧이 구간마다 다르다"
                        .formatted(터진));
    }

    /**
     * <b>둘째가 이어받을 수 있는 노드인가.</b> 제어 평면이 없으면 하트비트는
     * 남기지만 락을 못 잡는다 — 노드를 세는 것만으로는 그 둘이 안 갈린다.
     */
    private Optional<String> 둘째가_이어받을_수_있다(SecondNode 둘째) {
        try {
            String 이름 = 둘째.ownerId();
            return 이름 != null && !이름.isBlank() && !이름.equals(첫_노드.ownerId())
                    ? Optional.empty()
                    : Optional.of("전제 — 둘째의 주인 이름이 '%s' 다".formatted(이름));
        } catch (RuntimeException e) {
            return Optional.of("전제 — 둘째에 제어 평면이 없다: " + e.getMessage());
        }
    }

    /** 한 대만 서 있으면 이건 승계가 아니라 리더 부재다 — C4 가 재는 판이다. */
    private Optional<String> 노드가_둘이었다(GatewayNodes 노드) {
        int 산_노드 = 노드.살아있는_수();
        return 산_노드 >= 2 ? Optional.empty()
                : Optional.of("전제 — 살아 있는 노드가 %d 대다. 이어받을 노드가 없다"
                        .formatted(산_노드));
    }

    /**
     * <b>낡음이 안 열려야 한다.</b> 리스가 낡음 문턱보다 짧아 승계가 그 안에
     * 끝나는 것이 설계 의도다. 열렸다면 그 구간에 fail-open 이 열린다.
     *
     * <p>창 전체를 본다. 한 점만 보면 낡음이 가장 얕은 순간을 골라 안 열렸다고
     * 적는 셈이다 — 나이는 새 리더의 첫 발행 직전에 가장 크다.
     */
    private Optional<String> 낡음이_안_열렸다() {
        if (낡음_감시.본_판() == 0) {
            return Optional.of("전제 — 승계 구간을 한 번도 안 봤다");
        }
        return 낡음_감시.낡았던_노드().isEmpty() ? Optional.empty()
                : Optional.of("승계 중에 %s 의 재료가 낡았다 — 리스(%s)가 문턱을 못 지켰다"
                        .formatted(낡음_감시.낡았던_노드(), 제어.leader().lease()));
    }

    private boolean 둘째가_낡았나() {
        SnapshotHolder 둘째_홀더 = 둘째_홀더();
        return 둘째_홀더 != null && 둘째_홀더.isDataStale();
    }

    /** 판 번호가 안 앞서면 락을 한 번도 안 놓은 것이라 승계가 없었다. */
    private Optional<String> 판_번호가_앞섰다(long 전, long 후) {
        if (전 <= 0) {
            return Optional.of("전제 — 정상 구간에 판 번호가 없었다");
        }
        return 후 > 전 ? Optional.empty()
                : Optional.of("판 번호가 %d 에서 %d 로 안 늘었다 — 승계가 없었다"
                        .formatted(전, 후));
    }

    private Optional<String> 대조군이_받았다(String 구간, List<Integer> 상태) {
        if (상태.size() != 한산한_보낼_수) {
            return Optional.of("%s — %d 건을 보냈는데 %d 건만 관측됐다"
                    .formatted(구간, 한산한_보낼_수, 상태.size()));
        }
        long 못_받은_것 = 상태.stream().filter(status -> status != 200).count();
        return 못_받은_것 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 200 이 아니다 (보낸 %d): %s"
                        .formatted(구간, 못_받은_것, 상태.size(), 상태));
    }

    private Optional<String> 대조군이_뒷단까지_갔다(String 구간, long 도착) {
        return 도착 == 한산한_보낼_수 ? Optional.empty()
                : Optional.of("%s — 대조 쿠폰이 %d 건만 뒷단까지 갔다 (보낸 %d)"
                        .formatted(구간, 도착, 한산한_보낼_수));
    }

    private Optional<String> 줄에_세웠다(String 구간, List<Integer> 상태, int 보낸_수) {
        if (상태.size() != 보낸_수) {
            return Optional.of("%s — %d 건을 보냈는데 %d 건만 관측됐다"
                    .formatted(구간, 보낸_수, 상태.size()));
        }
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

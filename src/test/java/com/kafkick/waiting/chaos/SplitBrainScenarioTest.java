package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
import io.lettuce.core.api.StatefulRedisConnection;
import com.kafkick.waiting.control.ControlPlaneProperties;
import io.micrometer.core.instrument.MeterRegistry;
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
 * <p>재는 것은 <b>줄 선 사람을 추월하지 않는가</b> 하나다 (불변식 4). 자기가
 * 리더라고 믿는 것도, 재료가 낡은 것도 추월을 정당화하지 않는다. 초과 발급은
 * 여기서 못 잰다 — 뒷단이 스텁이라 재고를 안 깎는다 (AIJ-0170).
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

    /** 레디스 왕복의 상한. 전이 대기와 뜻이 달라 따로 둔다. */
    private static final Duration 레디스_한계 = Duration.ofSeconds(20);

    /** 상태가 바뀌기를 기다리는 예산. */
    private static final Duration 전이_한계 = Duration.ofSeconds(20);

    /** 심어 둔 줄의 생존 신호 수명. 시험 수명보다 길어야 스위퍼가 살아 있다고 읽는다. */
    private static final Duration 생존_수명 = Duration.ofMinutes(5);

    /** 재료가 낡아 fail-open 으로 열린 판정. */
    private static final String 낡아서_열림 = "PASS_FAIL_OPEN";

    /** 재료가 낡은 채로 줄에 세운 판정. */
    private static final String 낡아서_줄섬 = "ENQUEUE_STALE";

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

    @Autowired
    private MeterRegistry meters;

    @Autowired
    private ControlPlaneProperties 제어;

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
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON, 한산한_쿠폰).block(레디스_한계);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "50").block(레디스_한계);
        redis.opsForValue().set(RedisKeys.stock(한산한_쿠폰), "100000").block(레디스_한계);
        QueueSeed.줄을_세운다(연결, COUPON, 줄_선_사람, 생존_수명);
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
        // **한계가 리스보다 짧아야 이 시험이 판정이다.** 리스가 설정에서
        // 내려가면 사실 기반 강등과 만료 강등을 시간으로 못 가르고, 그러면
        // 강등을 통째로 들어내도 초록이 된다 (AIJ-0170).
        assertThat(강등_한계).as("강등 한계는 리스(%s)에서 시도 예산(%s)을 뺀 것보다 짧아야 한다",
                        제어.leader().lease(), 제어.leader().attempt())
                .isLessThan(제어.leader().lease().minus(제어.leader().attempt()));
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        try {
            LeaderFaults 락 = LeaderFaults.of(연결);
            List<Integer> 정상_상태 = new ArrayList<>();
            List<Integer> 장애중_줄_상태 = new ArrayList<>();
            List<Integer> 회복_상태 = new ArrayList<>();
            List<Integer> 회복_줄_상태 = new ArrayList<>();
            List<Integer> 정상_줄_상태 = new ArrayList<>();
            long[] 펜스 = new long[3];
            long[] 줄_도착 = new long[2];
            long[] 한산한_도착 = new long[3];
            long[] 낡음_직후 = new long[2];
            long[] 분단중_도착 = new long[1];
            List<Integer> 분단중_상태 = new ArrayList<>();
            long[] 인계_시각 = new long[1];
            Duration[] 강등까지 = new Duration[1];
            List<Integer> 한산한_장애중 = new ArrayList<>();
            Map<String, Double> 장애_전_자리 = new LinkedHashMap<>();
            Map<String, Double> 회복_뒤_자리 = new LinkedHashMap<>();

            ChaosScenario.named("C5 리더 분단")
                    .baseline(() -> {
                        재료를_심는다(연결);
                        Awaitility.await().alias("리더를 잡아야 배분이 돈다")
                                .atMost(전이_한계).until(leadership::isLeader);
                        // 재료가 닿아야 판정이 선다. 안 기다리면 전 구간이
                        // 거절이고 그러면 아무것도 못 잰다.
                        Awaitility.await().alias("첫 스냅샷이 닿아 재료가 신선해진다")
                                .atMost(전이_한계).until(() -> !holder.isDataStale());
                        펜스[0] = leadership.fence();
                        장애_전_자리.putAll(QueueSeed.자리들(연결, COUPON, 줄_선_사람));
                        한산한_도착[0] = 뒷단까지_센다(한산한_쿠폰, () -> 정상_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 1_100)));
                        정상_줄_상태.addAll(여러_번_시도한다(COUPON, 보낼_수, 1_000));
                    })
                    .inject(() -> {
                        // **갈라진 쪽이 락을 가져간다.** 우리 노드는 아직 자기가
                        // 리더라고 믿는다 — 그 창이 스플릿 브레인이다.
                        //
                        // **믿었는지를 판정하지는 않는다.** 인계는 원자적인데
                        // 연장 루프는 따로 돈다. 그 사이에 루프가 먼저 사실을
                        // 보면 옳은 구현인데도 "창이 없었다" 가 되어, 판정이
                        // 프로덕션이 아니라 스케줄러 타이밍을 잰다. 겹친 창
                        // 자체는 노드 둘짜리 하네스라야 확정한다 (CY-821).
                        assertThat(락.죽은_리더가_넘겨받는다(갈라진_리더,
                                Duration.ofSeconds(30))).isTrue();
                        // **그 창 안에서 재고 넘어간다.** 강등을 기다린 뒤에만
                        // 재면 남는 것은 "팔로워 + 낡은 재료" 라, C4 가 이미
                        // 재는 상태다. 이 시나리오만의 자리는 여기뿐이다.
                        인계_시각[0] = System.nanoTime();
                        분단중_도착[0] = 뒷단까지_센다(COUPON, () -> 분단중_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 4_000)));
                    })
                    .duringFault(() -> {
                        // **강등을 기다린다.** 다음 갱신이 실패해야 이 노드가
                        // 리더가 아님을 안다. 기회를 봐서 읽으면 갱신 주기와
                        // 요청 속도의 경합이라, 빠른 판에서는 아직 리더로 읽힌다.
                        Awaitility.await()
                                .alias("사실 기반 강등 — 남이 락을 쥔 것을 다음 갱신에서 알고 "
                                        + "내려와야 한다. 리스 만료를 기다려 내려오면 이 한계를 넘는다")
                                .atMost(전이_한계).until(() -> !leadership.isLeader());
                        // **인계 시각부터 잰다.** 대기 예산으로만 걸면 그 앞의
                        // 배치가 길어진 만큼 한계가 조용히 늘어나, 사실 기반
                        // 강등을 들어내도 초록이 된다.
                        강등까지[0] = Duration.ofNanos(System.nanoTime() - 인계_시각[0]);
                        펜스[1] = leadership.fence();
                        // **분단을 낡음 너머로 끌고 간다.** 200ms 만에 걷으면
                        // 배분 틱 하나도 안 놓치고 낡음도 안 열려, 줄을 심어 둔
                        // 것이 판정에 아무 역할을 못 한다 — fail-open 이 줄을
                        // 추월하는 회귀가 그 구간에 아예 없다.
                        Awaitility.await()
                                .alias("배분이 멎어 재료가 낡음 문턱을 넘어야 fail-open 갈래가 열린다")
                                .atMost(전이_한계).until(holder::isDataStale);
                        낡음_직후[0] = 결정_수(낡아서_열림);
                        낡음_직후[1] = 결정_수(낡아서_줄섬);
                        한산한_도착[1] = 뒷단까지_센다(한산한_쿠폰, () -> 한산한_장애중.addAll(
                                여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 2_100)));
                        줄_도착[0] = 뒷단까지_센다(COUPON, () -> 장애중_줄_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 2_000)));
                    })
                    .recover(() -> 락.lease를_만료시킨다(Duration.ofMillis(1)))
                    .afterRecovery(() -> {
                        Awaitility.await().alias("락을 되찾아 다시 리더가 된다")
                                .atMost(전이_한계).until(leadership::isLeader);
                        Awaitility.await().alias("새 스냅샷이 닿아 재료가 다시 신선해진다")
                                .atMost(전이_한계).until(() -> !holder.isDataStale());
                        펜스[2] = leadership.fence();
                        한산한_도착[2] = 뒷단까지_센다(한산한_쿠폰, () -> 회복_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 3_100)));
                        줄_도착[1] = 뒷단까지_센다(COUPON, () -> 회복_줄_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 3_000)));
                        회복_뒤_자리.putAll(QueueSeed.자리들(연결, COUPON, 줄_선_사람));
                    })
                    .assertEntry(() -> RecoveryCriteria.violations(
                            // **믿음이 추월을 정당화하지 않는다** (불변식 4).
                            줄에_세웠다("진입", 분단중_상태, 보낼_수),
                            줄을_추월하지_않았다("진입", 분단중_도착[0]),
                            // 전제를 여기서 본다. run() 뒤에 두면 다른 사유로
                            // 던졌을 때 영영 안 돌고, 깨진 전제가 안 보인다.
                            평시에_섰다(정상_줄_상태, 정상_상태, 한산한_도착[0])))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            // 양성 대조 — 한산한 쪽이 계속 통과해야 아래
                            // "추월 0" 이 무언가를 잰 것이 된다.
                            열려_있었다("유지", 한산한_도착[1], 한산한_도착[0], 한산한_장애중),
                            사실로_강등했다(강등까지[0]),
                            낡은_갈래를_밟았다(낡음_직후[0], 낡음_직후[1]),
                            판정이_멈추지_않았다("유지 한산", 한산한_장애중),
                            판정이_멈추지_않았다("유지", 장애중_줄_상태),
                            // 5xx 만 보면 전원이 429 로 거절돼도 통과한다.
                            줄에_세웠다("유지", 장애중_줄_상태, 보낼_수),
                            // **줄이 있으면 추월 금지** (불변식 4). 갈라진 동안
                            // 크레딧이 두 배여도 줄 선 사람을 건너뛰면 안 된다.
                            줄을_추월하지_않았다("유지", 줄_도착[0])))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            열려_있었다("회복", 한산한_도착[2], 한산한_도착[0], 회복_상태),
                            판정이_멈추지_않았다("회복", 회복_상태),
                            줄에_세웠다("회복", 회복_줄_상태, 보낼_수),
                            // **RC5 는 여기서 깨질 수 없다** (CY-844). 자리를
                            // 걷는 것은 스위퍼인데, 승계마다 재개 유예가 0 부터
                            // 다시 서고 그 유예가 시험 수명보다 길다. 유령
                            // 리더도 락만 쥘 뿐 배분을 안 돈다. 초록이지만
                            // 증거가 아니라, 재는 척하는 자리다.
                            RecoveryCriteria.seatLost(장애_전_자리, 회복_뒤_자리),
                            줄을_추월하지_않았다("회복", 줄_도착[1]),
                            리더십이_실제로_갈라졌다(펜스[0], 펜스[1], 펜스[2]),
                            뒷단.중복_수신이_없다()))
                    .run();
        } finally {
            연결.sync().del(RedisKeys.LEADER);
            연결.close();
        }
    }

    /**
     * <b>사실로 내려왔는가.</b> 리스 만료를 기다려 내려오면 그동안 두 노드가
     * 함께 배분을 돈다. 한계는 리스에서 시도 예산을 뺀 값이라, 만료 경로는
     * 여기 못 든다.
     */
    private Optional<String> 사실로_강등했다(Duration 걸린_시간) {
        if (걸린_시간 == null) {
            return Optional.of("전제 — 강등까지의 시간을 못 쟀다");
        }
        return 걸린_시간.compareTo(강등_한계) < 0 ? Optional.empty()
                : Optional.of("강등에 %dms 가 걸렸다 (한계 %dms) — 리스 만료를 기다린 것이다"
                        .formatted(걸린_시간.toMillis(), 강등_한계.toMillis()));
    }

    /** 평시가 성립해야 나머지 구간의 값에 뜻이 생긴다. */
    private Optional<String> 평시에_섰다(List<Integer> 줄_상태, List<Integer> 한산한_상태,
            long 한산한_도착) {
        Optional<String> 줄 = 줄에_세웠다("정상", 줄_상태, 보낼_수);
        if (줄.isPresent()) {
            return 줄;
        }
        Optional<String> 멈춤 = 판정이_멈추지_않았다("정상", 한산한_상태);
        if (멈춤.isPresent()) {
            return 멈춤;
        }
        return 한산한_도착 == 한산한_보낼_수 ? Optional.empty()
                : Optional.of("전제 — 평시에 한산한 쿠폰이 %d 건만 갔다 (보낸 %d)"
                        .formatted(한산한_도착, 한산한_보낼_수));
    }

    /** 전면 차단이 아니다. 대조군이 막히면 아래 추월 판정이 아무것도 안 잰다. */
    private Optional<String> 열려_있었다(String 구간, long 도착, long 평시, List<Integer> 상태) {
        if (평시 <= 0) {
            return Optional.of("전제 — 평시 도착을 못 쟀다 (%d)".formatted(평시));
        }
        return 도착 == 평시 ? Optional.empty()
                : Optional.of("%s — 한산한 쿠폰이 %d 건만 갔다 (평시 %d): %s"
                        .formatted(구간, 도착, 평시, 상태));
    }

    /**
     * 줄이 선 쿠폰이면 202 로 자리를 받아야 한다.
     *
     * <p>보낸 수를 함께 받는다. 단계가 중간에 터져 목록이 비면 "위반 없음" 과
     * 구분이 안 되는데, 그 경로는 실제로 열려 있다 — 뼈대가 예외를 삼킨다.
     */
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

    /** 줄이 선 쿠폰에 새로 온 사람이 뒷단까지 갔다면 줄을 건너뛴 것이다. */
    private Optional<String> 줄을_추월하지_않았다(String 구간, long 도착) {
        return 도착 == 0 ? Optional.empty()
                : Optional.of("%s — 줄이 선 쿠폰에서 %d 건이 뒷단까지 갔다".formatted(구간, 도착));
    }

    /**
     * <b>낡은 갈래를 실제로 밟았는가.</b> 상태 코드는 평시와 같은 202 라, 어느
     * 줄이 답했는지는 결정 계수로만 보인다. 이것이 없으면 판정이 낡음을 통째로
     * 못 보게 만들어도 관측값이 평시와 같아 초록이다.
     */
    private Optional<String> 낡은_갈래를_밟았다(long 열림_직후, long 줄섬_직후) {
        long 열림 = 결정_수(낡아서_열림) - 열림_직후;
        long 줄섬 = 결정_수(낡아서_줄섬) - 줄섬_직후;
        if (줄섬 < 보낼_수) {
            return Optional.of("낡은 재료로 줄에 세운 것이 %d 건뿐이다 (보낸 %d)"
                    .formatted(줄섬, 보낼_수));
        }
        return 열림 >= 한산한_보낼_수 ? Optional.empty()
                : Optional.of("낡은 재료로 열어 준 것이 %d 건뿐이다 (보낸 %d)"
                        .formatted(열림, 한산한_보낼_수));
    }

    /** 그 결정이 지금까지 몇 번 나왔는가. */
    private long 결정_수(String outcome) {
        return (long) meters.find("waiting.admission").tag("outcome", outcome)
                .counters().stream().mapToDouble(counter -> counter.count()).sum();
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

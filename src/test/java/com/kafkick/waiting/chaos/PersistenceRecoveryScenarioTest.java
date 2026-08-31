package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.SnapshotHolder;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * C12 — 레디스를 kill -9 한 뒤 영속으로 되살린다 (8.3.4 · 5절).
 *
 * <p>C1 과 갈리는 것은 <b>돌아온 뒤에 무엇이 남아 있는가</b>다. 저쪽은 통째로
 * 비어 있고 여기는 마지막 1초만 없다. <b>증발 자체는 허용한다</b>(E-6) —
 * 살아남은 사람을 재등록자가 추월하지 않는지만 본다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class PersistenceRecoveryScenarioTest {

    private static final String COUPON = "c12-queued";

    /** 줄이 없는 대조 쿠폰. 구간마다 따로 둬야 초당 예산이 구간을 안 넘는다. */
    private static final String[] 한산한_쿠폰 = {"c12-idle-normal", "c12-idle-recovered"};

    private static final int 한산한_보낼_수 = 2;

    private static final int 줄_선_사람 = 5;

    private static final int 보낼_수 = 10;

    /** 살아남을 사람들. 강제 내려쓰기 뒤에 등록해 디스크에 확실히 남긴다. */
    private static final int 살아남을_회원 = 1_000;

    /** 증발할 수도 있는 사람들. 죽기 직전에 등록해 everysec 창에 걸친다. */
    private static final int 증발할_회원 = 2_000;

    private static final Duration 기다림 = Duration.ofSeconds(30);

    private static final Duration 생존_수명 = Duration.ofMinutes(5);

    private static final long 가용량 = 2_000;

    private static final BackendStub 뒷단 = BackendStub.항상_받는다();

    private static final ScheduledExecutorService 보고 =
            Executors.newSingleThreadScheduledExecutor();

    /** 보고가 터진 수. 장애 구간에는 터지는 것이 정상이라 세기만 한다. */
    private static final AtomicLong 뛰다_터진_수 = new AtomicLong();

    /** 심어 둔 줄의 자리. 가장 확실한 생존자라 회복 판정에 그대로 쓴다. */
    private static final Map<String, Double> 심은_자리 = new LinkedHashMap<>();

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.영속으로_시작한다();
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

    /** 그 무리의 자리. 없는 사람은 안 담는다 — 증발한 것과 남은 것을 그렇게 가른다. */
    private Map<String, Double> 자리들(int 시작_회원, int 수) {
        Map<String, Double> 자리 = new LinkedHashMap<>();
        for (int i = 0; i < 수; i++) {
            String member = String.valueOf(시작_회원 + i);
            Double score = redis.opsForZSet()
                    .score(RedisKeys.queue(COUPON, 1, 0), member).block(기다림);
            if (score != null) {
                자리.put(member, score);
            }
        }
        return 자리;
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
    @DisplayName("C12_영속으로_돌아와도_재등록자가_추월하지_않는다")
    void C12_영속으로_돌아와도_재등록자가_추월하지_않는다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        try {
            List<Integer> 정상_상태 = new ArrayList<>();
            List<Integer> 정상_줄_상태 = new ArrayList<>();
            List<Integer> 장애중_줄_상태 = new ArrayList<>();
            List<Integer> 회복_상태 = new ArrayList<>();
            List<Integer> 재등록_상태 = new ArrayList<>();
            Map<String, Double> 살아남을_자리 = new LinkedHashMap<>();
            Map<String, Double> 회복_뒤_자리 = new LinkedHashMap<>();
            Map<String, Double> 재등록_자리 = new LinkedHashMap<>();
            Map<String, Double> 증발_뒤_자리 = new LinkedHashMap<>();
            Map<String, Double> 심은_뒤_자리 = new LinkedHashMap<>();
            long[] 한산한_도착 = new long[2];
            long[] 줄_도착 = new long[3];
            long[] 진입_임계 = new long[1];
            BackendReports[] 보고기 = new BackendReports[1];
            long[] 회복_임계 = new long[1];

            ChaosScenario.named("C12 영속 복구")
                    .baseline(() -> {
                        재료를_심는다(연결);
                        보고기[0] = BackendReports.실시계로(연결, Duration.ofSeconds(3));
                        BackendReports 보고서 = 보고기[0];
                        // **조용히 죽지 않는다.** 한 번 던지면 그 뒤로 영영
                        // 안 뛴다. 레디스를 죽이는 시나리오라 반드시 던지고,
                        // 그러면 회복 구간이 하한 크레딧에서 돌아 진입 구간과
                        // 다른 판이 된다 — 비교 자체가 성립하지 않는다.
                        보고.scheduleAtFixedRate(() -> {
                            try {
                                보고서.보고한다("c12-be", 가용량);
                            } catch (RuntimeException e) {
                                뛰다_터진_수.incrementAndGet();
                            }
                        }, 0, 500, TimeUnit.MILLISECONDS);
                        Awaitility.await().alias("첫 스냅샷이 닿아 재료가 신선해진다")
                                .atMost(기다림).until(() -> !holder.isDataStale());
                        한산한_도착[0] = 뒷단까지_센다(한산한_쿠폰[0], () -> 정상_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰[0], 한산한_보낼_수, 100)));
                        줄_도착[0] = 뒷단까지_센다(COUPON, () -> 정상_줄_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 살아남을_회원)));
                        // **여기까지를 디스크에 못 박는다.** everysec 은 마지막
                        // 1초를 잃는데, 그 1초 안에 살아남을 무리가 들어가면
                        // 이 시험이 재는 것이 통째 유실(C1)이 되어 버린다.
                        디스크에_내려쓴다();
                        살아남을_자리.putAll(자리들(살아남을_회원, 보낼_수));
                        진입_임계[0] = 임계를_읽는다();
                    })
                    .inject(() -> {
                        // **생존 신호를 지운다.** 폴링마다 나가는 가장 잦은
                        // 쓰기라 유실 꼬리에 가장 많이 걸린다 — 줄에는 사람이
                        // 있는데 살아 있는 신호가 하나도 없는 상태가 되고,
                        // 그게 스위프 스크립트가 막으려는 바로 그 판이다.
                        //
                        // 덧붙이기를 멈추기 전에 지운다. 뒤에 지우면 그 삭제
                        // 자체가 파일에 안 남아 회복 뒤에 도로 살아난다.
                        생존_신호를_지운다();
                        // **마지막 창을 결정적으로 만든다.** 그냥 등록하고
                        // 죽이면 everysec 이 이미 내려쓴 뒤라 열에 열이 살아남고,
                        // 그러면 "증발한 사람이 다시 선다" 는 이 시나리오의
                        // 판정이 한 번도 안 돈다 — 실측으로 그랬다.
                        //
                        // 덧붙이기를 멈춘 뒤에 등록하면 그 쓰기는 메모리에만
                        // 남는다. 파일은 그대로라 다시 켤 때 그때까지가 실린다.
                        // everysec 창이 통째로 날아간 최악의 판과 같은 모양이다.
                        덧붙이기를_멈춘다();
                        여러_번_시도한다(COUPON, 보낼_수, 증발할_회원);
                        faults.끊는다();
                    })
                    .duringFault(() -> {
                        Awaitility.await().alias("레디스가 죽어 재료가 낡는다")
                                .atMost(기다림).until(holder::isDataStale);
                        줄_도착[1] = 뒷단까지_센다(COUPON, () -> 장애중_줄_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 3_000)));
                    })
                    .recover(() -> faults.붙인다())
                    .afterRecovery(() -> {
                        Awaitility.await().alias("스냅샷이 다시 닿는다")
                                .atMost(기다림).until(() -> !holder.isDataStale());
                        회복_뒤_자리.putAll(자리들(살아남을_회원, 보낼_수));
                        심은_뒤_자리.putAll(QueueSeed.자리들(연결, COUPON, 줄_선_사람));
                        회복_임계[0] = 임계를_읽는다();
                        증발_뒤_자리.putAll(자리들(증발할_회원, 보낼_수));
                        한산한_도착[1] = 뒷단까지_센다(한산한_쿠폰[1], () -> 회복_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰[1], 한산한_보낼_수, 200)));
                        // **증발한 사람은 다시 선다.** 그때 받는 자리가 살아남은
                        // 사람보다 뒤라야 한다 — 그것이 이 시나리오의 판정이다.
                        줄_도착[2] = 뒷단까지_센다(COUPON, () -> 재등록_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 증발할_회원)));
                        재등록_자리.putAll(자리들(증발할_회원, 보낼_수));
                    })
                    .assertEntry(() -> RecoveryCriteria.violations(
                            대조군이_받았다("정상", 정상_상태),
                            대조군이_뒷단까지_갔다("정상", 한산한_도착[0]),
                            줄에_세웠다("정상", 정상_줄_상태, 보낼_수),
                            줄을_추월하지_않았다("정상", 줄_도착[0]),
                            줄이_서_있었다(살아남을_자리)))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            // **여기서 fail-open 이 열리는 것은 맞는 동작이다.**
                            // 레디스가 죽으면 줄에 세울 방법 자체가 없어, F1 의
                            // 세 번째 규칙이 적용되는 유일한 구간이다. 그 통과가
                            // 상한 안인지는 C1 이 재고 여기서 다시 안 잰다.
                            //
                            // 이 시나리오가 유지 구간에 요구하는 것은 하나다 —
                            // 판정이 멎지 않는다. 전원 5xx 면 회복 구간의 관측이
                            // 무엇을 뜻하는지 알 수 없다.
                            전면_차단이_아니었다(장애중_줄_상태),
                            낡음에_들어갔다()))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            대조군이_받았다("회복", 회복_상태),
                            대조군이_뒷단까지_갔다("회복", 한산한_도착[1]),
                            // **임계는 뒤로 안 간다.** 유실 꼬리에 배분의
                            // 마지막 상승이 걸리면 되돌아갈 수 있고, 그러면
                            // 이미 통과한 사람이 다시 대기가 된다.
                            임계가_뒤로_안_갔다(진입_임계[0], 회복_임계[0]),
                            줄을_추월하지_않았다("재등록", 줄_도착[2]),
                            보고가_다시_돈다(보고기[0]),
                            // **영속이 실제로 일했는가.** 아무도 안 남았으면
                            // 통째로 날아간 것이라 C1 을 다시 잰 것이다.
                            영속이_일했다(살아남을_자리, 회복_뒤_자리),
                            // **증발이 실제로 일어났는가.** 마지막 창이 다
                            // 살아남으면 재등록 갈래가 한 번도 안 돌아, 아래
                            // 추월 판정이 재는 척하는 자리가 된다.
                            증발이_일어났다(증발_뒤_자리),
                            // RC5 — 남은 사람의 자리는 안 움직인다.
                            //
                            // **추리지 않는다.** 사라진 사람을 비교 집합에서
                            // 빼면 이 판정이 정의상 안 깨진다. 내려쓴 무리와
                            // 심어 둔 줄은 전원 생존이 요구사항이다.
                            RecoveryCriteria.seatLost(살아남을_자리, 회복_뒤_자리),
                            RecoveryCriteria.seatLost(심은_자리, 심은_뒤_자리),
                            줄에_세웠다("재등록", 재등록_상태, 보낼_수),
                            // **추월 0.** 재등록자의 자리는 남은 사람보다 뒤다.
                            재등록자가_추월하지_않았다(회복_뒤_자리, 재등록_자리),
                            뒷단.중복_수신이_없다()))
                    .run();
        } finally {
            연결.close();
        }
    }

    /** 이 뒤의 쓰기는 파일에 안 남는다. 파일 자체는 그대로다. */
    private void 덧붙이기를_멈춘다() {
        try (StatefulRedisConnection<String, String> 연결 = faults.연결한다()) {
            연결.sync().configSet("appendonly", "no");
        }
    }

    /** 여기까지의 쓰기를 디스크에 못 박는다. 안 하면 재는 것이 통째 유실이 된다. */
    private void 디스크에_내려쓴다() {
        try (StatefulRedisConnection<String, String> 연결 = faults.연결한다()) {
            연결.sync().bgrewriteaof();
            Awaitility.await().alias("AOF 재작성이 끝난다").atMost(기다림).until(() ->
                    !연결.sync().info("persistence").contains("aof_rewrite_in_progress:1"));
        }
    }

    /** 배분이 올린 임계. 없으면 0 이다. */
    private long 임계를_읽는다() {
        String 값 = redis.opsForValue()
                .get(RedisKeys.admitted(COUPON, 1, 0)).block(기다림);
        return 값 == null ? 0 : (long) Double.parseDouble(값);
    }

    /** 살아 있는 신호를 통째로 지운다. 유실 꼬리가 실제로 만드는 모양이다. */
    private void 생존_신호를_지운다() {
        try (StatefulRedisConnection<String, String> 연결 = faults.연결한다()) {
            연결.sync().del(RedisKeys.alive(COUPON, 1, 0));
        }
    }

    /** 낡음에 안 들어갔으면 회복을 기다린 적도 없다 — 이 판은 장애가 아니다. */
    private Optional<String> 낡음에_들어갔다() {
        return holder.isDataStale() ? Optional.empty()
                : Optional.of("전제 — 유지 구간에 재료가 낡지 않았다");
    }

    /** 상태 코드만 보면 뒷단에 안 가고 200 을 낸 판을 못 가른다. */
    private Optional<String> 대조군이_뒷단까지_갔다(String 구간, long 도착) {
        return 도착 == 한산한_보낼_수 ? Optional.empty()
                : Optional.of("%s — 대조 쿠폰이 %d 건만 뒷단까지 갔다 (보낸 %d)"
                        .formatted(구간, 도착, 한산한_보낼_수));
    }

    /** 되돌아간 임계 위에서는 이미 통과한 사람이 다시 대기가 된다 (불변식 3). */
    private Optional<String> 임계가_뒤로_안_갔다(long 진입, long 회복) {
        if (진입 <= 0) {
            return Optional.of("전제 — 진입 구간에 임계가 안 올랐다 (%d)".formatted(진입));
        }
        return 회복 >= 진입 ? Optional.empty()
                : Optional.of("임계가 %d 에서 %d 로 뒤로 갔다".formatted(진입, 회복));
    }

    /**
     * 보고가 다시 도는가. <b>멎으면 회복 구간이 하한 크레딧에서 돈다</b> — 진입
     * 구간과 다른 판이 되어 두 구간을 비교하는 것 자체가 성립하지 않는다.
     * 장애 중에 터지는 것은 정상이라 수를 안 본다. 회복 뒤에 값이 있는지를 본다.
     */
    private Optional<String> 보고가_다시_돈다(BackendReports 보고서) {
        return 보고서.신선한_보고().containsKey("c12-be") ? Optional.empty()
                : Optional.of("전제 — 회복 뒤에 신선한 가용량 보고가 없다 (터진 판 %d)"
                        .formatted(뛰다_터진_수.get()));
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

    /**
     * 전면 차단이 아니다. 레디스가 죽은 것이 <b>모두를 5xx 로 돌려보낼 이유는
     * 아니다</b> — 줄이 있으니 추월은 안 시키되, 아예 판정을 멈추면 그것도 장애다.
     */
    private Optional<String> 전면_차단이_아니었다(List<Integer> 상태) {
        if (상태.size() != 보낼_수) {
            return Optional.of("유지 — %d 건을 보냈는데 %d 건만 관측됐다"
                    .formatted(보낼_수, 상태.size()));
        }
        long 답한_것 = 상태.stream().filter(status -> status < 500).count();
        return 답한_것 > 0 ? Optional.empty()
                : Optional.of("유지 — %d 건이 전부 5xx 다: %s".formatted(상태.size(), 상태));
    }

    private Optional<String> 줄이_서_있었다(Map<String, Double> 자리) {
        return 자리.size() == 보낼_수 ? Optional.empty()
                : Optional.of("전제 — 줄에 %d 명만 서 있다 (보낸 %d)"
                        .formatted(자리.size(), 보낼_수));
    }

    /**
     * 영속이 실제로 일했는가. <b>내려쓴 무리는 전원 남아야 한다</b> — 한 명이라도
     * 없으면 파일에 있던 것을 잃은 것이고, 그건 증발이 아니라 유실이다.
     */
    private Optional<String> 영속이_일했다(Map<String, Double> 전, Map<String, Double> 후) {
        return 후.size() == 전.size() ? Optional.empty()
                : Optional.of("내려쓴 %d 명 중 %d 명만 남았다".formatted(전.size(), 후.size()));
    }

    /**
     * 증발이 실제로 일어났는가. <b>마지막 창이 다 살아남으면</b> 재등록이 아니라
     * 재조회가 되어, 추월 판정이 원래 자리를 다시 읽는 것에 지나지 않는다.
     */
    private Optional<String> 증발이_일어났다(Map<String, Double> 증발_뒤) {
        return 증발_뒤.isEmpty() ? Optional.empty()
                : Optional.of("전제 — 마지막 창의 %d 명이 그대로 남았다. 증발이 없다"
                        .formatted(증발_뒤.size()));
    }

    /**
     * <b>재등록자가 남은 사람을 추월하지 않는다</b> (불변식 3·4). 증발은 허용해도
     * 새로 받은 자리가 남은 사람보다 앞서면 줄이 뒤집힌 것이다.
     */
    private Optional<String> 재등록자가_추월하지_않았다(Map<String, Double> 남은,
            Map<String, Double> 재등록) {
        if (재등록.size() != 보낼_수) {
            return Optional.of("전제 — 재등록이 %d 건만 자리를 받았다 (보낸 %d)"
                    .formatted(재등록.size(), 보낼_수));
        }
        double 남은_뒤 = 남은.values().stream().mapToDouble(Double::doubleValue).max()
                .orElse(Double.NEGATIVE_INFINITY);
        return 재등록.entrySet().stream()
                .filter(entry -> entry.getValue() <= 남은_뒤)
                .findFirst()
                .map(entry -> "재등록한 %s 가 자리 %s 를 받아 남은 사람(%s)을 앞섰다"
                        .formatted(entry.getKey(), entry.getValue(), 남은_뒤));
    }
}

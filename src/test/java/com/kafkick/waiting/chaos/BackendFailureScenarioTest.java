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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * C9 — 뒷단이 5xx 를 낸다 (8.3.4 · 5절).
 *
 * <p>C8 과 갈리는 것은 <b>스텁이 내는 값</b>이다 — 저쪽은 응답이 아예 안 오고
 * 여기는 오긴 오는데 500 이다. <b>서킷은 여기서 안 잰다</b>(CY-841). 재는 것은
 * 하나다 — 줄에 선 사람의 자리는 레디스에 있으니 뒷단이 망가져도 그대로여야 한다.
 */
@Tag("chaos")
// **컨텍스트를 닫는다.** 이 클래스는 배분·리더 루프를 켜는데, 안 닫으면 캐시에
// 남아 계속 돈다. 컨테이너가 반납한 고정 포트를 다음 시나리오가 다시 받으면
// 그 좀비의 재연결이 남의 시험 레디스에 리더 락과 스냅샷을 쓴다.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class BackendFailureScenarioTest {

    private static final String COUPON = "c9-queued";

    /**
     * 줄이 없는 대조 쿠폰. <b>구간마다 따로 둔다</b> — 하나를 나눠 쓰면 쿠폰별
     * 초당 예산이 앞 구간에서 소진돼, 뒤 구간의 202 가 뒷단이 살았는지와 무관한
     * 값이 된다. 그러면 대조군이 대조군인지가 초 경계에 달린다.
     */
    private static final String[] 한산한_쿠폰 = {"c9-idle-normal", "c9-idle-fault",
            "c9-idle-recovered"};

    /** 대조군에 보내는 수. 크레딧 안쪽이어야 줄이 안 선다. */
    private static final int 한산한_보낼_수 = 2;

    private static final int 줄_선_사람 = 5;

    private static final int 보낼_수 = 10;

    private static final Pattern POSITION = Pattern.compile("\"position\":(-?\\d+)");

    private static final Duration 기다림 = Duration.ofSeconds(20);

    /** 심어 둔 줄의 생존 신호 수명. 시험 수명보다 길어야 스위퍼가 살아 있다고 읽는다. */
    private static final Duration 생존_수명 = Duration.ofMinutes(5);

    /** 뒷단이 보고하는 여유. 바닥값을 크게 넘겨야 대조군 예산이 구간을 버틴다. */
    private static final long 가용량 = 2_000;

    private static final ScheduledExecutorService 보고 =
            Executors.newSingleThreadScheduledExecutor();

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
        return 물어본다(couponId, member).상태();
    }

    /**
     * 상태와 순번을 함께 받는다. <b>사용자가 보는 것은 score 가 아니라
     * 순번이다</b> — 자리를 그대로 두고 순번만 0 으로 만들어도 자리 판정은
     * 통과한다.
     */
    private Reply 물어본다(String couponId, int member) {
        var 결과 = 클라이언트().post()
                .uri("/api/v1/coupons/" + couponId + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .exchange()
                .returnResult(String.class);
        String 본문 = 결과.getResponseBody().blockFirst(기다림);
        return new Reply(결과.getStatus().value(), 순번을_뽑는다(본문));
    }

    /** 상태 코드와, 202 면 응답이 알려 준 순번. 아니면 -1. */
    private record Reply(int 상태, long 순번) {
    }

    private static long 순번을_뽑는다(String 본문) {
        if (본문 == null) {
            return -1;
        }
        Matcher matcher = POSITION.matcher(본문);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : -1;
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

    private void 재료를_심는다(StatefulRedisConnection<String, String> 연결) {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "50").block(기다림);
        for (String 쿠폰 : 한산한_쿠폰) {
            redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, 쿠폰).block(기다림);
            redis.opsForValue().set(RedisKeys.stock(쿠폰), "100000").block(기다림);
        }
        QueueSeed.줄을_세운다(연결, COUPON, 줄_선_사람, 생존_수명);
    }

    /**
     * 게이트웨이를 지나 실제로 줄에 선 사람들의 자리.
     *
     * <p><b>심어 둔 값이 아니라 등록 스크립트가 매긴 값을 본다.</b> 심은 쪽은
     * 이 시험 내내 어떤 프로덕션 코드도 안 읽어, 자리 유실 판정이 레디스를
     * 시험하는 자리가 된다.
     */
    private Map<String, Double> 등록된_자리들(int 시작_회원, int 수) {
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

    /**
     * 대조군이 <b>성공 응답을 받았는가.</b>
     *
     * <p>5xx 만 세면 429 나 410 이 통과한다 — 뒷단까지 갔다는 것과 사용자가 쓸
     * 수 있는 응답을 받았다는 것은 다르다. 뒷단이 살아 있는 구간이므로 200 이다.
     */
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

    @Test
    @DisplayName("C9_뒷단이_오백을_내도_순번이_남는다")
    void C9_뒷단이_오백을_내도_순번이_남는다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        try {
            List<Integer> 정상_상태 = new ArrayList<>();
            List<Integer> 장애중_줄_상태 = new ArrayList<>();
            List<Integer> 회복_상태 = new ArrayList<>();
            List<Integer> 회복_줄_상태 = new ArrayList<>();
            List<Integer> 정상_줄_상태 = new ArrayList<>();
            Map<String, Double> 장애_전_자리 = new LinkedHashMap<>();
            Map<String, Double> 회복_뒤_자리 = new LinkedHashMap<>();
            long[] 줄_도착 = new long[3];
            List<Integer> 새로고침_상태 = new ArrayList<>();
            Map<String, Double> 등록_전_자리 = new LinkedHashMap<>();
            Map<String, Double> 새로고침_뒤_자리 = new LinkedHashMap<>();
            long[] 한산한_도착 = new long[3];
            int[] 뒷단_상태 = new int[3];
            List<Integer> 한산한_장애중 = new ArrayList<>();
            long[] 새로고침_도착 = new long[1];
            List<Long> 평시_순번 = new ArrayList<>();
            List<Long> 새로고침_순번 = new ArrayList<>();
            RankTracker 순번 = new RankTracker();

            ChaosScenario.named("C9 뒷단 5xx")
                    .baseline(() -> {
                        재료를_심는다(연결);
                        // **뒷단 여유를 보고한다.** 안 하면 크레딧이 바닥값이라
                        // 대조 쿠폰의 통과 예산이 세 건이고, 세 구간이 그것을
                        // 나눠 쓰다 뒤 구간이 줄을 선다 — 뒷단이 살았는지와
                        // 무관한 202 가 되어 회복 판정이 도달 불가다.
                        BackendReports 보고서 = BackendReports.실시계로(연결,
                                Duration.ofSeconds(3));
                        보고.scheduleAtFixedRate(() -> 보고서.보고한다("c9-be", 가용량),
                                0, 500, TimeUnit.MILLISECONDS);
                        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                        장애_전_자리.putAll(QueueSeed.자리들(연결, COUPON, 줄_선_사람));
                        뒷단_상태[0] = 뒷단이_직접_답한다();
                        한산한_도착[0] = 뒷단까지_센다(한산한_쿠폰[0], () -> 정상_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰[0], 한산한_보낼_수, 1_100)));
                        줄_도착[0] = 뒷단까지_센다(COUPON, () -> {
                            for (int i = 0; i < 보낼_수; i++) {
                                Reply 답 = 물어본다(COUPON, 1_000 + i);
                                정상_줄_상태.add(답.상태());
                                평시_순번.add(답.순번());
                            }
                        });
                        등록_전_자리.putAll(등록된_자리들(1_000, 보낼_수));
                        for (int i = 0; i < 보낼_수; i++) {
                            String member = String.valueOf(1_000 + i);
                            순번.waiting(member, 평시_순번.get(i),
                                    (long) (double) 등록_전_자리.getOrDefault(member, 0.0));
                        }
                    })
                    .inject(() -> 실패한다.set(true))
                    .duringFault(() -> {
                        뒷단_상태[1] = 뒷단이_직접_답한다();
                        // **장애 중 새로고침 연타.** 프로덕션에서 가장 흔한
                        // 행동이고, 등록 스크립트의 재등록 갈래를 밟는 유일한
                        // 길이다. 자리를 그대로 돌려주지 않으면 그 사람은
                        // 맨 뒤로 밀린다 — 순번 역행이다.
                        새로고침_도착[0] = 뒷단까지_센다(COUPON, () -> {
                            for (int i = 0; i < 보낼_수; i++) {
                                Reply 답 = 물어본다(COUPON, 1_000 + i);
                                새로고침_상태.add(답.상태());
                                순번.waiting(String.valueOf(1_000 + i), 답.순번(),
                                        (long) (double) 등록_전_자리
                                                .getOrDefault(String.valueOf(1_000 + i), 0.0));
                                새로고침_순번.add(답.순번());
                            }
                        });
                        한산한_도착[1] = 뒷단까지_센다(한산한_쿠폰[1], () -> 한산한_장애중.addAll(
                                여러_번_시도한다(한산한_쿠폰[1], 한산한_보낼_수, 2_100)));
                        줄_도착[1] = 뒷단까지_센다(COUPON, () -> 장애중_줄_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 2_000)));
                    })
                    .recover(() -> 실패한다.set(false))
                    .afterRecovery(() -> {
                        뒷단_상태[2] = 뒷단이_직접_답한다();
                        한산한_도착[2] = 뒷단까지_센다(한산한_쿠폰[2], () -> 회복_상태.addAll(
                                여러_번_시도한다(한산한_쿠폰[2], 한산한_보낼_수, 3_100)));
                        줄_도착[2] = 뒷단까지_센다(COUPON, () -> 회복_줄_상태.addAll(
                                여러_번_시도한다(COUPON, 보낼_수, 3_000)));
                        회복_뒤_자리.putAll(QueueSeed.자리들(연결, COUPON, 줄_선_사람));
                        새로고침_뒤_자리.putAll(등록된_자리들(1_000, 보낼_수));
                    })
                    .assertEntry(() -> RecoveryCriteria.violations(
                            대조군이_받았다("정상", 정상_상태),
                            // 정상 구간에도 건다. 여기에 없으면 줄이 선 쿠폰이
                            // 평시에 뒷단으로 새도 아무 판정이 안 깨진다.
                            줄에_세웠다("정상", 정상_줄_상태, 보낼_수),
                            줄을_추월하지_않았다("정상", 줄_도착[0]),
                            줄이_서_있었다(등록_전_자리, 보낼_수, "등록"),
                            줄이_서_있었다(장애_전_자리, 줄_선_사람, "심은"),
                            한산한_쿠폰이_평시에_통했다(한산한_도착[0])))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            // **주입이 정말 걸렸는가.** 없으면 주입을 안 해도
                            // 전 판정이 통과한다 — 줄이 선 쿠폰은 뒷단에 아예
                            // 안 가므로 장애 유무로 관측이 안 갈린다.
                            뒷단이_망가졌다(뒷단_상태[0], 뒷단_상태[1]),
                            // **게이트웨이가 500 을 겪었는가.** 뒷단을 직접
                            // 찌른 것은 스텁이 망가진 증거일 뿐이다. 대조군이
                            // 게이트웨이를 지나 뒷단까지 가고 그 500 을 그대로
                            // 받아야, 이 시나리오가 재려는 경로가 열린 것이다.
                            대조군이_통했다("유지", 한산한_도착[1]),
                            오백이_그대로_나갔다(한산한_장애중),
                            // **줄에 세웠는가.** 5xx 만 보면 전원이 429 로
                            // 거절돼도 통과한다 — 아무도 자리를 못 받은 판과
                            // 모두가 받은 판이 같은 초록이 된다.
                            줄에_세웠다("유지", 장애중_줄_상태, 보낼_수),
                            줄에_세웠다("새로고침", 새로고침_상태, 보낼_수),
                            // **줄이 선 쿠폰이라 뒷단까지 가면 추월이다.**
                            줄을_추월하지_않았다("유지", 줄_도착[1]),
                            줄을_추월하지_않았다("새로고침", 새로고침_도착[0])))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            뒷단이_돌아왔다(뒷단_상태[2]),
                            대조군이_받았다("회복", 회복_상태),
                            // **통과 경로가 되살아났는가.** 이것이 없으면
                            // 서킷이 영영 안 닫히는 게이트웨이가 통과한다 —
                            // 전 요청이 줄에 서서 뒷단 도착이 0 이 되는데,
                            // 다른 판정은 그 0 을 추월 없음으로 읽는다.
                            대조군이_통했다("회복", 한산한_도착[2]),
                            줄에_세웠다("회복", 회복_줄_상태, 보낼_수),
                            // RC5 — 장애 중 새로고침한 사람이 자리를 지켰는가.
                            RecoveryCriteria.seatLost(등록_전_자리, 새로고침_뒤_자리),
                            // RC2 — 사용자가 본 순번이 뒤로 안 갔는가. 자리를
                            // 그대로 두고 순번만 0 으로 만들어도 위가 통과한다.
                            순번이_안_밀렸다(순번),
                            // **같은 순번을 돌려줘야 한다.** 역행만 보면 0 으로
                            // 뭉개는 것을 놓친다 — 앞으로 당겨진 것도 추월이다.
                            순번을_그대로_돌려줬다(평시_순번, 새로고침_순번),
                            줄을_추월하지_않았다("회복", 줄_도착[2]),
                            // 약한 쪽이다. 심어 둔 이름은 스위퍼 말고는 아무도
                            // 안 읽는다 — 본체는 위의 등록 회원 판정이다.
                            RecoveryCriteria.seatLost(장애_전_자리, 회복_뒤_자리),
                            뒷단.중복_수신이_없다()))
                    .run();

        } finally {
            실패한다.set(false);
            연결.close();
        }
    }

    private long 뒷단까지_센다(String couponId, Runnable 배치) {
        long 전 = 뒷단.받은_수(couponId);
        배치.run();
        return 뒷단.받은_수(couponId) - 전;
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

    /**
     * 줄이 선 쿠폰이면 202 로 자리를 받아야 한다. 그 밖은 못 선 것이다.
     *
     * <p>보낸 수를 함께 받는다. 단계가 중간에 터져 목록이 비면 "위반 없음" 과
     * 구분이 안 되는데, 뼈대가 예외를 삼키므로 그 경로는 실제로 열려 있다.
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

    /** 줄이 서 있어야 이 시나리오의 표제가 성립한다. */
    private Optional<String> 줄이_서_있었다(Map<String, Double> 자리, int 기대, String 이름) {
        return 자리.size() == 기대 ? Optional.empty()
                : Optional.of("전제 — %s 줄에 %d 명만 서 있다 (기대 %d)"
                        .formatted(이름, 자리.size(), 기대));
    }

    /** 길이 애초에 막혀 있으면 나머지 구간의 도착 수에 뜻이 없다. */
    private Optional<String> 한산한_쿠폰이_평시에_통했다(long 도착) {
        return 대조군이_통했다("정상", 도착).map(why -> "전제 — " + why);
    }

    /**
     * 대조 쿠폰이 게이트웨이를 지나 뒷단까지 갔는가.
     *
     * <p>구간마다 쿠폰이 달라 예산이 새로 찬다. 하나를 나눠 쓰면 앞 구간이
     * 예산을 먹고, 뒤 구간의 202 가 뒷단이 살았는지와 무관한 값이 된다.
     */
    private Optional<String> 대조군이_통했다(String 구간, long 도착) {
        return 도착 == 한산한_보낼_수 ? Optional.empty()
                : Optional.of("%s — 대조 쿠폰이 %d 건만 뒷단까지 갔다 (보낸 %d)"
                        .formatted(구간, 도착, 한산한_보낼_수));
    }

    /**
     * 뒷단의 500 이 그대로 나갔는가. <b>이건 전제이지 위반이 아니다</b> — 뒷단이
     * 낸 것을 흘리는 것이 맞는 동작이고, 한 건도 없으면 게이트웨이가 장애를
     * 아예 안 겪은 것이다.
     */
    private Optional<String> 오백이_그대로_나갔다(List<Integer> 상태) {
        long 오백 = 상태.stream().filter(status -> status >= 500).count();
        return 오백 == 한산한_보낼_수 ? Optional.empty()
                : Optional.of("전제 — 유지 구간에 5xx 가 %d 건이다 (보낸 %d): %s"
                        .formatted(오백, 한산한_보낼_수, 상태));
    }

    /**
     * 새로고침이 <b>같은 순번</b>을 돌려줬는가.
     *
     * <p>줄에서 빠지는 사람이 없는 구간이라 앞에 선 수가 안 변한다. 0 으로
     * 뭉개거나 경계를 한 칸 밀면 여기서 걸린다 — 자리는 그대로라 자리 판정은
     * 통과하고, 역행만 보는 판정도 앞으로 당겨진 것을 안 잡는다.
     */
    private Optional<String> 순번을_그대로_돌려줬다(List<Long> 평시, List<Long> 새로고침) {
        if (평시.size() != 보낼_수 || 새로고침.size() != 보낼_수) {
            return Optional.of("전제 — 순번을 %d·%d 건만 봤다 (보낸 %d)"
                    .formatted(평시.size(), 새로고침.size(), 보낼_수));
        }
        for (int i = 0; i < 보낼_수; i++) {
            if (!평시.get(i).equals(새로고침.get(i))) {
                return Optional.of("%d 번째가 순번 %d 에서 %d 로 바뀌었다 (평시 %s · 새로고침 %s)"
                        .formatted(i, 평시.get(i), 새로고침.get(i), 평시, 새로고침));
            }
        }
        return Optional.empty();
    }

    /** RC2 — 사용자가 본 순번이 뒤로 안 갔는가. 관측이 없으면 그것도 위반이다. */
    private Optional<String> 순번이_안_밀렸다(RankTracker 순번) {
        List<String> 밀림 = 순번.regressions();
        return 밀림.isEmpty() ? Optional.empty() : Optional.of(String.join(" · ", 밀림));
    }

    private Optional<String> 줄을_추월하지_않았다(String 구간, long 도착) {
        return 도착 == 0 ? Optional.empty()
                : Optional.of("%s — 줄이 선 쿠폰에서 %d 건이 뒷단까지 갔다".formatted(구간, 도착));
    }
}

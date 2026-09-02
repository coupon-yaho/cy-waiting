package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * X1 — 리더가 죽은 채로 레디스까지 멈춘다 (8.3.6 · 5절).
 *
 * <p>C4 는 줄에 세울 수 있어 <b>추월 0</b> 이 답이었다. 여기서는 줄에 세울 수가
 * 없다 — F1 셋째 줄이 적용되는 유일한 상황이고, 그때만 통과시키되 <b>상한 안</b>
 * 이어야 한다.
 */
// **C1 이 못 잰 자리를 여기서 잰다.** 저쪽은 한산한 쿠폰 하나로 재는데 그 쿠폰은
// fail-open 경로를 안 거친다 — "상한을 재려면 줄이 선 재료가 필요하다" 고 저쪽
// 주석이 적어 두었다. 여기는 줄 선 쿠폰과 한산한 쿠폰을 나란히 둔다.
//
// **영속을 켜고 띄운다.** C1 은 `--appendonly no` 라 끊었다 붙이면 줄이 통째로
// 사라져 RC5 를 구조적으로 못 쟀다. 여기서는 줄이 살아남으므로 자리 보존을 잰다.
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class LeaderAndRedisLostScenarioTest {

    /** 줄이 선 쿠폰. 이 쿠폰의 통과가 fail-open 상한을 재는 자리다. */
    private static final String 줄_선_쿠폰 = "x1-queued";

    /**
     * 줄이 없는 쿠폰. <b>양성 대조다.</b>
     *
     * <p>이쪽이 계속 통과해야 게이트웨이가 살아 있는 것이고, 그래야 저쪽의
     * 상한이 "다 막혔다" 가 아니라 "상한이 물렸다" 는 뜻이 된다.
     */
    private static final String 한산한_쿠폰 = "x1-idle";

    private static final String 죽은_리더 = "x1-dead-leader";

    /** 죽은 리더가 쥐고 갈 리스. 낡음 임계보다 길어야 유지 구간이 열린다. */
    private static final Duration 죽은_리스 = Duration.ofSeconds(60);

    private static final Duration 기다림 = Duration.ofSeconds(30);

    private static final int 줄_선_사람 = 5;

    /**
     * 줄 선 쿠폰에 한 구간에 보내는 수.
     *
     * <p><b>상한보다 훨씬 커야 한다.</b> 상한 안이면 "상한 안이다" 라는 판정이
     * 아무것도 안 잰다 — 리미터를 들어내도 통과한다.
     */
    private static final int 보낼_수 = 40;

    /** 대조군에 보내는 수. 크레딧 안쪽이어야 한 번 서면 영영 못 나오는 일이 없다. */
    private static final int 한산한_보낼_수 = 2;

    /** 낡음이 걷힐 때까지의 한계. 그동안 fail-open 이 열려 있다. */
    private static final Duration 낡음이_걷힐_한계 = Duration.ofSeconds(5);

    private static final Map<String, AtomicLong> 뒷단이_받은_수 = new ConcurrentHashMap<>();

    private static final DisposableServer 뒷단 = HttpServer.create()
            .port(0)
            .handle((request, response) -> {
                뒷단이_받은_수.computeIfAbsent(쿠폰을_뽑는다(request.uri()),
                        id -> new AtomicLong()).incrementAndGet();
                return response.status(HttpStatus.OK.value()).send();
            })
            .bindNow();

    private static String 쿠폰을_뽑는다(String uri) {
        int 시작 = uri.indexOf("/coupons/");
        if (시작 < 0) {
            return "?";
        }
        String 뒤 = uri.substring(시작 + "/coupons/".length());
        int 끝 = 뒤.indexOf('/');
        return 끝 < 0 ? 뒤 : 뒤.substring(0, 끝);
    }

    private static long 받은_수(String couponId) {
        AtomicLong 계수 = 뒷단이_받은_수.get(couponId);
        return 계수 == null ? 0 : 계수.get();
    }

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        // 영속을 켠다 — 끊었다 붙여도 줄이 남아야 자리 보존을 잰다.
        faults = RedisFaults.영속으로_시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.url", faults::주소);
    }

    @AfterAll
    static void 내린다() {
        뒷단.disposeNow();
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
    private QueueToken tokens;

    @Autowired
    private Clock clock;

    /** 장애 전후의 자리. RC5 가 이 둘을 비교한다. */
    private Map<String, Double> 장애_전_자리 = Map.of();

    private Map<String, Double> 회복_뒤_자리 = Map.of();

    private Instant 회복을_기다린_시각;

    private Duration 낡음이_걷히기까지;

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build();
    }

    private void 재료를_심는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, 줄_선_쿠폰, 한산한_쿠폰).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(줄_선_쿠폰), "50").block(기다림);
        redis.opsForValue().set(RedisKeys.stock(한산한_쿠폰), "100000").block(기다림);
        for (int i = 0; i < 줄_선_사람; i++) {
            redis.opsForZSet()
                    .add(RedisKeys.queue(줄_선_쿠폰, 1, 0), "q" + i, 100 + i)
                    .block(기다림);
        }
    }

    private int 발급을_시도한다(String couponId, int member) {
        return 클라이언트().post()
                .uri("/api/v1/coupons/" + couponId + "/issue")
                .header("X-Member-Id", String.valueOf(member))
                .header("X-Member-Grade", "GOLD")
                .header("Queue-Token",
                        tokens.issue(couponId, String.valueOf(member), clock.instant()))
                .exchange()
                .returnResult(Void.class)
                .getStatus()
                .value();
    }

    private List<Integer> 여러_번_시도한다(String couponId, int 횟수, int 시작_회원) {
        List<Integer> 상태 = new ArrayList<>();
        for (int i = 0; i < 횟수; i++) {
            상태.add(발급을_시도한다(couponId, 시작_회원 + i));
        }
        return 상태;
    }

    /**
     * 같은 초에 몰아친다.
     *
     * <p><b>순서대로 보내면 상한을 못 잰다.</b> 레디스가 죽은 동안 요청 하나가
     * 줄 등록 시도에서 600ms 를 쓰므로, 40 건을 줄 세우면 24 초에 걸친다 —
     * 초당 예산이 그 사이 스물네 번 다시 차서 전부 통과한다. 실제로 그렇게
     * 재다가 "상한이 안 물렸다" 를 봤다.
     */
    private List<Integer> 한꺼번에_시도한다(String couponId, int 횟수, int 시작_회원) {
        ExecutorService 일꾼 = Executors.newFixedThreadPool(횟수);
        CountDownLatch 출발 = new CountDownLatch(1);
        try {
            List<Future<Integer>> 결과 = new ArrayList<>();
            for (int i = 0; i < 횟수; i++) {
                int member = 시작_회원 + i;
                결과.add(일꾼.submit(() -> {
                    출발.await();
                    return 발급을_시도한다(couponId, member);
                }));
            }
            출발.countDown();
            List<Integer> 상태 = new ArrayList<>();
            for (Future<Integer> 하나 : 결과) {
                상태.add(하나.get(기다림.toSeconds(), TimeUnit.SECONDS));
            }
            return 상태;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("몰아치기가 끊겼다", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("몰아치기가 실패했다", e);
        } finally {
            일꾼.shutdownNow();
        }
    }

    private Map<String, Double> 자리들() {
        Map<String, Double> 자리 = new LinkedHashMap<>();
        for (int i = 0; i < 줄_선_사람; i++) {
            String member = "q" + i;
            Double score = redis.opsForZSet()
                    .score(RedisKeys.queue(줄_선_쿠폰, 1, 0), member).block(기다림);
            if (score != null) {
                자리.put(member, score);
            }
        }
        return 자리;
    }

    private long 잰다(String couponId, Runnable 배치) {
        long 전 = 받은_수(couponId);
        배치.run();
        return 받은_수(couponId) - 전;
    }

    /** 지금 재료가 허용하는 초당 fail-open 상한. 필터가 보는 것과 같은 값이다. */
    private long 초당_상한() {
        return (long) (AdmissionDecider.globalCap(holder.view().snapshot().meta()) * 0.5);
    }

    /** 초 경계를 넘긴다. 리미터의 창이 바뀌어야 앞 배치의 예산과 안 섞인다. */
    private void 다음_초를_기다린다() {
        long 지금 = clock.instant().getEpochSecond();
        Awaitility.await().atMost(기다림)
                .pollInterval(Duration.ofMillis(20))
                .until(() -> clock.instant().getEpochSecond() > 지금);
    }

    private void 죽은_리더가_락을_쥔다(LeaderFaults 락) {
        assertThat(락.죽은_리더가_넘겨받는다(죽은_리더, 죽은_리스)).isTrue();
        락.프로세스를_죽인다(죽은_리더);
    }

    /**
     * X1 — 리더가 죽고 레디스까지 멈춘다.
     *
     * <p>줄에 세울 수 없는 유일한 상황이다. 그때만 통과시키되 상한 안이어야 한다.
     */
    @Test
    @DisplayName("X1_리더가_죽은_채_레디스가_멈추면_상한_안에서만_연다")
    void X1_리더가_죽은_채_레디스가_멈추면_상한_안에서만_연다() {
        재료를_심는다();
        Awaitility.await().atMost(기다림).until(leadership::isLeader);
        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());

        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        LeaderFaults 락 = LeaderFaults.of(연결);
        죽은_리더가_락을_쥔다(락);

        List<Integer> 정상_상태 = new ArrayList<>();
        List<Integer> 장애중_상태 = new ArrayList<>();
        List<Integer> 회복_상태 = new ArrayList<>();
        List<Integer> 장애중_줄_상태 = new ArrayList<>();
        long[] 줄_쿠폰_도착 = new long[3];
        long[] 한산한_쿠폰_도착 = new long[3];
        long[] 상한 = new long[1];
        long[] 걸린_초 = new long[1];
        int[] 주입_직후_쿠폰 = new int[1];
        boolean[] 주입_직후_낡음 = new boolean[1];
        List<Integer> 회복_줄_상태 = new ArrayList<>();

        ChaosScenario.named("X1 리더 사망 + 레디스 정지")
                .baseline(() -> {
                    장애_전_자리 = 자리들();
                    한산한_쿠폰_도착[0] = 잰다(한산한_쿠폰,
                            () -> 정상_상태.addAll(
                                    여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 1_100)));
                    줄_쿠폰_도착[0] = 잰다(줄_선_쿠폰,
                            () -> 여러_번_시도한다(줄_선_쿠폰, 한산한_보낼_수, 1_500));
                    assertThat(정상_상태).as("전제 — 한산한 쿠폰은 5xx 없이 답한다")
                            .noneMatch(status -> status >= 500);
                    assertThat(한산한_쿠폰_도착[0]).as("전제 — 한산한 쿠폰은 뒷단까지 간다")
                            .isPositive();
                    assertThat(줄_쿠폰_도착[0]).as("전제 — 줄이 선 쿠폰은 평시에 안 간다")
                            .isZero();
                })
                .inject(() -> {
                    // **리더는 이미 죽어 있다.** 여기서 레디스를 끊어 줄 등록까지
                    // 막는다 — 두 장애가 겹쳐야 F1 셋째 줄이 열린다.
                    faults.끊는다();
                    // **스냅샷을 지우지 않는다** (C1 진입 기준). 레디스가 죽었다고
                    // 재료를 버리면 판정이 그 자리에서 멎는다 — 낡은 재료로라도
                    // 판정을 이어 가는 것이 이 설계의 전제다.
                    주입_직후_쿠폰[0] = holder.current().coupons().size();
                    주입_직후_낡음[0] = holder.isDataStale();
                })
                .duringFault(() -> {
                    Awaitility.await().atMost(기다림).until(holder::isDataStale);
                    // 상한은 필터가 보는 재료에서 읽는다. 시험이 따로 계산하면
                    // 재료가 바뀌는 날 둘이 조용히 갈라진다.
                    상한[0] = 초당_상한();
                    long 낡기_전 = 받은_수(한산한_쿠폰);
                    장애중_상태.addAll(여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 2_000));
                    한산한_쿠폰_도착[1] = 받은_수(한산한_쿠폰) - 낡기_전;

                    // **대조군과 같은 초에 몰아치지 않는다.** 리미터가 노드 하나를
                    // 세는 것이라 예산을 같이 쓴다 — 대조군 두 건이 그 초의 2 를
                    // 먹으면 이 배치가 전멸해 "전면 차단" 으로 읽힌다. 실제로 봤다.
                    다음_초를_기다린다();
                    long 시작_초 = clock.instant().getEpochSecond();
                    줄_쿠폰_도착[1] = 잰다(줄_선_쿠폰,
                            () -> 장애중_줄_상태.addAll(
                                    한꺼번에_시도한다(줄_선_쿠폰, 보낼_수, 2_500)));
                    // 리미터가 초 단위다. 걸친 창의 수만큼 예산이 다시 찬다 —
                    // 경과 시간이 아니라 **넘은 초 경계**로 세야 한 창이 안 빠진다.
                    걸린_초[0] = clock.instant().getEpochSecond() - 시작_초 + 1;
                })
                .recover(() -> {
                    faults.붙인다();
                    락.lease를_만료시킨다(Duration.ofMillis(1));
                    // **컨테이너가 뜨는 시간은 안 센다.** 그건 도커를 재는 것이고,
                    // 여기서 잴 것은 레디스가 돌아온 뒤 게이트웨이가 fail-open 을
                    // 그만두기까지다 — 그동안 줄이 계속 추월당한다.
                    회복을_기다린_시각 = clock.instant();
                })
                .afterRecovery(() -> {
                    Awaitility.await().atMost(기다림).until(leadership::isLeader);
                    Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                    낡음이_걷히기까지 = Duration.between(회복을_기다린_시각, clock.instant());
                    long 회복_전 = 받은_수(한산한_쿠폰);
                    회복_상태.addAll(여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 3_000));
                    한산한_쿠폰_도착[2] = 받은_수(한산한_쿠폰) - 회복_전;
                    회복_뒤_자리 = 자리들();
                    줄_쿠폰_도착[2] = 잰다(줄_선_쿠폰,
                            () -> 회복_줄_상태.addAll(
                                    여러_번_시도한다(줄_선_쿠폰, 한산한_보낼_수, 3_500)));
                })
                // 진입 판정은 주입 직후다. 아직 아무것도 안 보냈으므로 잴 것이 없다.
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 레디스가 죽었다고 재료를 버리면 판정이 멎는다.
                        스냅샷을_안_지웠다(주입_직후_쿠폰[0]),
                        // 주입 직후는 아직 안 낡았다. 낡았으면 이 시나리오가 재려는
                        // 구간(신선한 재료 + 죽은 레디스)을 건너뛴 것이다.
                        주입_직후에_안_낡았다(주입_직후_낡음[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 양성 대조 — 한산한 쿠폰은 평시만큼 간다.
                        열려_있었다(한산한_쿠폰_도착[1], 한산한_쿠폰_도착[0]),
                        // **전면 차단이 아니다.** 줄에 못 세우면 그때만 연다.
                        줄_선_쿠폰도_열렸다(줄_쿠폰_도착[1]),
                        // **상한이 실제로 물렸다.** 보낸 것보다 적게 가야 한다.
                        상한이_물렸다(줄_쿠폰_도착[1]),
                        // 상한은 초당이므로 걸친 초 수를 곱한 것이 한계다.
                        상한_안이다(줄_쿠폰_도착[1], 상한[0], 걸린_초[0]),
                        // 넘친 몫은 되돌려 보낸다. 답을 못 받은 것은 아니다.
                        전부_답을_받았다(장애중_줄_상태),
                        레디스가_정말_죽었다()))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        판정이_멈추지_않았다(회복_상태),
                        열려_있었다(한산한_쿠폰_도착[2], 한산한_쿠폰_도착[0]),
                        // 레디스가 돌아오면 다시 줄에 세운다 — 추월이 끝난다.
                        줄을_추월하지_않았다(줄_쿠폰_도착[2]),
                        // **도착 0 만 보면 전원 5xx 도 통과다.** 줄로 갔는지를
                        // 응답으로 따로 본다.
                        줄에_다시_세웠다(회복_줄_상태),
                        RecoveryCriteria.slowVerdictReturn(
                                낡음이_걷히기까지, 낡음이_걷힐_한계),
                        // **RC5 — C1 이 구조적으로 못 잰 자리다.** 영속을 켰으므로
                        // 끊었다 붙여도 줄이 남아야 한다.
                        RecoveryCriteria.seatLost(장애_전_자리, 회복_뒤_자리)))
                // **RC1·RC2·RC4·RC6 은 여기서 안 잰다.** 뒷단 도착이 상한에 눌려
                // 표본이 적고, 줄 선 쿠폰은 순번을 안 돌려주므로 RC2 를 못 본다.
                .run();

        연결.close();
    }

    private Optional<String> 판정이_멈추지_않았다(List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("보낸 것이 없다 — 판정 중단을 잴 수 없다");
        }
        long 멈춘_것 = 상태.stream().filter(status -> status >= 500).count();
        return 멈춘_것 == 0 ? Optional.empty()
                : Optional.of("판정이 %d 건 멈췄다 (보낸 %d)".formatted(멈춘_것, 상태.size()));
    }

    /** 한산한 쿠폰이 평시만큼 간다. 여기가 막히면 아래 상한 판정이 아무것도 안 잰다. */
    private Optional<String> 열려_있었다(long 뒷단에_닿은_수, long 평시_도착) {
        return 뒷단에_닿은_수 == 평시_도착 ? Optional.empty()
                : Optional.of("한산한 쿠폰이 %d 건만 뒷단에 갔다 (평시 %d)"
                        .formatted(뒷단에_닿은_수, 평시_도착));
    }

    /**
     * <b>줄에 못 세우면 그때만 연다</b> (F1 셋째 줄).
     *
     * <p>0 이면 레디스 장애가 곧 전면 장애다. 게이트웨이가 존재할 이유가 없다.
     */
    private Optional<String> 줄_선_쿠폰도_열렸다(long 뒷단에_닿은_수) {
        return 뒷단에_닿은_수 > 0 ? Optional.empty()
                : Optional.of("줄에 못 세우는데 한 건도 안 통과했다 — 전면 차단이다");
    }

    /**
     * <b>상한이 실제로 물렸다.</b>
     *
     * <p>보낸 것이 전부 갔다면 상한이 없는 것과 같다. 리미터를 들어내도 초록인
     * 자리가 되지 않게, 보낸 수보다 적게 갔는지를 따로 본다.
     */
    private Optional<String> 상한이_물렸다(long 뒷단에_닿은_수) {
        return 뒷단에_닿은_수 < 보낼_수 ? Optional.empty()
                : Optional.of("보낸 %d 건이 전부 뒷단에 갔다 — 상한이 안 물렸다"
                        .formatted(뒷단에_닿은_수));
    }

    /** 초당 상한 × 걸친 초 수가 한계다. 이것을 넘으면 뒷단이 무너진다. */
    private Optional<String> 상한_안이다(long 뒷단에_닿은_수, long 초당, long 초) {
        long 한계 = 초당 * 초;
        return 뒷단에_닿은_수 <= 한계 ? Optional.empty()
                : Optional.of("%d 건이 뒷단에 갔다 — 초당 상한 %d × %d 초 = %d 이내여야 한다"
                        .formatted(뒷단에_닿은_수, 초당, 초, 한계));
    }

    /**
     * <b>넘친 몫도 답을 받는다.</b> 통과가 아니라 되돌려 보내는 것이라, 끊긴 것과
     * 구분해야 한다. 여기가 없으면 절반이 타임아웃돼도 "상한이 물렸다" 로 읽힌다.
     */
    private Optional<String> 전부_답을_받았다(List<Integer> 상태) {
        if (상태.size() != 보낼_수) {
            return Optional.of("%d 건만 답을 받았다 (보낸 %d)"
                    .formatted(상태.size(), 보낼_수));
        }
        long 엉뚱한_답 = 상태.stream()
                .filter(status -> status != 200 && status != 202 && status != 503)
                .count();
        return 엉뚱한_답 == 0 ? Optional.empty()
                : Optional.of("%d 건이 200·202·503 이 아니다: %s".formatted(엉뚱한_답, 상태));
    }

    /** 레디스가 돌아오면 줄에 세운다. 그때부터는 추월이 없어야 한다. */
    private Optional<String> 줄을_추월하지_않았다(long 뒷단에_닿은_수) {
        return 뒷단에_닿은_수 == 0 ? Optional.empty()
                : Optional.of("회복 뒤에도 줄이 선 쿠폰에서 %d 건이 뒷단까지 갔다 — 추월이다"
                        .formatted(뒷단에_닿은_수));
    }

    /** 장애가 정말 걸렸는지. 안 걸렸으면 위 판정 전부가 평시를 잰 것이다. */
    private Optional<String> 레디스가_정말_죽었다() {
        try {
            redis.opsForValue().get(RedisKeys.stock(줄_선_쿠폰)).block(Duration.ofSeconds(2));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        return Optional.of("레디스가 살아 있다 — 장애가 안 걸렸다");
    }

    /**
     * <b>레디스가 죽었다고 재료를 버리지 않는다</b> (C1 진입 기준).
     *
     * <p>버리면 판정이 그 자리에서 멎는다 — 낡은 재료로라도 판정을 이어 가는
     * 것이 이 설계의 전제다.
     */
    private Optional<String> 스냅샷을_안_지웠다(int 쿠폰_수) {
        return 쿠폰_수 >= 2 ? Optional.empty()
                : Optional.of("주입 직후 재료에 쿠폰이 %d 개다 — 둘이 그대로 있어야 한다"
                        .formatted(쿠폰_수));
    }

    /** 주입 직후는 아직 안 낡았다. 낡았으면 재려는 구간을 건너뛴 것이다. */
    private Optional<String> 주입_직후에_안_낡았다(boolean 낡음) {
        return 낡음 ? Optional.of("주입 직후에 이미 낡았다 — 신선한 재료 구간을 건너뛰었다")
                : Optional.empty();
    }

    /** 레디스가 돌아오면 줄로 간다. 202 가 아니면 그 사람은 줄에 못 선 것이다. */
    private Optional<String> 줄에_다시_세웠다(List<Integer> 상태) {
        if (상태.size() != 한산한_보낼_수) {
            return Optional.of("회복 — %d 건만 답을 받았다 (보낸 %d)"
                    .formatted(상태.size(), 한산한_보낼_수));
        }
        long 못_선_수 = 상태.stream().filter(status -> status != 202).count();
        return 못_선_수 == 0 ? Optional.empty()
                : Optional.of("회복 — %d 건이 줄에 못 섰다: %s".formatted(못_선_수, 상태));
    }
}

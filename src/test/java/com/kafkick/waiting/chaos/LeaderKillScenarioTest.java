package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * C4 — 배분 리더 강제 종료 → 승계 (8.3.4 · 5절).
 *
 * <p>죽은 리더는 락을 쥔 채 사라진다. 그 사이 배분이 멎고 재료가 낡는데,
 * <b>낡았다는 것이 줄 선 사람을 추월할 이유가 되지 않는다</b>.
 */
// 이 시나리오가 C1 과 다른 점은 **제어 평면을 실제로 돌린다**는 것이다.
// 스케줄러를 켜고 진짜 레디스를 쓴다 — 스텁으로는 리더가 없다.
//
// **이름과 달리 승계 자체는 못 잰다** (CY-821). 낡음 임계가 리스보다 길어,
// 대기 노드가 있는 정상 승계에서는 낡음이 아예 안 열리는 것이 설계 의도다.
// 여기서는 죽은 리스를 길게 잡아 대기 노드가 없는 상태를 만들어 강제로 열었다.
// 그래서 재는 것은 **리더가 없는 동안의 판정**이고, 회복도 승계가 아니라 같은
// 노드의 재획득이다. 진짜 승계는 노드 둘짜리 하네스가 있어야 한다.
@Tag("chaos")
// **컨텍스트를 캐시에 남기지 않는다.** 스케줄러를 켜고 띄우므로 제어 평면 루프가
// 계속 도는데, 뒷정리가 자원을 내린 뒤에도 캐시된 컨텍스트는 살아 있다 — 그 루프가
// 사라진 자원을 치면서 뒤 시험을 흔든다.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class LeaderKillScenarioTest {

    private static final String COUPON = "c4-queued";

    /**
     * 줄이 없는 쿠폰. <b>대조군이자 양성 대조다.</b>
     *
     * <p>낡음 중에 이쪽이 계속 통과해야 fail-open 이 실제로 열린 것이고,
     * 그래야 저쪽이 안 통과한 것이 "추월을 안 했다" 는 뜻이 된다. 이게 없으면
     * 둘 다 막힌 경우와 구분이 안 간다.
     */
    private static final String 한산한_쿠폰 = "c4-idle";

    /** 죽은 리더의 이름. 이 소유자는 갱신도 해제도 안 한다 — 그래서 죽음이다. */
    private static final String 죽은_리더 = "c4-dead-leader";

    /** 죽은 리더가 쥐고 갈 리스. 낡음 임계보다 길어야 유지 구간이 열린다. */
    private static final Duration 죽은_리스 = Duration.ofSeconds(60);

    private static final Duration 기다림 = Duration.ofSeconds(20);

    private static final int 줄_선_사람 = 5;

    /** 각 구간에 보내는 요청 수. 정상 구간과 같아야 비교가 성립한다. */
    private static final int 보낼_수 = 20;

    /** 낡은 재료로 줄에 세웠을 때의 결정 이름. 사다리 7번이 밟혔다는 증거다. */
    private static final String 낡은_결정 = "ENQUEUE_STALE";

    /**
     * 낡음이 걷힐 때까지의 한계. 계획의 3틱을 여기에 건다.
     */
    // **락을 줍는 시간에 걸면 아무것도 안 조인다.** 리스를 시험이 직접 걷어
    // 냈으므로 그건 100~300ms 로 끝나고, 갱신 주기를 열 배로 늘리는 회귀도
    // 통과한다. 재야 할 것은 게이트웨이가 fail-open 을 그만두기까지다 —
    // 갱신에 배분 한 틱과 재료 받아 오기가 얹혀 예산이 실제로 빠듯하다.
    private static final Duration 낡음이_걷힐_한계 = Duration.ofSeconds(3);

    /** 대조군에 보내는 수. <b>크레딧 안쪽이어야 한다.</b> */
    // 뒷단 가용량 보고가 없으면 크레딧은 바닥값이고, 넘겨 보내면 한산한 쿠폰에도
    // 줄이 선다. 한 번 서면 그 뒤로는 영영 통과가 없어 대조군이 죽는다 — 막힌
    // 것이 판정 탓인지 크레딧 탓인지 못 가리게 된다. 보고를 심어 크레딧을 올리는
    // 길도 있는데, 새 인스턴스는 60초 램프업을 거치므로 시나리오 안에서는 안 오른다.
    private static final int 한산한_보낼_수 = 2;


    /** 뒷단이 쿠폰별로 받은 수. 추월은 "누가 뒷단에 닿았나" 로만 보인다. */
    private static final Map<String, AtomicLong> 뒷단이_받은_수 = new ConcurrentHashMap<>();

    private static final DisposableServer 뒷단 = HttpServer.create()
            .port(0)
            .handle((request, response) -> {
                뒷단이_받은_수.computeIfAbsent(쿠폰을_뽑는다(request.uri()),
                        id -> new AtomicLong()).incrementAndGet();
                return response.status(HttpStatus.OK.value()).send();
            })
            .bindNow();

    /** 경로에서 쿠폰 번호를 뽑는다. 어느 쿠폰이 뒷단에 닿았는지가 판정의 전부다. */
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
        faults = RedisFaults.시작한다();
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

    /**
     * 순번 조회 토큰.
     */
    // **발급 경로는 이걸 안 읽는다** — 거기가 보는 것은 `Entry-Token` 이다.
    // 그래서 이 시나리오는 사다리 2번(토큰 통과)을 한 번도 안 밟고, "추월 0"
    // 은 아무도 토큰을 안 든 체제에서만 잰 값이다 (CY-826).
    @Autowired
    private QueueToken tokens;

    /** 토큰의 발급 시각. 시험이 실시계를 직접 읽으면 앱과 갈릴 수 있다. */
    @Autowired
    private Clock clock;

    @Autowired
    private MeterRegistry meters;

    /** 장애 전후의 자리. RC5 가 이 둘을 비교한다. */
    private Map<String, Double> 장애_전_자리 = Map.of();

    private Map<String, Double> 회복_뒤_자리 = Map.of();

    /** 낡음이 열린 직후의 결정 수. 유지 구간의 증가분이 사다리 7번의 몫이다. */
    private long 낡음_직후_결정;

    private Instant 승계를_기다린_시각;

    private Duration 낡음이_걷히기까지;

    private WebTestClient 클라이언트() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build();
    }

    /** 재료를 심는다. 줄이 선 쿠폰과 한산한 쿠폰을 나란히 둔다. */
    private void 재료를_심는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON, 한산한_쿠폰).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "50").block(기다림);
        redis.opsForValue().set(RedisKeys.stock(한산한_쿠폰), "100000").block(기다림);
        for (int i = 0; i < 줄_선_사람; i++) {
            redis.opsForZSet()
                    .add(RedisKeys.queue(COUPON, 1, 0), "q" + i, 100 + i)
                    .block(기다림);
        }
    }


    /** 발급을 한 번 시도한다. 상태 코드만 본다 — 판정이 답했는가가 관심사다. */
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

    /** 줄에 선 사람들의 자리. 이름으로 짚어야 같은 값을 가진 둘이 안 섞인다. */
    private Map<String, Double> 자리들() {
        Map<String, Double> 자리 = new LinkedHashMap<>();
        for (int i = 0; i < 줄_선_사람; i++) {
            String member = "q" + i;
            Double score = redis.opsForZSet()
                    .score(RedisKeys.queue(COUPON, 1, 0), member).block(기다림);
            if (score != null) {
                자리.put(member, score);
            }
        }
        return 자리;
    }

    /** 죽은 리더가 락을 넘겨받는다. 만료와 획득이 한 회차라 앱이 못 끼어든다. */
    private void 죽은_리더가_락을_쥔다(LeaderFaults 락) {
        assertThat(락.죽은_리더가_넘겨받는다(죽은_리더, 죽은_리스)).isTrue();
        // **아무것도 안 한다. 그것이 죽음이다** — 갱신도 해제도 없다.
        락.프로세스를_죽인다(죽은_리더);
    }

    /**
     * C4 — 리더가 락을 쥔 채 죽는다. 배분이 멎고 재료가 낡는다.
     *
     * <p>낡았다는 것이 줄 선 사람을 추월할 이유가 되지 않는다 (불변식 4).
     */
    @Test
    @DisplayName("C4_리더가_죽고_승계된다")
    void C4_리더가_죽고_승계된다() {
        재료를_심는다();
        Awaitility.await().atMost(기다림).until(leadership::isLeader);
        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());

        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        try {
            LeaderFaults 락 = LeaderFaults.of(연결);
            List<Integer> 정상_상태 = new ArrayList<>();
            List<Integer> 장애중_상태 = new ArrayList<>();
            List<Integer> 회복_상태 = new ArrayList<>();
            // **줄 선 쿠폰의 응답도 든다.** 안 들면 그 20 명이 전원 503 을
            // 맞아도 초록이다 — 도착이 0 인 것은 추월이 없다는 뜻도 되고
            // 아무도 답을 못 받았다는 뜻도 된다.
            List<Integer> 진입_줄_상태 = new ArrayList<>();
            List<Integer> 장애중_줄_상태 = new ArrayList<>();
            List<Integer> 회복_줄_상태 = new ArrayList<>();
            long[] 줄_쿠폰_도착 = new long[3];
            long[] 한산한_쿠폰_도착 = new long[3];

            ChaosScenario.named("C4 리더 강제 종료")
                    .baseline(() -> {
                        장애_전_자리 = 자리들();
                        한산한_쿠폰_도착[0] = 잰다(한산한_쿠폰,
                                () -> 정상_상태.addAll(
                                        여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 1_100)));
                        줄_쿠폰_도착[0] = 잰다(COUPON,
                                () -> 여러_번_시도한다(COUPON, 보낼_수, 1_500));
                        assertThat(정상_상태).as("전제 — 한산한 쿠폰은 5xx 없이 답한다")
                                .noneMatch(status -> status >= 500);
                        assertThat(한산한_쿠폰_도착[0]).as("전제 — 한산한 쿠폰은 뒷단까지 간다")
                                .isPositive();
                        assertThat(줄_쿠폰_도착[0]).as("전제 — 줄이 선 쿠폰은 평시에도 안 간다")
                                .isZero();
                    })
                    .inject(() -> {
                        죽은_리더가_락을_쥔다(락);
                        // **진입 구간이 여기다.** 배분은 이미 멎었는데 재료는
                        // 아직 안 낡았다. 계획이 "판정 중단 0" 을 요구하는
                        // 구간이 정확히 이 몇 초다 — 낡음을 기다린 뒤에 재면
                        // 이 구간을 통째로 건너뛴다.
                        // 실패하면 왜인지가 보여야 한다. 불리언만 보면 러너가
                        // 느려 틱을 놓친 것이 제품 결함처럼 읽힌다.
                        assertThat(holder.isDataStale())
                                .as("전제 — 아직 안 낡았다 (나이 %s)", holder.dataAge())
                                .isFalse();
                        진입_줄_상태.addAll(여러_번_시도한다(COUPON, 보낼_수, 1_800));
                    })
                    .duringFault(() -> {
                        // **재료가 정말 낡을 때까지 기다린다.** 안 낡았으면 이
                        // 시나리오는 fail-open 갈래를 한 번도 안 밟은 것이다.
                        Awaitility.await().atMost(기다림).until(holder::isDataStale);
                        낡음_직후_결정 = 결정_수(낡은_결정);
                        long 낡기_전 = 받은_수(한산한_쿠폰);
                        장애중_상태.addAll(여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 2_000));
                        한산한_쿠폰_도착[1] = 받은_수(한산한_쿠폰) - 낡기_전;
                        줄_쿠폰_도착[1] = 잰다(COUPON,
                                () -> 장애중_줄_상태.addAll(
                                        여러_번_시도한다(COUPON, 보낼_수, 2_500)));
                    })
                    .recover(() -> {
                        승계를_기다린_시각 = clock.instant();
                        락.lease를_만료시킨다(Duration.ofMillis(1));
                    })
                    .afterRecovery(() -> {
                        Awaitility.await().atMost(기다림).until(leadership::isLeader);
                        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                        낡음이_걷히기까지 = Duration.between(승계를_기다린_시각, clock.instant());
                        long 회복_전 = 받은_수(한산한_쿠폰);
                        회복_상태.addAll(여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 3_000));
                        회복_뒤_자리 = 자리들();
                        한산한_쿠폰_도착[2] = 받은_수(한산한_쿠폰) - 회복_전;
                        줄_쿠폰_도착[2] = 잰다(COUPON,
                                () -> 회복_줄_상태.addAll(
                                        여러_번_시도한다(COUPON, 보낼_수, 3_500)));
                    })
                    // **진입 판정은 주입 직후다.** 아직 아무것도 안 보냈으므로
                    // 여기서 잴 것이 없다 — 유지 구간이 그것을 잰다.
                    // **진입 — 배분은 멎었는데 재료는 아직 신선하다.**
                    // 계획이 요구하는 "판정 중단 0" 이 이 구간이다.
                    .assertEntry(() -> RecoveryCriteria.violations(
                            줄에_세웠다(진입_줄_상태, "진입")))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            판정이_멈추지_않았다(장애중_상태),
                            // 양성 대조 — 낡음 중에도 한산한 쿠폰은 통과해야
                            // 한다. 안 통과하면 전면 차단이고, 그러면 아래
                            // "추월 0" 은 아무것도 안 잰 것이다.
                            열려_있었다(한산한_쿠폰_도착[1], 한산한_쿠폰_도착[0]),
                            줄을_추월하지_않았다(줄_쿠폰_도착[1]),
                            // 줄로 보냈는가. 도착 0 만 보면 전원 503 도 통과다.
                            줄에_세웠다(장애중_줄_상태, "유지"),
                            // **낡음 갈래를 실제로 밟았는가.** 상태 코드는
                            // 평시와 같은 202 라, 어느 줄이 답했는지는 결정
                            // 계수로만 보인다.
                            낡은_갈래를_밟았다()))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            판정이_멈추지_않았다(회복_상태),
                            열려_있었다(한산한_쿠폰_도착[2], 한산한_쿠폰_도착[0]),
                            줄을_추월하지_않았다(줄_쿠폰_도착[2]),
                            줄에_세웠다(회복_줄_상태, "회복"),
                            // 낡음이 오래 남으면 그동안 fail-open 이 열려 있다.
                            RecoveryCriteria.slowVerdictReturn(
                                    낡음이_걷히기까지, 낡음이_걷힐_한계),
                            // **RC5 는 여기서 깨질 수 없다** (CY-822). 자리를
                            // 걷는 것은 스위퍼인데, 기동 첫 틱의 낡음이 재개
                            // 유예를 시험 수명보다 길게 걸어 한 번도 안 돈다.
                            // 초록이지만 증거가 아니라, 재는 척하는 자리다.
                            RecoveryCriteria.seatLost(장애_전_자리, 회복_뒤_자리)))
                    // **RC1·RC2·RC4·RC6 은 여기서 안 잰다.** 이 시나리오는
                    // 레디스가 살아 있어 줄이 안 사라지므로 RC2 를 잴 수 있지만,
                    // 줄 선 쿠폰은 전 구간 202 라 순번이 안 나온다 — 폴링을
                    // 붙여야 하고 그건 이 시나리오의 대상이 아니다. RC1·RC4 는
                    // 뒷단 도착이 대조군 몇 건뿐이라 잴 표본이 없다.
                    //
                    // **EWMA 이월(F9)과 진동 0 도 못 잰다** (CY-820). 이월은
                    // 기동 직후 한 번만 일어나는데 여기 회복은 같은 JVM 의
                    // 재획득이라 그 코드를 아예 안 밟는다.
                    .run();
        } finally {
            연결.close();
        }
    }

    /** 한 배치가 그 쿠폰으로 뒷단에 몇 건 닿았는지 잰다. */
    private long 잰다(String couponId, Runnable 배치) {
        long 전 = 받은_수(couponId);
        배치.run();
        return 받은_수(couponId) - 전;
    }

    /**
     * <b>판정이 멈추지 않는다.</b> 리더가 죽어도 이 노드의 판정은 로컬이라
     * 계속 답해야 한다 — 5xx 는 어딘가에서 제어 평면을 기다렸다는 뜻이다.
     */
    private Optional<String> 판정이_멈추지_않았다(List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("보낸 것이 없다 — 판정 중단을 잴 수 없다");
        }
        long 멈춘_것 = 상태.stream().filter(status -> status >= 500).count();
        return 멈춘_것 == 0 ? Optional.empty()
                : Optional.of("판정이 %d 건 멈췄다 (보낸 %d)".formatted(멈춘_것, 상태.size()));
    }

    /**
     * <b>줄 선 사람을 추월하지 않았다</b> (불변식 4).
     *
     * <p>줄이 있는 쿠폰에 새로 온 사람이 뒷단까지 갔다면, 그는 줄을 건너뛴
     * 것이다. 낡아서 상태를 모른다는 것이 추월의 이유가 되지 않는다.
     */
    private Optional<String> 줄을_추월하지_않았다(long 뒷단에_닿은_수) {
        return 뒷단에_닿은_수 == 0 ? Optional.empty()
                : Optional.of("줄이 선 쿠폰에서 %d 건이 뒷단까지 갔다 — 추월이다"
                        .formatted(뒷단에_닿은_수));
    }

    /**
     * <b>전면 차단이 아니다.</b> 한산한 쿠폰까지 막히면 게이트웨이가 존재할
     * 이유가 없고, 그때 위의 "추월 0" 은 아무것도 안 잰 것이 된다.
     */
    // **0 보다 큰가로 안 본다.** 그러면 절반만 통과해도 초록이라, 상한을
    // 반으로 깎는 회귀가 그대로 지나간다. 평시와 같은 수가 가야 정상이다.
    private Optional<String> 열려_있었다(long 뒷단에_닿은_수, long 평시_도착) {
        return 뒷단에_닿은_수 == 평시_도착 ? Optional.empty()
                : Optional.of("한산한 쿠폰이 %d 건만 뒷단에 갔다 (평시 %d)"
                        .formatted(뒷단에_닿은_수, 평시_도착));
    }

    /**
     * <b>줄로 보냈는가.</b> 추월을 도착 수로만 보면 전원 5xx 도 통과한다 —
     * 아무도 안 갔다는 점에서 같기 때문이다. 자리를 받았는지를 따로 본다.
     */
    private Optional<String> 줄에_세웠다(List<Integer> 상태, String 구간) {
        if (상태.size() != 보낼_수) {
            return Optional.of("%s — %d 건만 답을 받았다 (보낸 %d)"
                    .formatted(구간, 상태.size(), 보낼_수));
        }
        long 자리를_못_받은_수 = 상태.stream().filter(status -> status != 202).count();
        return 자리를_못_받은_수 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 줄에 못 섰다: %s"
                        .formatted(구간, 자리를_못_받은_수, 상태));
    }

    /**
     * <b>낡은 갈래를 실제로 밟았는가.</b>
     *
     * <p>상태 코드는 평시와 같은 202 다. 백로그로 줄을 선 것과 재료가 낡아
     * 줄을 선 것이 겉으로 구분이 안 되므로, 결정 계수로만 보인다.
     */
    private Optional<String> 낡은_갈래를_밟았다() {
        long 증가 = 결정_수(낡은_결정) - 낡음_직후_결정;
        return 증가 >= 보낼_수 ? Optional.empty()
                : Optional.of("낡은 재료로 줄에 세운 것이 %d 건뿐이다 (보낸 %d)"
                        .formatted(증가, 보낼_수));
    }

    /** 그 결정이 지금까지 몇 번 나왔는가. */
    private long 결정_수(String outcome) {
        return (long) meters.find("waiting.admission").tag("outcome", outcome)
                .counters().stream().mapToDouble(c -> c.count()).sum();
    }


}

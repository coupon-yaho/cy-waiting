package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Clock;
import java.time.Duration;
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
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class LeaderKillScenarioTest {

    private static final String COUPON = "c4-queued";

    /**
     * 줄이 없는 쿠폰. <b>대조군이자 양성 대조다.</b>
     *
     * <p>낡음 중에 이쪽이 계속 통과해야 fail-open 이 실제로 열린 것이고,
     * 그래야 저쪽이 안 통과한 것이 "추월을 안 했다" 는 뜻이 된다. 이게 없으면
     * 둘 다 막힌 판과 구분이 안 간다.
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

    /** 순번 조회 토큰. 발급 요청에도 실어 보낸다. */
    @Autowired
    private QueueToken tokens;

    /** 토큰의 발급 시각. 시험이 실시계를 직접 읽으면 앱과 갈릴 수 있다. */
    @Autowired
    private Clock clock;

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

    /**
     * <b>죽은 리더가 락을 쥔다.</b>
     *
     * <p>이 노드가 락을 놓는 순간과 죽은 리더가 잡는 순간 사이는 경합이다 —
     * 이 노드가 먼저 잡으면 다시 만료시키고 다시 시도한다.
     */
    private void 죽은_리더가_락을_쥔다(LeaderFaults 락) {
        Awaitility.await().atMost(기다림).until(() -> {
            락.lease를_만료시킨다(Duration.ofMillis(1));
            return 락.리더로_만든다(죽은_리더, 죽은_리스);
        });
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
            long[] 줄_쿠폰_도착 = new long[3];
            long[] 한산한_쿠폰_도착 = new long[3];

            ChaosScenario.named("C4 리더 강제 종료")
                    .baseline(() -> {
                        정상_상태.addAll(여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 1_000));
                        줄_쿠폰_도착[0] = 잰다(COUPON, () -> 여러_번_시도한다(COUPON, 보낼_수, 1_500));
                        한산한_쿠폰_도착[0] = 받은_수(한산한_쿠폰);
                        assertThat(정상_상태).as("전제 — 한산한 쿠폰은 5xx 없이 답한다")
                                .noneMatch(status -> status >= 500);
                        assertThat(한산한_쿠폰_도착[0]).as("전제 — 한산한 쿠폰은 뒷단까지 간다")
                                .isPositive();
                        assertThat(줄_쿠폰_도착[0]).as("전제 — 줄이 선 쿠폰은 평시에도 안 간다")
                                .isZero();
                    })
                    .inject(() -> 죽은_리더가_락을_쥔다(락))
                    .duringFault(() -> {
                        // **재료가 정말 낡을 때까지 기다린다.** 안 낡았으면 이
                        // 시나리오는 fail-open 갈래를 한 번도 안 밟은 것이다.
                        Awaitility.await().atMost(기다림).until(holder::isDataStale);
                        long 낡기_전 = 받은_수(한산한_쿠폰);
                        장애중_상태.addAll(여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 2_000));
                        한산한_쿠폰_도착[1] = 받은_수(한산한_쿠폰) - 낡기_전;
                        줄_쿠폰_도착[1] = 잰다(COUPON, () -> 여러_번_시도한다(COUPON, 보낼_수, 2_500));
                    })
                    .recover(() -> 락.lease를_만료시킨다(Duration.ofMillis(1)))
                    .afterRecovery(() -> {
                        Awaitility.await().atMost(기다림).until(leadership::isLeader);
                        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                        long 회복_전 = 받은_수(한산한_쿠폰);
                        회복_상태.addAll(여러_번_시도한다(한산한_쿠폰, 한산한_보낼_수, 3_000));
                        한산한_쿠폰_도착[2] = 받은_수(한산한_쿠폰) - 회복_전;
                        줄_쿠폰_도착[2] = 잰다(COUPON, () -> 여러_번_시도한다(COUPON, 보낼_수, 3_500));
                    })
                    // **진입 판정은 주입 직후다.** 아직 아무것도 안 보냈으므로
                    // 여기서 잴 것이 없다 — 유지 구간이 그것을 잰다.
                    .assertEntry(ChaosScenario.Verdict.none())
                    .assertDuring(() -> RecoveryCriteria.violations(
                            판정이_멈추지_않았다(장애중_상태),
                            // 양성 대조 — 낡음 중에도 한산한 쿠폰은 통과해야
                            // 한다. 안 통과하면 전면 차단이고, 그러면 아래
                            // "추월 0" 은 아무것도 안 잰 것이다.
                            열려_있었다(한산한_쿠폰_도착[1]),
                            줄을_추월하지_않았다(줄_쿠폰_도착[1])))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            판정이_멈추지_않았다(회복_상태),
                            열려_있었다(한산한_쿠폰_도착[2]),
                            줄을_추월하지_않았다(줄_쿠폰_도착[2]),
                            자리가_그대로다()))
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
    private Optional<String> 열려_있었다(long 뒷단에_닿은_수) {
        return 뒷단에_닿은_수 > 0 ? Optional.empty()
                : Optional.of("한산한 쿠폰이 한 건도 뒷단에 못 갔다 — 전면 차단이다");
    }

    /** 줄에 선 사람들의 자리가 그대로다 (RC5). 재입장은 새 score 라 역행이다. */
    private Optional<String> 자리가_그대로다() {
        Map<String, Double> 지금 = 자리들();
        if (지금.size() < 줄_선_사람) {
            return Optional.of("줄에서 %d 명이 사라졌다".formatted(줄_선_사람 - 지금.size()));
        }
        for (int i = 0; i < 줄_선_사람; i++) {
            double 기대 = 100 + i;
            Double 실제 = 지금.get("q" + i);
            if (실제 == null || Double.compare(실제, 기대) != 0) {
                return Optional.of("q%d 의 자리가 %s 로 바뀌었다 (원래 %.0f)"
                        .formatted(i, 실제, 기대));
            }
        }
        return Optional.empty();
    }
}

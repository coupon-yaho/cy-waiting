package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.control.SnapshotHolder;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
 * C7 — 뒷단 인스턴스 한 대가 빠지고 콜드로 돌아온다 (8.3.4 · 5절).
 *
 * <p>빠진 인스턴스의 보고는 낡아 합산에서 빠지고 크레딧이 줄어야 한다. 돌아올
 * 때가 더 위험하다 — 재기동 직후는 캐시도 커넥션도 비어 실제보다 크게 부르는데,
 * 그 값을 그대로 믿으면 방금 뜬 인스턴스가 전부를 맞는다 (F6).
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
@Import(BackendLossScenarioTest.ShortRamp.class)
class BackendLossScenarioTest {

    private static final String COUPON = "c7-idle";

    private static final List<String> 인스턴스 = List.of("c7-be-a", "c7-be-b");

    /** 한 대가 부르는 여유. 둘이면 합이 그 두 배다. */
    private static final long 한_대_가용량 = 400;

    /** 콜드 복귀가 부르는 값. 실제보다 크게 부른 상태다 (F6). */
    private static final long 부풀린_가용량 = 4_000;

    private static final Duration 램프 = Duration.ofSeconds(3);

    private static final Duration 기다림 = Duration.ofSeconds(30);

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

    /**
     * 램프를 짧게 잡는다. 운영값 60초로 재면 시험 하나가 그만큼 걸린다.
     * 값만 다를 뿐 프로덕션이 도달하는 상태다.
     */
    // 클래스 이름은 영문이어야 한다 (checkstyle). 램프를 짧게 잡는 배선이다.
    @TestConfiguration
    static class ShortRamp {

        @Bean
        @Primary
        ControlPlaneProperties 속성() {
            ControlPlaneProperties 기본 = ControlPlaneProperties.defaults();
            ControlPlaneProperties.Capacity c = 기본.capacity();
            return new ControlPlaneProperties(기본.scheduler(), 기본.leader(),
                    new ControlPlaneProperties.Capacity(램프, c.freshness(), c.floor(),
                            c.perInstanceCap(), c.rampDownTicks(), c.expectedNodes()));
        }
    }

    @LocalServerPort
    private int port;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private SnapshotHolder holder;

    @Autowired
    private MeterRegistry meters;

    /** 살아 있는 인스턴스. 계속 보고해야 신선하다. */
    private final Set<String> 보고하는_인스턴스 = ConcurrentHashMap.newKeySet();

    private final ScheduledExecutorService 보고 = Executors.newSingleThreadScheduledExecutor();

    private long 크레딧() {
        return (long) meters.find("waiting.capacity.credit").gauge().value();
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

    private void 재료를_심는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "1000000").block(기다림);
    }

    private Optional<String> 오백이_안_샌다(String 구간, List<Integer> 상태) {
        if (상태.isEmpty()) {
            return Optional.of("%s — 보낸 것이 없다".formatted(구간));
        }
        long 샌_것 = 상태.stream().filter(status -> status >= 500).count();
        return 샌_것 == 0 ? Optional.empty()
                : Optional.of("%s — %d 건이 5xx 다 (보낸 %d)".formatted(구간, 샌_것, 상태.size()));
    }

    @Test
    @DisplayName("C7_뒷단이_빠졌다_콜드로_돌아온다")
    void C7_뒷단이_빠졌다_콜드로_돌아온다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        try {
            BackendReports 보고서 = BackendReports.실시계로(연결, ControlPlaneProperties
                    .defaults().capacity().freshness());
            long[] 크레딧_값 = new long[4];
            List<Integer> 장애중_상태 = new ArrayList<>();
            List<Integer> 회복_상태 = new ArrayList<>();

            ChaosScenario.named("C7 뒷단 인스턴스 소실")
                    .baseline(() -> {
                        재료를_심는다();
                        보고하는_인스턴스.addAll(인스턴스);
                        보고.scheduleAtFixedRate(
                                () -> 보고하는_인스턴스.forEach(id -> 보고서.보고한다(id, 한_대_가용량)),
                                0, 500, TimeUnit.MILLISECONDS);
                        Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                        // 둘이 다 램프를 지나 합이 반영될 때까지 기다린다.
                        Awaitility.await().atMost(기다림)
                                .until(() -> 크레딧() >= 한_대_가용량 * 인스턴스.size());
                        크레딧_값[0] = 크레딧();
                    })
                    .inject(() -> 보고하는_인스턴스.remove(인스턴스.get(0)))
                    .duringFault(() -> {
                        // 보고가 낡아 합산에서 빠지면 크레딧이 한 대 몫으로 준다.
                        Awaitility.await().atMost(기다림)
                                .until(() -> 크레딧() <= 한_대_가용량);
                        크레딧_값[1] = 크레딧();
                        장애중_상태.addAll(여러_번_시도한다(보낼_수, 2_000));
                        // **램프 창보다 오래 끈다.** 창 안에 돌아오면 그 인스턴스는
                        // 콜드가 아니라 잠깐 안 보인 것이고, 램프를 안 탄다 —
                        // 그게 맞는 동작이라 F6 을 밟으려면 창을 넘겨야 한다.
                        Awaitility.await().pollDelay(램프.plusSeconds(1))
                                .atMost(기다림).until(() -> true);
                    })
                    .recover(() -> {
                        // **콜드로 돌아온다.** 제 여유를 크게 부른다 (F6).
                        보고서.콜드로_복귀한다(인스턴스.get(0), 부풀린_가용량);
                        크레딧_값[2] = 크레딧();
                    })
                    .afterRecovery(() -> {
                        Awaitility.await().atMost(기다림)
                                .until(() -> 크레딧() > 크레딧_값[1]);
                        크레딧_값[3] = 크레딧();
                        회복_상태.addAll(여러_번_시도한다(보낼_수, 3_000));
                    })
                    .assertEntry(ChaosScenario.Verdict.none())
                    .assertDuring(() -> RecoveryCriteria.violations(
                            크레딧이_줄었다(크레딧_값[0], 크레딧_값[1]),
                            오백이_안_샌다("유지", 장애중_상태)))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            콜드가_한번에_안_올랐다(크레딧_값[2], 크레딧_값[3]),
                            오백이_안_샌다("회복", 회복_상태),
                            뒷단.중복_수신이_없다()))
                    .run();
        } finally {
            보고.shutdownNow();
            인스턴스.forEach(id -> 연결.sync().hdel(BackendReports.KEY, id));
            연결.close();
        }
    }

    private static final int 보낼_수 = 5;

    /** 빠진 대의 몫이 빠져야 한다. 안 빠지면 없는 여유로 배분한다. */
    private Optional<String> 크레딧이_줄었다(long 정상, long 장애중) {
        return 장애중 < 정상 ? Optional.empty()
                : Optional.of("크레딧이 %d 에서 %d 로 안 줄었다".formatted(정상, 장애중));
    }

    /**
     * <b>콜드가 부른 값을 그대로 안 쓴다</b> (F6).
     *
     * <p>재기동 직후는 캐시도 커넥션도 비어 실제보다 크게 부른다. 그 값을 믿으면
     * 방금 뜬 인스턴스가 전부를 맞고 다시 죽는다.
     */
    private Optional<String> 콜드가_한번에_안_올랐다(long 복귀_직후, long 회복) {
        long 부풀린_합 = 부풀린_가용량 + 한_대_가용량;
        if (복귀_직후 >= 부풀린_합) {
            return Optional.of("복귀 직후 크레딧이 %d 로 부른 값을 그대로 썼다".formatted(복귀_직후));
        }
        return 회복 < 부풀린_합 ? Optional.empty()
                : Optional.of("램프 없이 %d 까지 올랐다 (부른 값 %d)".formatted(회복, 부풀린_합));
    }
}

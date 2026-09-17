package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.AllocationRound;
import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * C2c — 레디스가 느린 동안에도 배분이 줄을 들이고 발행을 전진시킨다 (CY-927).
 *
 * <p>C2 는 배분 루프를 꺼 이 한계를 안 잰다. 리더는 유지되는데 회차가 틱 시한 안에 못 끝나 발행이 멎은 것이 연장
 * 밴드 시나리오에서 드러났다. 루프를 켜고 줄을 세운 채, 들인 인원과 서로 다른 발행 수를 본다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class AllocationUnderLatencyScenarioTest {

    private static final String[] COUPONS = {"c2c-a", "c2c-b", "c2c-c"};

    /** 쿠폰마다 세울 인원. 창 동안 다 빠지면 적용이 멎어 발행 수가 배분이 아니라 줄 길이를 잰다. */
    private static final int 줄_길이 = 3_000;

    /** 발행을 세는 창. 틱(1초) 여덟 개가 들어간다. */
    private static final Duration 전진_창 = Duration.ofSeconds(8);

    /**
     * 창 안에서 봐야 하는 서로 다른 발행 수. <b>한 번이라도 오르면 통과로 두지 않는다</b> — 회차가 절반씩 잘려도
     * 초록이었다. 적용은 한 틱에 한 번이고 이 지연에서 회차가 틱을 조금 넘겨, 줄을 세우고 잰 값은 세 번 다 여섯이었다.
     */
    private static final int 최소_발행 = 5;

    /** 발행 사이 틈의 한계를 세는 단위. <b>틱은 설정값이라 상수로 박으면 계약과 끊긴다.</b> */
    private static final int 한계_틱 = 3;

    private static final Duration 기다림 = Duration.ofSeconds(20);

    private static RedisWireFaults 선;

    private static RedisClient 클라이언트;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        선 = RedisWireFaults.시작한다();
        registry.add("spring.data.redis.host", 선::호스트);
        registry.add("spring.data.redis.port", 선::포트);
    }

    @AfterAll
    static void 내린다() {
        if (클라이언트 != null) {
            클라이언트.shutdown();
        }
        if (선 != null) {
            선.close();
        }
    }

    @Autowired
    private Leadership leadership;

    @Autowired
    private SnapshotHolder holder;

    @Autowired
    private AllocationRound round;

    @Autowired
    private ControlPlaneProperties properties;

    /**
     * 주입할 지연. <b>리더 연장 한 번의 상한보다 100ms 짧다.</b> 그보다 길면 연장이 시도 상한에 걸려 리더를 잃고, 그러면
     * "리더가 있는데 발행이 멎는다" 를 못 잰다. 리더가 사는 가장 높은 지연대에서 회차 예산을 본다.
     */
    private Duration 지연() {
        return properties.leader().attempt().minusMillis(100);
    }

    /**
     * 발행이 안 바뀐 가장 긴 시간의 한계. <b>틱에서 끌어온다</b> — 상수로 박으면 발행 주기를 어긴 회귀를
     * 그대로 통과시킨다.
     *
     * <p><b>세 틱을 조이지 않는다.</b> 발행 시각이 초 단위라 측정에 1초 가까운 양자화가 실린다 —
     * 두 틱으로 내리면 정상 주기에서도 한계에 닿아 거짓 빨강이 된다. 왕복을 직렬로 되돌리는 회귀는
     * 되감기 인자를 못 박은 단위 시험이 결정적으로 잡는다 (CY-939).
     */
    private Duration 최대_틈() {
        return properties.scheduler().tick().multipliedBy(한계_틱);
    }

    @Test
    @DisplayName("C2c_레디스가_느린_동안에도_발행이_전진한다")
    void C2c_레디스가_느린_동안에도_발행이_전진한다() {
        SnapshotRecoveryWatch 관측 = SnapshotRecoveryWatch.of(holder);
        클라이언트 = RedisClient.create(RedisURI.create(선.호스트(), 선.포트()));
        StatefulRedisConnection<String, String> 연결 = 클라이언트.connect();
        Instant[] 느리기_전_발행 = new Instant[1];
        long[] 느리기_전_들인_수 = new long[1];
        long[] 왕복 = new long[1];
        boolean[] 리더를_지켰다 = new boolean[1];
        boolean[] 회복_뒤_전진 = new boolean[1];
        long[] 발행_수 = new long[1];
        long[] 들인_수 = new long[1];
        long[] 낡은_표본 = new long[1];
        long[] 최대_틈_ms = new long[1];
        long[] 주입_시각 = new long[1];
        double[] 느리기_전_초과 = new double[1];
        double[] 초과 = new double[1];

        ChaosScenario.named("C2c 레디스 지연 중 발행")
                .baseline(() -> {
                    for (String 쿠폰 : COUPONS) {
                        연결.sync().sadd(RedisKeys.ACTIVE_COUPONS, 쿠폰);
                        연결.sync().set(RedisKeys.stock(쿠폰), "100000");
                        QueueSeed.줄을_세운다(연결, 쿠폰, 줄_길이);
                    }
                    Awaitility.await().atMost(기다림).until(leadership::isLeader);
                    Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale());
                })
                .inject(() -> {
                    try {
                        선.느리게(지연());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    long 앞 = System.nanoTime();
                    연결.sync().ping();
                    왕복[0] = Duration.ofNanos(System.nanoTime() - 앞).toMillis();
                    // 지연이 붙은 뒤의 첫 발행부터 센다. 붙기 전에 나간 회차를 섞지 않는다.
                    느리기_전_발행[0] = 관측.들고_있는_발행();
                    // 틈은 주입 시각부터 잰다. 주입 직후가 가장 긴 틈이 생기는 구간이다.
                    주입_시각[0] = System.nanoTime();
                    느리기_전_들인_수[0] = (long) round.admitted();
                    느리기_전_초과[0] = round.enteredOvershoot();
                })
                .duringFault(() -> {
                    long[] 잃은_표본 = new long[1];
                    Set<Instant> 본_발행 = new HashSet<>();
                    Instant[] 앞_발행 = {느리기_전_발행[0]};
                    long[] 바뀐_시각 = {주입_시각[0]};
                    Awaitility.await().during(전진_창).atMost(전진_창.plusSeconds(2))
                            .pollInterval(Duration.ofMillis(100)).until(() -> {
                                Instant 지금_발행 = holder.view().snapshot().publishedAt();
                                본_발행.add(지금_발행);
                                long 지금 = System.nanoTime();
                                if (!지금_발행.equals(앞_발행[0])) {
                                    앞_발행[0] = 지금_발행;
                                    바뀐_시각[0] = 지금;
                                }
                                최대_틈_ms[0] = Math.max(최대_틈_ms[0],
                                        Duration.ofNanos(지금 - 바뀐_시각[0]).toMillis());
                                if (!leadership.isLeader()) {
                                    잃은_표본[0]++;
                                }
                                if (holder.isDataStale()) {
                                    낡은_표본[0]++;
                                }
                                return true;
                            });
                    발행_수[0] = 본_발행.stream().filter(t -> t.isAfter(느리기_전_발행[0])).count();
                    들인_수[0] = (long) round.admitted() - 느리기_전_들인_수[0];
                    초과[0] = round.enteredOvershoot() - 느리기_전_초과[0];
                    리더를_지켰다[0] = 잃은_표본[0] == 0;
                })
                .recover(() -> {
                    try {
                        선.걷는다();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .afterRecovery(() -> 회복_뒤_전진[0] = 발행이_오른다(관측.들고_있는_발행(), 기다림))
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 전제 — 지연이 실제로 붙었다. 안 붙었으면 평시를 잰 것이다.
                        왕복[0] >= 지연().toMillis() ? Optional.empty()
                                : Optional.of("전제 — 왕복 %dms 가 지연 %s 보다 짧다".formatted(왕복[0], 지연()))))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 전제 — 리더가 유지돼야 "리더가 있는데 발행이 멎는다" 를 잰다.
                        리더를_지켰다[0] ? Optional.empty()
                                : Optional.of("전제 — 지연 %s 에서 리더를 잃었다".formatted(지연())),
                        발행_수[0] >= 최소_발행 ? Optional.empty()
                                : Optional.of("지연 %s 동안 %s 에 발행 %d 번"
                                        .formatted(지연(), 전진_창, 발행_수[0])),
                        // 발행이 드문드문 올라도 틈이 낡음 한계를 넘으면 노드들이 줄을 통째로 켠다.
                        낡은_표본[0] == 0 ? Optional.empty()
                                : Optional.of("지연 %s 동안 재료가 낡은 표본 %d".formatted(지연(), 낡은_표본[0])),
                        // 초 단위 발행 시각이라 개수는 경계에서 겹친다. 틈은 겹침에 안 흔들린다.
                        최대_틈_ms[0] <= 최대_틈().toMillis() ? Optional.empty()
                                : Optional.of("지연 %s 동안 발행 사이 틈 %dms (한계 %s)"
                                        .formatted(지연(), 최대_틈_ms[0], 최대_틈())),
                        // 적용이 몰려 한 회차 예산을 넘겨 들이면 초과 발급의 직접 증거다.
                        초과[0] == 0 ? Optional.empty()
                                : Optional.of("지연 %s 동안 예산을 넘겨 들인 인원 %.0f".formatted(지연(), 초과[0])),
                        // 발행만 오르고 적용이 전부 실패하면 줄은 멎은 채 초록이다.
                        들인_수[0] > 0 ? Optional.empty()
                                : Optional.of("지연 %s 동안 들인 인원이 없다".formatted(지연()))))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        회복_뒤_전진[0] ? Optional.empty()
                                : Optional.of("지연을 걷었는데 발행이 안 올랐다")))
                .run();
    }

    private boolean 발행이_오른다(Instant 앞, Duration 창) {
        try {
            Awaitility.await().pollInterval(Duration.ofMillis(100)).atMost(창)
                    .until(() -> holder.view().snapshot().publishedAt().isAfter(앞));
            return true;
        } catch (ConditionTimeoutException e) {
            return false;
        }
    }
}

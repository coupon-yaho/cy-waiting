package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
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
import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * C2c — 레디스가 느린 동안에도 배분이 발행을 전진시킨다 (CY-927).
 *
 * <p>C2 는 배분 루프를 꺼 이 한계를 안 잰다. 명령 상한 아래 지연에서 리더는 유지되는데 회차가 틱 시한 안에
 * 못 끝나 발행이 멎은 것이 연장 밴드 시나리오에서 드러났다. 루프를 켜고 발행 시각이 오르는지를 본다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class AllocationUnderLatencyScenarioTest {

    private static final String[] COUPONS = {"c2c-a", "c2c-b", "c2c-c"};

    /** 발행을 세는 창. 틱(1초) 여덟 개가 들어간다. */
    private static final Duration 전진_창 = Duration.ofSeconds(8);

    /**
     * 창 안에서 봐야 하는 서로 다른 발행 수. 틱마다 한 번이면 여덟이고, 발행 시각이 초 단위라 경계에서 하나쯤 겹친다.
     * <b>한 번이라도 오르면 통과로 두지 않는다</b> — 회차가 절반씩 잘려도 초록이었다. 고치기 전 넷, 고친 뒤 일곱.
     */
    private static final int 최소_발행 = 6;

    private static final Duration 기다림 = Duration.ofSeconds(20);

    private static RedisWireFaults 선;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        선 = RedisWireFaults.시작한다();
        registry.add("spring.data.redis.host", 선::호스트);
        registry.add("spring.data.redis.port", 선::포트);
    }

    @AfterAll
    static void 내린다() {
        if (선 != null) {
            선.close();
        }
    }

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private Leadership leadership;

    @Autowired
    private SnapshotHolder holder;

    @Autowired
    private DataRedisProperties redisProperties;

    /**
     * 주입할 지연. <b>명령 상한의 0.4 배다.</b> 0.6 배(C2 의 값)는 리더 연장 밴드에 들어 리더를 잃는다 — 그러면
     * "리더가 있는데 발행이 멎는다" 를 못 잰다. 리더가 사는 가장 높은 지연대에서 회차 예산을 본다.
     */
    private Duration 지연() {
        return redisProperties.getTimeout().multipliedBy(4).dividedBy(10);
    }

    @Test
    @DisplayName("C2c_레디스가_느린_동안에도_발행이_전진한다")
    void C2c_레디스가_느린_동안에도_발행이_전진한다() {
        SnapshotRecoveryWatch 관측 = SnapshotRecoveryWatch.of(holder);
        Instant[] 느리기_전_발행 = new Instant[1];
        boolean[] 리더를_지켰다 = new boolean[1];
        boolean[] 전진했다 = new boolean[1];
        boolean[] 회복_뒤_전진 = new boolean[1];
        long[] 발행_수 = new long[1];

        ChaosScenario.named("C2c 레디스 지연 중 발행")
                .baseline(() -> {
                    for (String 쿠폰 : COUPONS) {
                        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, 쿠폰).block(기다림);
                        redis.opsForValue().set(RedisKeys.stock(쿠폰), "100000").block(기다림);
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
                    // 지연이 붙은 뒤의 첫 발행부터 센다. 붙기 전에 나간 회차를 섞지 않는다.
                    느리기_전_발행[0] = 관측.들고_있는_발행();
                })
                .duringFault(() -> {
                    long[] 잃은_표본 = new long[1];
                    Set<Instant> 본_발행 = new HashSet<>();
                    Awaitility.await().during(전진_창).atMost(전진_창.plusSeconds(2))
                            .pollInterval(Duration.ofMillis(100)).until(() -> {
                                본_발행.add(holder.view().snapshot().publishedAt());
                                if (!leadership.isLeader()) {
                                    잃은_표본[0]++;
                                }
                                return true;
                            });
                    발행_수[0] = 본_발행.stream().filter(t -> t.isAfter(느리기_전_발행[0])).count();
                    리더를_지켰다[0] = 잃은_표본[0] == 0;
                    전진했다[0] = 발행_수[0] >= 최소_발행;
                })
                .recover(() -> {
                    try {
                        선.걷는다();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .afterRecovery(() -> 회복_뒤_전진[0] = 발행이_오른다(관측.들고_있는_발행(), 기다림))
                .assertEntry(ChaosScenario.Verdict.none())
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 전제 — 리더가 유지돼야 "리더가 있는데 발행이 멎는다" 를 잰다.
                        리더를_지켰다[0] ? Optional.empty()
                                : Optional.of("전제 — 지연 %s 에서 리더를 잃었다".formatted(지연())),
                        전진했다[0] ? Optional.empty()
                                : Optional.of("지연 %s 동안 %s 에 발행 %d 번"
                                        .formatted(지연(), 전진_창, 발행_수[0]))))
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

package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.AllocationRound;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.domain.allocation.Grant;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.Optional;
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
 * C22b — 레디스가 쓰기를 거부하는 동안 리더가 바뀐다 (CY-932).
 *
 * <p>리더는 CY-296 에서 상한에서도 서게 했다. 새 리더의 봉인이 거부되면 게이트가 안 잠근 채 열리고, 쓰기가 풀리는
 * 순간 유령의 지연된 몫이 먼저 들어간다. 상한 중 승계에서 봉인이 서고, 들이는 것은 멎었다가, 상한을 걷으면 재개하는지 본다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class MemoryLimitHandoverScenarioTest {

    private static final String COUPON = "c22b-queued";

    /** 창 동안 다 빠지지 않을 만큼. 빠지면 멎음이 상한 때문인지 줄 때문인지 못 가른다. */
    private static final int 줄_길이 = 3_000;

    private static final Duration 기다림 = Duration.ofSeconds(20);

    /** 상한을 유지하는 창. <b>발행 울타리 수명(10초)보다 길다</b> — 짧으면 상한 중 봉인이 사라지는 경로를 안 탄다. */
    private static final Duration 유지_창 = Duration.ofSeconds(12);

    /** 상한을 걷은 뒤 들이기가 재개돼야 하는 한계. */
    private static final Duration 회복_한계 = Duration.ofSeconds(30);

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("spring.data.redis.url", faults::주소);
    }

    @AfterAll
    static void 내린다() {
        if (faults != null) {
            faults.close();
        }
    }

    @Autowired
    private Leadership leadership;

    @Autowired
    private AllocationRound round;

    @Autowired
    private AllocationRedisPort port;

    @Test
    @DisplayName("C22b_쓰기가_막힌_동안_리더가_바뀌어도_봉인하고_풀리면_재개한다")
    void C22b_쓰기가_막힌_동안_리더가_바뀌어도_봉인하고_풀리면_재개한다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        LeaderFaults 락 = LeaderFaults.of(연결);
        String[] 원래_상한 = new String[1];
        long[] 앞_임기 = new long[1];
        long[] 새_임기 = new long[1];
        String[] 봉인된_표 = new String[1];
        String[] 발행_표 = new String[1];
        String[] 유지_끝_발행_표 = new String[1];
        boolean[] 유령을_막았다 = new boolean[1];
        long[] 표_수명_ms = new long[1];
        String[] 막히기_전_임계 = new String[1];
        String[] 유지_끝_임계 = new String[1];
        double[] 앞_초과 = new double[1];
        boolean[] 재개했다 = new boolean[1];

        ChaosScenario.named("C22b 상한 중 승계")
                .baseline(() -> {
                    연결.sync().sadd(RedisKeys.ACTIVE_COUPONS, COUPON);
                    연결.sync().set(RedisKeys.stock(COUPON), "100000");
                    QueueSeed.줄을_세운다(연결, COUPON, 줄_길이);
                    Awaitility.await().atMost(기다림).until(leadership::isLeader);
                    // 평시에 들이고 있어야 상한 중 멎음이 뜻을 가진다.
                    Awaitility.await().atMost(기다림)
                            .until(() -> 연결.sync().get(RedisKeys.admitted(COUPON, 1, 0)) != null);
                    앞_초과[0] = round.enteredOvershoot();
                })
                .inject(() -> {
                    원래_상한[0] = faults.메모리_상한을_내린다();
                    막히기_전_임계[0] = 연결.sync().get(RedisKeys.admitted(COUPON, 1, 0));
                    앞_임기[0] = leadership.fence();
                    // 리스를 끝내 승계를 만든다. 이 노드가 새 임기로 다시 잡고 승계 봉인을 돈다.
                    if (!락.lease를_만료시킨다(Duration.ofMillis(1))) {
                        throw new IllegalStateException("리스를 못 끝냈다 — 승계를 안 만들었다");
                    }
                    try {
                        Awaitility.await().atMost(기다림).pollInterval(Duration.ofMillis(50))
                                .until(() -> leadership.isLeader() && leadership.fence() > 앞_임기[0]);
                        새_임기[0] = leadership.fence();
                        Awaitility.await().atMost(기다림).pollInterval(Duration.ofMillis(50)).until(() ->
                                Long.toString(새_임기[0]).equals(연결.sync().get(RedisKeys.applyFence(COUPON, 1, 0))));
                        // 발행 봉인은 게이트가 안 기다린다. 따로 기다린다.
                        Awaitility.await().atMost(기다림).pollInterval(Duration.ofMillis(50)).until(() ->
                                Long.toString(새_임기[0]).equals(연결.sync().get(RedisKeys.SNAPSHOT_FENCE)));
                    } catch (ConditionTimeoutException e) {
                        // 판정이 이유를 적는다.
                    }
                    봉인된_표[0] = 연결.sync().get(RedisKeys.applyFence(COUPON, 1, 0));
                    발행_표[0] = 연결.sync().get(RedisKeys.SNAPSHOT_FENCE);
                    표_수명_ms[0] = 연결.sync().pttl(RedisKeys.SNAPSHOT_FENCE);
                })
                .duringFault(() -> {
                    Awaitility.await().pollDelay(유지_창).atMost(유지_창.plusSeconds(1)).until(() -> true);
                    유지_끝_임계[0] = 연결.sync().get(RedisKeys.admitted(COUPON, 1, 0));
                    유지_끝_발행_표[0] = 연결.sync().get(RedisKeys.SNAPSHOT_FENCE);
                })
                .recover(() -> {
                    if (원래_상한[0] != null) {
                        faults.메모리_상한을_되돌린다(원래_상한[0]);
                    }
                    // 풀리는 순간 멎었던 옛 리더의 적용이 먼저 닿는 경우다. 봉인이 섰으면 막힌다.
                    try {
                        port.apply(new Grant(COUPON, 1), 앞_임기[0]).block(기다림);
                    } catch (AllocationRedisPort.FencedOutException e) {
                        유령을_막았다[0] = true;
                    }
                })
                .afterRecovery(() -> {
                    try {
                        Awaitility.await().atMost(회복_한계).pollInterval(Duration.ofMillis(200)).until(() ->
                                !유지_끝_임계[0].equals(연결.sync().get(RedisKeys.admitted(COUPON, 1, 0))));
                        재개했다[0] = true;
                    } catch (ConditionTimeoutException e) {
                        재개했다[0] = false;
                    }
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        새_임기[0] > 앞_임기[0] ? Optional.empty()
                                : Optional.of("상한 중에 후임이 안 섰다 — 임기 %d".formatted(leadership.fence())),
                        Long.toString(새_임기[0]).equals(봉인된_표[0]) ? Optional.empty()
                                : Optional.of("상한 중 승계 봉인이 안 섰다 — 표 %s, 새 임기 %d"
                                        .formatted(봉인된_표[0], 새_임기[0])),
                        Long.toString(새_임기[0]).equals(발행_표[0]) ? Optional.empty()
                                : Optional.of("상한 중 발행 봉인이 안 섰다 — 표 %s".formatted(발행_표[0])),
                        // 전제 — 표 수명이 유지 창보다 짧아야 "봉인이 남는다" 판정이 무언가를 잰다.
                        표_수명_ms[0] < 유지_창.toMillis() ? Optional.empty()
                                : Optional.of("전제 — 발행 표 수명 %dms 가 유지 창 %s 보다 길다"
                                        .formatted(표_수명_ms[0], 유지_창))))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 적용은 사람 수만큼 쓰는 스크립트라 상한에서 거부되는 것이 맞다.
                        막히기_전_임계[0].equals(유지_끝_임계[0]) ? Optional.empty()
                                : Optional.of("상한 중에 입장 임계가 올랐다 — %s → %s"
                                        .formatted(막히기_전_임계[0], 유지_끝_임계[0])),
                        // 발행이 거부되는 동안 표의 수명이 끝나면 봉인이 사라진 채 풀린다.
                        Long.toString(새_임기[0]).equals(유지_끝_발행_표[0]) ? Optional.empty()
                                : Optional.of("상한 %s 끝에 발행 봉인이 사라졌다 — 표 %s"
                                        .formatted(유지_창, 유지_끝_발행_표[0]))))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        유령을_막았다[0] ? Optional.empty() : Optional.of("풀린 순간 옛 임기의 적용이 통과했다"),
                        재개했다[0] ? Optional.empty()
                                : Optional.of("상한을 걷고 %s 안에 들이기가 재개되지 않았다".formatted(회복_한계)),
                        round.enteredOvershoot() == 앞_초과[0] ? Optional.empty()
                                : Optional.of("예산을 넘겨 들였다 — %.0f 명"
                                        .formatted(round.enteredOvershoot() - 앞_초과[0]))))
                .run();
        연결.close();
    }
}

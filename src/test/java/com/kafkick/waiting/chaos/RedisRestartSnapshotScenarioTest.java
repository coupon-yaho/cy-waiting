package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * C1b — 레디스가 죽었다 살아난 뒤 첫 스냅샷까지 5초 (CY-816).
 *
 * <p>C1 은 재료를 스텁으로 받아 레디스를 끊어도 받아오기가 안 멎는다. 여기서는 실제 받아오기와
 * 실시계로 돌려, 레디스가 준비된 순간부터 <b>발행된 스냅샷을 다시 받아오기까지</b>를 잰다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class RedisRestartSnapshotScenarioTest {

    private static final String COUPON = "c1b";

    /** 계획서가 요구하는 재적재 한계. */
    private static final Duration 재적재_한계 = Duration.ofSeconds(5);

    /** 받아오기가 멎었다고 볼 나이. 갱신 주기보다 넉넉히 길어야 한 번 늦은 것과 갈린다. */
    private static final Duration 멎은_나이 = Duration.ofSeconds(3);

    private static final Duration 기다림 = Duration.ofSeconds(20);

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        // **영속으로 띄운다.** 안 그러면 재기동에 활성 쿠폰이 사라져 발행이 비고, 갱신 루프는 빈
        // 스냅샷을 안 받아 끝내 재적재가 안 된다. 운영 레디스는 영속성을 켠다.
        faults = RedisFaults.영속으로_시작한다();
        registry.add("spring.data.redis.url", faults::주소);
    }

    @AfterAll
    static void 내린다() {
        if (faults != null) {
            faults.close();
        }
    }

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private Leadership leadership;

    @Autowired
    private SnapshotHolder holder;

    @Autowired
    private Clock clock;

    /** 마지막으로 받아온 시각. 실패한 받아오기는 이것을 안 움직인다. */
    private Instant 받아온_시각() {
        return clock.instant().minus(holder.fetchAge());
    }

    @Test
    @DisplayName("C1b_레디스가_살아나면_5초_안에_스냅샷을_다시_받는다")
    void C1b_레디스가_살아나면_5초_안에_스냅샷을_다시_받는다() {
        boolean[] 발행을_지웠다 = new boolean[1];
        Duration[] 멎은_뒤_나이 = new Duration[1];
        Instant[] 준비된_시각 = new Instant[1];
        Duration[] 재적재까지 = new Duration[1];

        ChaosScenario.named("C1b 레디스 재기동 뒤 첫 스냅샷")
                .baseline(() -> {
                    redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
                    redis.opsForValue().set(RedisKeys.stock(COUPON), "100").block(기다림);
                    Awaitility.await().atMost(기다림).until(leadership::isLeader);
                    Awaitility.await().atMost(기다림)
                            .until(() -> holder.view().snapshot().isPublished()
                                    && holder.fetchAge().compareTo(Duration.ofSeconds(2)) < 0);
                })
                .inject(() -> faults.끊는다())
                .duringFault(() -> {
                    // **받아오기가 정말 레디스를 치는가.** 안 멎으면 아래 재적재는 아무것도 안 잰다.
                    Awaitility.await().atMost(기다림)
                            .until(() -> holder.fetchAge().compareTo(멎은_나이) > 0);
                    멎은_뒤_나이[0] = holder.fetchAge();
                    발행을_지웠다[0] = !holder.view().snapshot().isPublished();
                })
                .recover(() -> {
                    faults.붙인다();
                    준비된_시각[0] = clock.instant();
                })
                .afterRecovery(() -> {
                    try {
                        Awaitility.await().pollInterval(Duration.ofMillis(50)).atMost(기다림)
                                .until(() -> 받아온_시각().isAfter(준비된_시각[0])
                                        && holder.view().snapshot().isPublished());
                        재적재까지[0] = Duration.between(준비된_시각[0], 받아온_시각());
                    } catch (ConditionTimeoutException e) {
                        재적재까지[0] = null;
                    }
                })
                .assertEntry(ChaosScenario.Verdict.none())
                .assertDuring(() -> RecoveryCriteria.violations(
                        받아오기가_멎었다(멎은_뒤_나이[0]),
                        // 계획서 진입 기대 — 스냅샷을 지우지 않는다.
                        발행을_지웠다[0]
                                ? Optional.of("레디스가 죽자 발행된 스냅샷을 버렸다")
                                : Optional.empty()))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        제때_다시_받았다(재적재까지[0])))
                .run();
    }

    private Optional<String> 받아오기가_멎었다(Duration 나이) {
        return 나이 != null && 나이.compareTo(멎은_나이) > 0 ? Optional.empty()
                : Optional.of("전제 — 레디스를 죽였는데 받아오기가 안 멎었다: 나이 %s".formatted(나이));
    }

    private Optional<String> 제때_다시_받았다(Duration 걸림) {
        if (걸림 == null) {
            return Optional.of("레디스가 살아났는데 %s 안에 스냅샷을 못 받았다".formatted(기다림));
        }
        return 걸림.compareTo(재적재_한계) <= 0 ? Optional.empty()
                : Optional.of("첫 스냅샷까지 %s 걸렸다 (한계 %s)".formatted(걸림, 재적재_한계));
    }
}

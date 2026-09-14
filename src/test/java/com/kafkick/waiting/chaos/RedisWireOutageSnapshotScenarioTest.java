package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.control.SnapshotHolder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.awaitility.Awaitility;
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
 * C1c — 레디스 회선이 끊겼다 돌아온 뒤 새 발행까지 5초 (CY-930).
 *
 * <p>C1b 는 프로세스를 죽여 재연결이 곧바로 거부된다. 회선 장애는 그보다 늦다 — 붙은 연결이 응답을 안
 * 받거나, 재연결 시도가 연결 상한까지 매달린다. 예산 여유가 가장 적은 판이다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class RedisWireOutageSnapshotScenarioTest {

    private static final String COUPON = "c1c";

    /** 계획서가 요구하는 재적재 한계. */
    private static final Duration 재적재_한계 = Duration.ofSeconds(5);

    /** 받아오기가 멎었다고 볼 나이. 갱신 주기보다 넉넉히 길어야 한 번 늦은 것과 갈린다. */
    private static final Duration 멎은_나이 = Duration.ofSeconds(3);

    /** 멎은 채로 두는 시간. 재연결 지연이 불어날 만큼 길어야 상한이 빠진 회귀가 드러난다. */
    private static final Duration 오래_끊는다 = Duration.ofSeconds(10);

    /** 루프를 지켜보는 시간과 루프가 돈 뒤 나이의 한계. C1b 와 같다. */
    private static final Duration 루프_관찰 = Duration.ofSeconds(4);
    private static final Duration 루프_한계 = Duration.ofMillis(2_500);

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
    private Clock clock;

    /** 장애 한 판. 주입 방식만 다르고 판정은 같다. */
    private interface FaultInjection {
        void 넣는다() throws IOException;
    }

    /**
     * 붙은 연결이 응답을 못 받는다. <b>회복은 버려진 바이트 뒤에서의 회복이다</b> — 실제 TCP 는 흐름 중간에 바이트를
     * 조용히 잃지 않는다. 이 판이 제대로 재는 것은 루프 정지 0 과 낡음 진입이다.
     */
    @Test
    @DisplayName("C1c_붙은_연결이_응답을_안_받다_돌아오면_5초_안에_낡음이_풀린다")
    void C1c_붙은_연결이_응답을_안_받다_돌아오면_5초_안에_낡음이_풀린다() {
        돌린다("C1c 레디스 회선 블랙홀", 선::끊는다, false);
    }

    /** 재연결이 연결 상한까지 매달린다. 재연결 지연 상한이 빠진 회귀는 이 판만 잡는다. */
    @Test
    @DisplayName("C1c_재연결이_매달리다_돌아오면_5초_안에_낡음이_풀린다")
    void C1c_재연결이_매달리다_돌아오면_5초_안에_낡음이_풀린다() {
        돌린다("C1c 레디스 재연결 매달림", 선::재연결을_매단다, true);
    }

    private void 돌린다(String 이름, FaultInjection 장애, boolean 매달림을_확인한다) {
        SnapshotRecoveryWatch 관측 = SnapshotRecoveryWatch.of(holder, clock);
        boolean[] 매달렸다 = {!매달림을_확인한다};
        Duration[] 멎은_뒤_나이 = new Duration[1];
        Duration[] 가장_긴_틱_나이 = new Duration[1];
        boolean[] 낡음에_들었다 = new boolean[1];
        Instant[] 앞_발행 = new Instant[1];
        Instant[] 걷은_시각 = new Instant[1];
        Duration[] 새_발행까지 = new Duration[1];

        ChaosScenario.named(이름)
                .baseline(() -> {
                    redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
                    redis.opsForValue().set(RedisKeys.stock(COUPON), "100").block(기다림);
                    Awaitility.await().atMost(기다림).until(leadership::isLeader);
                    Awaitility.await().atMost(기다림).until(() -> !holder.isDataStale()
                            && holder.fetchAge().compareTo(Duration.ofSeconds(2)) < 0);
                })
                .inject(() -> {
                    앞_발행[0] = 관측.들고_있는_발행();
                    try {
                        장애.넣는다();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .duringFault(() -> {
                    if (매달림을_확인한다) {
                        매달렸다[0] = 선.새_연결이_매달린다(Duration.ofMillis(500));
                    }
                    멎은_뒤_나이[0] = 관측.받아오기가_멎을_때까지(멎은_나이, 기다림);
                    가장_긴_틱_나이[0] = 관측.가장_긴_틱_나이(루프_관찰);
                    관측.멎은_채로_둔다(오래_끊는다, 멎은_나이);
                    낡음에_들었다[0] = holder.isDataStale();
                })
                .recover(() -> {
                    try {
                        선.걷는다();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    걷은_시각[0] = clock.instant();
                })
                .afterRecovery(() -> 새_발행까지[0] =
                        관측.새_발행으로_낡음이_풀리기까지(걷은_시각[0], 앞_발행[0], 기다림))
                .assertEntry(ChaosScenario.Verdict.none())
                .assertDuring(() -> RecoveryCriteria.violations(
                        매달렸다[0] ? Optional.empty()
                                : Optional.of("하네스 — 새 연결이 매달리지 않는다. 거부나 응답으로 떨어졌다"),
                        멎은_뒤_나이[0] != null ? Optional.empty()
                                : Optional.of("전제 — 회선을 끊었는데 받아오기가 안 멎었다"),
                        가장_긴_틱_나이[0] != null && 가장_긴_틱_나이[0].compareTo(루프_한계) < 0
                                ? Optional.empty()
                                : Optional.of("회선이 끊긴 동안 갱신 루프가 %s 멎었다 (한계 %s)"
                                        .formatted(가장_긴_틱_나이[0], 루프_한계)),
                        // 계획서 유지 기대 — 낡음에 든다. 안 들면 fail-open 갈래를 안 밟는다.
                        낡음에_들었다[0] ? Optional.empty()
                                : Optional.of("회선이 %s 끊겼는데 낡음에 안 들었다".formatted(오래_끊는다))))
                .assertRecovery(() -> RecoveryCriteria.violations(새_발행으로_풀렸다(새_발행까지[0])))
                .run();
    }

    private Optional<String> 새_발행으로_풀렸다(Duration 걸림) {
        if (걸림 == null) {
            return Optional.of("회선을 걷었는데 %s 안에 새 발행으로 낡음이 안 풀렸다".formatted(기다림));
        }
        return 걸림.compareTo(재적재_한계) <= 0 ? Optional.empty()
                : Optional.of("새 발행으로 낡음이 풀리기까지 %s 걸렸다 (한계 %s)".formatted(걸림, 재적재_한계));
    }
}

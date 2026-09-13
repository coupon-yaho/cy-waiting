package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.Leadership;
import java.io.IOException;
import java.time.Duration;
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
 * C2b — 연장만 끊기고 쓰기는 되는 지연 밴드 (CY-847).
 *
 * <p><b>리더 연장 한 번의 상한이 명령 상한보다 짧으면</b> 그 사이의 지연에서 명령은 되는데
 * 연장만 시한에 걸린다. 리스가 지나 리더가 0 이 되고, 다시 잡는 시도도 같은 시한에 걸려
 * 지연이 이어지는 내내 배분이 멎는다. 지연 시나리오(C2)는 배분 루프를 꺼 이 밴드를 안 때린다.
 */
@Tag("chaos")
// 배분·리더 루프를 켜므로 컨텍스트를 닫는다. 캐시에 남으면 다음 시나리오의 레디스에 쓴다.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class LeaderRenewBandScenarioTest {

    private static final String COUPON = "c2b-idle";

    /**
     * 명령 상한(350ms) 바로 아래. <b>옛 설정(연장 300ms · 명령 500ms)에서는 연장만 끊기는
     * 밴드 안이었다.</b> 두 상한이 같아진 지금은 명령도 되고 연장도 돼야 한다.
     */
    private static final Duration 지연 = Duration.ofMillis(300);

    /** 리스(2초)를 여러 번 넘길 만큼 둔다. 한 리스 안이면 모름이 리더로 버틴다. */
    private static final Duration 유지 = Duration.ofSeconds(6);

    private static final Duration 기다림 = Duration.ofSeconds(20);

    private static final BackendStub 뒷단 = BackendStub.항상_받는다();

    private static RedisWireFaults 선;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        선 = RedisWireFaults.시작한다();
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
        registry.add("spring.data.redis.host", 선::호스트);
        registry.add("spring.data.redis.port", 선::포트);
    }

    @AfterAll
    static void 내린다() {
        뒷단.close();
        if (선 != null) {
            선.close();
        }
    }

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private Leadership leadership;

    private void 재료를_심는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "100000").block(기다림);
    }

    /** 마지막 발행 시각(초). 아직 없으면 0. 앱과 같은 선을 지나 지연에 같이 걸린다. */
    private long 발행_시각() {
        Object raw = redis.opsForHash().get(RedisKeys.SNAPSHOT, "#published").block(기다림);
        return raw == null ? 0 : Long.parseLong(raw.toString());
    }

    private long 카나리_지연() {
        long 시작 = System.nanoTime();
        redis.opsForValue().get("chaos:canary").block(기다림);
        return Duration.ofNanos(System.nanoTime() - 시작).toMillis();
    }

    @Test
    @DisplayName("C2b_명령_상한_아래_지연에서_리더를_안_놓는다")
    void C2b_명령_상한_아래_지연에서_리더를_안_놓는다() {
        long[] 발행 = new long[3];
        long[] 카나리 = new long[2];
        boolean[] 리더 = new boolean[1];

        ChaosScenario.named("C2b 연장 밴드 지연 %s".formatted(지연))
                .baseline(() -> {
                    재료를_심는다();
                    Awaitility.await().atMost(기다림)
                            .until(() -> leadership.isLeader() && 발행_시각() > 0);
                    카나리[0] = 카나리_지연();
                    발행[0] = 발행_시각();
                })
                .inject(() -> 지연을_넣는다(지연))
                .duringFault(() -> {
                    카나리[1] = 카나리_지연();
                    long 시작 = 발행_시각();
                    Awaitility.await().pollDelay(유지).atMost(유지.plusSeconds(5))
                            .until(() -> true);
                    리더[0] = leadership.isLeader();
                    발행[0] = 시작;
                    발행[1] = 발행_시각();
                })
                .recover(this::지연을_걷는다)
                .afterRecovery(() -> Awaitility.await().atMost(기다림)
                        .until(() -> (발행[2] = 발행_시각()) > 발행[1]))
                .assertEntry(() -> RecoveryCriteria.violations(
                        발행이_있다(발행[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        주입이_걸렸다(카나리[0], 카나리[1]),
                        // **리더를 놓지 않는다.** 명령은 되는데 연장만 시한에 걸려
                        // 리스를 잃으면, 지연이 이어지는 내내 아무도 배분을 안 돈다.
                        리더를_지켰다(리더[0])))
                // **발행 전진은 여기서 안 판정한다** (CY-927). 리더를 지켜도 회차가 레디스
                // 왕복을 차례로 여러 번 해 이 지연에서는 틱 시한 안에 못 끝난다 — 연장
                // 밴드와 다른 한계라, 섞으면 이 시나리오가 무엇을 쟀는지 못 가린다.
                .assertRecovery(() -> RecoveryCriteria.violations(
                        배분이_돌아왔다(발행[1], 발행[2])))
                .run();
    }

    private Optional<String> 발행이_있다(long 시각) {
        return 시각 > 0 ? Optional.empty() : Optional.of("전제 — 정상 구간에 발행이 없다");
    }

    /** 주입이 정말 걸렸는가. 안 걸렸으면 뒤의 판정이 아무것도 안 잰다. */
    private Optional<String> 주입이_걸렸다(long 정상, long 장애중) {
        return 장애중 - 정상 >= 지연.toMillis() / 2 ? Optional.empty()
                : Optional.of("전제 — 카나리가 %dms → %dms 로 안 느려졌다".formatted(정상, 장애중));
    }

    private Optional<String> 리더를_지켰다(boolean 리더) {
        return 리더 ? Optional.empty()
                : Optional.of("유지 — 연장 밴드 지연에서 리더를 놓았다 (리스 %s 넘게)".formatted(유지));
    }

    private Optional<String> 배분이_돌아왔다(long 전, long 후) {
        return 후 > 전 ? Optional.empty()
                : Optional.of("회복 — 지연을 걷어도 발행이 안 전진한다 (%d → %d)".formatted(전, 후));
    }

    private void 지연을_넣는다(Duration 만큼) {
        try {
            선.느리게(만큼);
        } catch (IOException e) {
            throw new IllegalStateException("지연을 못 넣었다", e);
        }
    }

    private void 지연을_걷는다() {
        try {
            선.걷는다();
        } catch (IOException e) {
            throw new IllegalStateException("지연을 못 걷었다", e);
        }
    }
}

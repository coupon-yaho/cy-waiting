package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.Leadership;
import com.kafkick.waiting.gateway.QueuePort;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.locks.LockSupport;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * C2b — 연장만 끊기고 명령은 되는 지연 밴드 (CY-847).
 *
 * <p><b>연장 상한이 명령 상한보다 짧아</b> 그 사이 지연에서 리더가 0 이 된다. 받아들인 동작이다 —
 * 명령 상한을 내리면 줄 등록이 먼저 끊겨 fail-open(추월)이 열린다. 내려오는 판정은
 * {@code LeadershipTest} 가 결정적으로 재고, 여기는 실제 회선 지연의 통합 시나리오다.
 */
@Tag("chaos")
// 배분·리더 루프를 켜므로 컨텍스트를 닫는다. 캐시에 남으면 다음 시나리오의 레디스에 쓴다.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=true")
class LeaderRenewBandScenarioTest {

    private static final String COUPON = "c2b-queued";

    /** 리스(2초)를 여러 번 넘길 만큼 둔다. 한 리스 안이면 모름이 리더로 버틴다. */
    private static final Duration 유지 = Duration.ofSeconds(6);

    /** 리더 표본 간격. 끝의 한 번만 보면 잃었다 되찾은 구간을 못 가른다. */
    private static final Duration 표본_간격 = Duration.ofMillis(100);

    /** 유지 구간에 줄에 세워 볼 사람 수. */
    private static final int 등록_수 = 5;

    /** 줄에 세우는 시각. 재는 것은 등록이 되는가이지 순번이 아니라 고정한다. */
    private static final Instant 등록_시각 = Instant.parse("2026-09-13T00:00:00Z");

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

    @Autowired
    private QueuePort queue;

    @Autowired
    private DataRedisProperties redisProperties;

    /**
     * 주입할 지연. <b>명령 상한의 0.85 배다</b> — 명령은 되고, 연장 시도(상한보다 짧다)는
     * 넘는 자리다. 설정에서 구해 상한이 바뀌어도 그 밴드 안에 머문다.
     */
    private Duration 지연() {
        return redisProperties.getTimeout().multipliedBy(85).dividedBy(100);
    }

    private void 재료를_심는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "100000").block(기다림);
    }

    /** 마지막 발행 시각(초). 아직 없으면 0. 앱과 같은 선을 지나 지연에 같이 걸린다. */
    private long 발행_시각() {
        Object raw = redis.opsForHash().get(RedisKeys.SNAPSHOT, "#published").block(기다림);
        return raw == null ? 0 : Long.parseLong(raw.toString());
    }

    /** 유지 구간의 발행 시각. 명령 시한에 걸리면 앞서 본 값을 쓴다 — 그 사이 전진을 못 봤을 뿐이다. */
    private long 발행_시각_또는(long 앞서_본_값) {
        try {
            return 발행_시각();
        } catch (QueryTimeoutException e) {
            return 앞서_본_값;
        }
    }

    /**
     * 같은 선을 지나는 카나리의 걸린 시간(ms). <b>명령 시한에 걸리면 상한만큼 걸린 것으로 적는다</b>
     * — 지연이 상한 가까이라 왕복 비용이 더해지면 끊길 수 있고, 그걸 예외로 두면 유지 구간의
     * 나머지 관측이 통째로 빠진다. 다른 레디스 오류는 지연이 아니라 그대로 던진다.
     */
    private long 카나리_지연() {
        long 시작 = System.nanoTime();
        try {
            redis.opsForValue().get("chaos:canary").block(기다림);
        } catch (QueryTimeoutException e) {
            return redisProperties.getTimeout().toMillis();
        }
        return Duration.ofNanos(System.nanoTime() - 시작).toMillis();
    }

    /** 유지 구간 내내 리더를 묻고, 리더가 아닌 표본 수를 돌려준다. */
    private int 리더가_아닌_표본() {
        int 놓침 = 0;
        long 끝 = System.nanoTime() + 유지.toNanos();
        while (System.nanoTime() < 끝) {
            if (!leadership.isLeader()) {
                놓침++;
            }
            LockSupport.parkNanos(표본_간격.toNanos());
        }
        return 놓침;
    }

    /**
     * 요청 경로와 같은 포트로 줄에 세운다. <b>실패한 수를 돌려준다</b> — 실패하면 요청 경로가
     * fail-open 으로 넘어가 줄 선 사람을 추월하므로, 그 문턱이 이 수로 보인다.
     */
    private int 줄_등록이_실패한_수() {
        int 실패 = 0;
        for (int i = 0; i < 등록_수; i++) {
            try {
                queue.enqueue(COUPON, "c2b-" + i, 1_000, 등록_시각).block(기다림);
            } catch (RuntimeException e) {
                실패++;
            }
        }
        return 실패;
    }

    @Test
    @DisplayName("C2b_연장_밴드_지연에서_리더는_사라지고_줄_등록은_산다")
    void C2b_연장_밴드_지연에서_리더는_사라지고_줄_등록은_산다() {
        long[] 발행 = new long[2];
        long[] 카나리 = new long[2];
        int[] 놓침 = new int[1];
        int[] 등록_실패 = new int[1];
        boolean[] 리더_복귀 = new boolean[1];
        boolean[] 정상_리더 = new boolean[1];

        ChaosScenario.named("C2b 연장 밴드 지연 %s".formatted(지연()))
                .baseline(() -> {
                    재료를_심는다();
                    Awaitility.await().atMost(기다림)
                            .until(() -> leadership.isLeader() && 발행_시각() > 0);
                    카나리[0] = 카나리_지연();
                    정상_리더[0] = leadership.isLeader();
                    발행[0] = 발행_시각();
                })
                .inject(() -> 지연을_넣는다(지연()))
                .duringFault(() -> {
                    카나리[1] = 카나리_지연();
                    놓침[0] = 리더가_아닌_표본();
                    등록_실패[0] = 줄_등록이_실패한_수();
                    발행[0] = 발행_시각_또는(발행[0]);
                })
                .recover(this::지연을_걷는다)
                .afterRecovery(() -> {
                    Awaitility.await().atMost(기다림)
                            .until(() -> (발행[1] = 발행_시각()) > 발행[0]);
                    리더_복귀[0] = leadership.isLeader();
                })
                // 평시에 리더가 있어야 "사라졌다" 가 이 지연 탓이다.
                .assertEntry(() -> RecoveryCriteria.violations(
                        리더가_돌아왔다(정상_리더[0]).map(why -> "전제 — 평시에 리더가 없다")))
                .assertDuring(() -> RecoveryCriteria.violations(
                        주입이_걸렸다(카나리[0], 카나리[1]),
                        // **리더가 사라지는 것을 사실로 적는다.** 받아들인 동작이라 여기서
                        // 리더가 남기를 기대하면 시나리오가 아니라 소원이 된다. 이 값이
                        // 뒤집히는 날이 시한 관계가 바뀐 날이고, 그때 줄 등록도 다시 본다.
                        리더가_사라졌다(놓침[0]),
                        // **맞바꾼 쪽이 서 있는가.** 명령 상한을 안 내린 이유가 이것이다.
                        줄_등록이_산다(등록_실패[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        리더가_돌아왔다(리더_복귀[0]),
                        배분이_돌아왔다(발행[0], 발행[1])))
                .run();
    }

    /** 주입이 정말 걸렸는가. 안 걸렸으면 뒤의 판정이 아무것도 안 잰다. */
    private Optional<String> 주입이_걸렸다(long 정상, long 장애중) {
        return 장애중 - 정상 >= 지연().toMillis() / 2 ? Optional.empty()
                : Optional.of("전제 — 카나리가 %dms → %dms 로 안 느려졌다".formatted(정상, 장애중));
    }

    private Optional<String> 리더가_사라졌다(int 놓침) {
        return 놓침 > 0 ? Optional.empty()
                : Optional.of("유지 — 연장 밴드 지연에서 리더가 한 번도 안 사라졌다. 연장 시도와 "
                        + "명령 상한의 관계가 바뀌었는지 보고, 줄 등록 문턱을 같이 다시 잰다");
    }

    private Optional<String> 줄_등록이_산다(int 실패) {
        return 실패 == 0 ? Optional.empty()
                : Optional.of("유지 — 줄 등록이 %d/%d 번 실패했다. 요청 경로가 fail-open 으로 넘어간다"
                        .formatted(실패, 등록_수));
    }

    private Optional<String> 리더가_돌아왔다(boolean 리더) {
        return 리더 ? Optional.empty() : Optional.of("회복 — 지연을 걷어도 리더가 없다");
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

package com.kafkick.waiting.chaos;

import com.kafkick.waiting.WaitingApplication;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.GatewayRegistry;
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
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * C6b — 실제 노드가 곱게 내려가면 남은 노드의 분모가 등록 해제로 곧 내려온다 (CY-929).
 *
 * <p>C6 은 레디스 해시를 고쳐 노드 소실을 흉내 내 <b>죽는 노드 자신의 종료 경로</b>를 안 밟는다. 여기서는
 * 두 번째 노드를 띄웠다 닫아 드레인·등록 해제가 실제로 돌게 한다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
class NodeShutdownDenominatorScenarioTest {

    /**
     * 닫힌 뒤 분모가 내려와야 하는 한계. 해제됐으면 다음 하트비트부터 한 대로 세어 감소 확정 세 틱 뒤다.
     * 해제 여부는 등록을 직접 봐서 가르고, 이것은 감소 확정이 늦어지는 회귀를 거른다.
     */
    private static final Duration 해제_한계 = Duration.ofMillis(4_500);

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

    @Autowired
    private GatewayRegistry registry;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Test
    @DisplayName("C6b_노드가_곱게_내려가면_남은_노드의_분모가_곧_내려온다")
    void C6b_노드가_곱게_내려가면_남은_노드의_분모가_곧_내려온다() {
        SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                "http://localhost:" + 뒷단.port(), false);
        int[] 내려가기_전 = new int[1];
        Duration[] 내려오기까지 = new Duration[1];
        int[] 회복_뒤 = new int[1];
        long[] 닫은_직후_등록 = new long[1];

        try {
            ChaosScenario.named("C6b 노드 종료 → 분모")
                    .baseline(() -> {
                        Awaitility.await().atMost(기다림).until(() -> registry.count() == 2);
                        내려가기_전[0] = registry.count();
                    })
                    .inject(() -> {
                        둘째.close();
                        // **해제를 직접 본다.** 시간으로만 가르면 종료가 느린 날 해제 탓으로 빨개진다.
                        닫은_직후_등록[0] = 등록된_노드_수();
                        // 시간은 닫힌 뒤부터 잰다. 컨텍스트 종료 시간을 섞지 않는다.
                        내려오기까지[0] = 분모가_내려오기까지(System.nanoTime());
                    })
                    .duringFault(() -> {
                        // 내려온 뒤로 다시 오르지 않는다. 닫힌 노드의 표가 남아 오르내리면 분모가 흔들린다.
                        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5))
                                .until(() -> registry.count() == 1);
                    })
                    .recover(() -> {
                    })
                    .afterRecovery(() -> 회복_뒤[0] = registry.count())
                    .assertEntry(() -> RecoveryCriteria.violations(
                            내려가기_전[0] == 2 ? Optional.empty()
                                    : Optional.of("전제 — 두 노드가 분모에 안 잡혔다: " + 내려가기_전[0])))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            닫은_직후_등록[0] == 1 ? Optional.empty()
                                    : Optional.of("닫은 노드가 등록에 남았다 — 등록 해제가 안 돌았다: %d 대"
                                            .formatted(닫은_직후_등록[0])),
                            곧_내려왔다(내려오기까지[0])))
                    // 되살리는 판이 아니다. 남은 한 대로 분모가 굳었는지만 본다.
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            회복_뒤[0] == 1 ? Optional.empty()
                                    : Optional.of("남은 노드의 분모가 한 대로 안 굳었다: " + 회복_뒤[0])))
                    .run();
        } finally {
            둘째.close();
        }
    }

    /** 레디스 등록에 남은 노드 수. 표·통과 수 필드는 {@code #} 로 시작해 뺀다. */
    private long 등록된_노드_수() {
        return redis.<String, String>opsForHash().keys(RedisKeys.INSTANCES)
                .filter(field -> !field.startsWith("#")).count().block(기다림);
    }

    private Duration 분모가_내려오기까지(long 시작) {
        try {
            Awaitility.await().pollInterval(Duration.ofMillis(50)).atMost(기다림)
                    .until(() -> registry.count() == 1);
            return Duration.ofNanos(System.nanoTime() - 시작);
        } catch (ConditionTimeoutException e) {
            return null;
        }
    }

    private Optional<String> 곧_내려왔다(Duration 걸림) {
        if (걸림 == null) {
            return Optional.of("노드를 닫았는데 %s 안에 분모가 안 내려왔다".formatted(기다림));
        }
        return 걸림.compareTo(해제_한계) <= 0 ? Optional.empty()
                : Optional.of("닫힌 뒤 분모가 내려오기까지 %s 걸렸다 (한계 %s) — 감소 확정이 늦다"
                        .formatted(걸림, 해제_한계));
    }
}

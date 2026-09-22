package com.kafkick.waiting.chaos;

import com.kafkick.waiting.WaitingApplication;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.control.GatewayRegistry;
import java.time.Duration;
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
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * C6b — 실제 노드가 곱게 내려가면 그 노드가 등록에서 빠지고 남은 노드의 분모가 내려온다 (CY-929).
 *
 * <p>C6 은 레디스 해시를 고쳐 노드 소실을 흉내 내 <b>죽는 노드 자신의 종료 경로</b>를 안 밟는다. 여기서는
 * 두 번째 노드를 띄웠다 닫아 드레인·등록 해제가 실제로 돌게 한다. 감소 확정 시간은 레지스트리 시험이 잰다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
class NodeShutdownDenominatorScenarioTest {

    /** 교착을 잡는 한계. 시간 정책을 재는 값이 아니다. */
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
    @DisplayName("C6b_노드가_곱게_내려가면_그_노드가_빠지고_분모가_내려온다")
    void C6b_노드가_곱게_내려가면_그_노드가_빠지고_분모가_내려온다() {
        // **띄우기 전의 등록을 적어 둔다.** 뒤와 견줘 두 번째 노드의 식별자를 가린다.
        Awaitility.await().atMost(기다림).until(() -> 등록된_노드().size() == 1);
        Set<String> 첫_노드 = 등록된_노드();
        SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                "http://localhost:" + 뒷단.port(), false);
        Set<String> 둘째_노드 = new HashSet<>();
        Set<String> 닫은_직후 = new HashSet<>();
        boolean[] 분모가_내려왔다 = new boolean[1];
        int[] 내려가기_전 = new int[1];

        try {
            ChaosScenario.named("C6b 노드 종료 → 분모")
                    .baseline(() -> {
                        Awaitility.await().atMost(기다림).until(() -> registry.count() == 2);
                        내려가기_전[0] = registry.count();
                        Set<String> 뒤 = 등록된_노드();
                        뒤.removeAll(첫_노드);
                        둘째_노드.addAll(뒤);
                    })
                    .inject(() -> {
                        둘째.close();
                        닫은_직후.addAll(등록된_노드());
                    })
                    .duringFault(() -> {
                        try {
                            Awaitility.await().pollInterval(Duration.ofMillis(50)).atMost(기다림)
                                    .until(() -> registry.count() == 1);
                            분모가_내려왔다[0] = true;
                        } catch (ConditionTimeoutException e) {
                            분모가_내려왔다[0] = false;
                        }
                    })
                    .recover(() -> {
                    })
                    // 내려온 뒤로 다시 오르지 않는다. 닫힌 노드의 표가 남아 오르내리면 분모가 흔들린다.
                    .afterRecovery(() -> Awaitility.await().during(Duration.ofSeconds(3))
                            .atMost(Duration.ofSeconds(5)).until(() -> registry.count() == 1))
                    .assertEntry(() -> RecoveryCriteria.violations(
                            내려가기_전[0] == 2 && 둘째_노드.size() == 1 ? Optional.empty()
                                    : Optional.of("전제 — 두 번째 노드가 분모와 등록에 안 잡혔다: 분모 %d, 새 등록 %s"
                                            .formatted(내려가기_전[0], 둘째_노드))))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            // **누가 빠졌는지 본다.** 수만 보면 남은 노드를 지우고 닫힌 노드를 남겨도 초록이다.
                            닫은_직후.equals(첫_노드) ? Optional.empty()
                                    : Optional.of("닫은 직후 등록이 첫 노드만이 아니다 — 등록 %s, 첫 노드 %s, 닫은 노드 %s"
                                            .formatted(닫은_직후, 첫_노드, 둘째_노드)),
                            분모가_내려왔다[0] ? Optional.empty()
                                    : Optional.of("노드를 닫았는데 %s 안에 분모가 안 내려왔다".formatted(기다림))))
                    .assertRecovery(ChaosScenario.Verdict.none())
                    .run();
        } finally {
            둘째.close();
        }
    }

    /** 레디스 등록에 있는 노드 식별자. 표·통과 수 필드는 {@code #} 로 시작해 뺀다. */
    private Set<String> 등록된_노드() {
        return new HashSet<>(redis.<String, String>opsForHash().keys(RedisKeys.INSTANCES)
                .filter(field -> !field.startsWith("#")).collectList().block(기다림));
    }
}

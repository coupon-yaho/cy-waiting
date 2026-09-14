package com.kafkick.waiting.chaos;

import com.kafkick.waiting.WaitingApplication;
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
     * 분모가 내려와야 하는 한계. 등록 해제가 돌면 다음 하트비트부터 한 대로 세어 감소 확정 세 틱(약 3초)
     * 뒤 내려온다. 해제가 빠지면 표 신선도(3초)가 지나야 빠져 약 6초다. 둘 사이에 둔다.
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

    @Test
    @DisplayName("C6b_노드가_곱게_내려가면_남은_노드의_분모가_곧_내려온다")
    void C6b_노드가_곱게_내려가면_남은_노드의_분모가_곧_내려온다() {
        SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                "http://localhost:" + 뒷단.port(), false);
        int[] 내려가기_전 = new int[1];
        Duration[] 내려오기까지 = new Duration[1];
        int[] 회복_뒤 = new int[1];

        try {
            ChaosScenario.named("C6b 노드 종료 → 분모")
                    .baseline(() -> {
                        Awaitility.await().atMost(기다림).until(() -> registry.count() == 2);
                        내려가기_전[0] = registry.count();
                    })
                    .inject(() -> {
                        long 시작 = System.nanoTime();
                        둘째.close();
                        내려오기까지[0] = 분모가_내려오기까지(시작);
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
                    .assertDuring(() -> RecoveryCriteria.violations(곧_내려왔다(내려오기까지[0])))
                    // 되살리는 판이 아니다. 남은 한 대로 분모가 굳었는지만 본다.
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            회복_뒤[0] == 1 ? Optional.empty()
                                    : Optional.of("남은 노드의 분모가 한 대로 안 굳었다: " + 회복_뒤[0])))
                    .run();
        } finally {
            둘째.close();
        }
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
                : Optional.of("분모가 내려오기까지 %s 걸렸다 (한계 %s) — 등록 해제가 안 돌았는지 본다"
                        .formatted(걸림, 해제_한계));
    }
}

package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.WaitingApplication;
import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.domain.allocation.Grant;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;

/**
 * 임기 울타리가 <b>두 노드 사이에서</b> 문으로 서는가 (CY-976).
 *
 * <p>지금까지 이것을 재는 시험은 전부 한 노드에서 유령 임기를 손으로 써 넣어 돌았다. 그
 * 방식은 봉인 코드를 한 줄도 안 지나므로 스크립트가 숫자를 비교하는지만 잰다. 여기는
 * 둘째 노드가 실제로 봉인한다.
 */
@Tag("chaos")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "waiting.scheduler.enabled=false")
class FenceAcrossNodesScenarioTest {

    private static final String COUPON = "cy976-fence";

    private static final Duration 기다림 = Duration.ofSeconds(30);

    /** 첫 노드가 쥐는 임기. 둘째는 이보다 큰 값으로 다시 봉인한다. */
    private static final long 앞_임기 = 1_000L;

    private static final long 뒤_임기 = 2_000L;

    private static RedisFaults faults;

    private static BackendStub 뒷단;

    @Autowired
    private AllocationRedisPort 첫_노드;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @BeforeAll
    static void 띄운다() {
        faults = RedisFaults.시작한다();
        뒷단 = BackendStub.항상_받는다();
    }

    @AfterAll
    static void 내린다() {
        뒷단.close();
        faults.close();
    }

    @org.springframework.test.context.DynamicPropertySource
    static void 배선(org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.url", () -> faults.주소());
        registry.add("waiting.backend.uri", () -> "http://localhost:" + 뒷단.port());
    }

    private boolean 막혔나(Runnable 호출) {
        try {
            호출.run();
            return false;
        } catch (AllocationRedisPort.FencedOutException e) {
            return true;
        }
    }

    /**
     * <b>둘째 노드가 봉인하면 첫 노드의 낡은 임기가 막힌다.</b>
     *
     * <p>막는 것이 손으로 써 넣은 숫자가 아니라 실제로 돈 둘째 노드라는 것이 이 시험의 값이다.
     */
    @Test
    @DisplayName("둘째_노드의_봉인이_첫_노드의_낡은_임기를_막는다")
    void 둘째_노드의_봉인이_첫_노드의_낡은_임기를_막는다() {
        // **문이 둘이다.** 쿠폰별 울타리와 발행 울타리를 따로 잠근다 — 승계 배선도 둘 다
        // 부른다. 하나만 잠그면 나머지 한쪽으로 유령이 그대로 나간다.
        // **지울 것이 있어야 막은 것이 보인다.** 재고 키가 없으면 삭제 스크립트가 "안
        // 지웠다" 로 끝나고, 그 결과가 막힌 것과 똑같은 빈 목록이다 — 울타리를 빼도
        // 초록인 단언이 된다. 매진(0)과 줄 하나를 심어 두 경우를 가른다.
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(기다림);
        redis.opsForZSet().add(RedisKeys.queue(COUPON, 1, 0), "u1", 1.0).block(기다림);

        첫_노드.sealFences(List.of(COUPON), 앞_임기).block(기다림);
        첫_노드.sealSnapshotFence(앞_임기).block(기다림);
        assertThat(막혔나(() -> 첫_노드.apply(new Grant(COUPON, 1), 앞_임기).block(기다림)))
                .as("전제 — 제 임기로는 통과한다").isFalse();

        try (SecondNode 둘째 = SecondNode.띄운다(WaitingApplication.class, faults.주소(),
                "http://localhost:" + 뒷단.port(), false)) {
            AllocationRedisPort 둘째_포트 =
                    둘째.빈("allocationRedisPort", AllocationRedisPort.class);
            // **첫 노드의 것과 다른 객체여야 한다.** 같은 것을 집어 오면 두 노드를 띄운 뜻이
            // 사라지고, 한 노드가 제 임기를 두 번 쓰는 시험이 된다.
            assertThat(둘째_포트).as("전제 — 둘째 노드가 제 어댑터를 들고 떴다")
                    .isNotSameAs(첫_노드);
            assertThat(둘째.port()).as("전제 — 둘째가 제 포트로 떴다").isPositive();

            // **둘째가 더 높은 임기로 다시 봉인한다.** 승계가 하는 일이 이것이다.
            둘째_포트.sealFences(List.of(COUPON), 뒤_임기).block(기다림);
            둘째_포트.sealSnapshotFence(뒤_임기).block(기다림);

            assertThat(막혔나(() -> 첫_노드.apply(new Grant(COUPON, 1), 앞_임기).block(기다림)))
                    .as("낡은 임기의 적용이 막힌다 — 안 막히면 크레딧이 두 번 쓰인다").isTrue();
            assertThat(막혔나(() -> 첫_노드.publish(Map.of("v", "old"), 앞_임기).block(기다림)))
                    .as("낡은 임기의 발행이 막힌다 — 안 막히면 낡은 재료가 전 노드로 간다").isTrue();
            assertThat(첫_노드.dropSoldOutQueues(List.of(COUPON), 앞_임기).block(기다림))
                    .as("낡은 임기의 매진 정리가 막힌다 — 되살리는 코드가 없다").isEmpty();
            // **줄이 남았는지를 직접 본다.** 빈 목록은 막힌 것·지울 게 없는 것·오류를
            // 삼킨 것 셋을 다 뜻한다. 남은 줄만이 안 지웠다는 사실이다.
            assertThat(redis.hasKey(RedisKeys.queue(COUPON, 1, 0)).block(기다림))
                    .as("줄이 그대로 있다 — 지워졌으면 되살릴 수 없다").isTrue();

            // **둘째는 통과해야 한다.** 다 막히면 문이 선 것이 아니라 잠긴 것이다.
            assertThat(막혔나(() -> 둘째_포트.apply(new Grant(COUPON, 1), 뒤_임기).block(기다림)))
                    .as("봉인한 노드는 통과한다").isFalse();
        }
    }
}

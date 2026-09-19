package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.admission.CircuitState;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * <b>메모리 상한에서도 승계 봉인이 선다</b> (CY-932).
 *
 * <p>배선은 `noeviction` 이라 상한에 닿으면 메모리를 늘리는 쓰기가 거부된다. 리더는 CY-296 에서 서게 했지만 새 리더의
 * 봉인이 첫 쓰기에서 거부되면 문이 안 잠긴 채 열린다.
 */
@Tag("integration")
@SpringBootTest
class ScriptsUnderMemoryLimitTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);

    private static final String COUPON = "oom-seal";

    private static final long FENCE = 1_770_000_000_123_456L;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private AllocationRedisPort port;

    @BeforeEach
    void 준비() {
        port = AllocationRedisPort.of(redis, 1);
        비운다();
    }

    @AfterEach
    void 치운다() {
        비운다();
    }

    private void 비운다() {
        redis.delete(RedisKeys.applyFence(COUPON, 1, 0), RedisKeys.dropFence(COUPON, 1, 0),
                RedisKeys.SNAPSHOT_FENCE).block(WAIT);
        redis.opsForHash().remove(RedisKeys.INSTANCES, "oom-node").block(WAIT);
        redis.delete(RedisKeys.queue(COUPON, 1, 0), RedisKeys.alive(COUPON, 1, 0),
                RedisKeys.stock(COUPON)).block(WAIT);
    }

    /**
     * 하트비트가 거부되면 산 노드가 분모에서 빠진다. 남은 노드가 그만큼 큰 몫을 쓰므로 유입이 예산을 넘는다.
     *
     * <p>상한 중에도 계속 불리는 경로라, 쓰는 양이 노드 수에만 비례한다는 조건과 함께 RD-12 의 대상이다.
     */
    @Test
    @DisplayName("메모리_상한에서도_하트비트가_남는다")
    void 메모리_상한에서도_하트비트가_남는다() throws Exception {
        GatewayRedisPort gateway = GatewayRedisPort.of(redis);

        메모리_상한에서(() -> {
            assertThatThrownBy(() -> redis.opsForValue().set("oom-write-probe", "1").block(WAIT))
                    .as("전제 — 상한이 실제로 걸려 메모리를 늘리는 쓰기가 막힌다")
                    .rootCause().hasMessageContaining("OOM");

            assertThat(gateway.beat("oom-node", 30, 3, CircuitState.CLOSED, 0).block(WAIT).alive())
                    .as("상한 중에도 제 자리를 남긴다").isPositive();
        });
    }

    /**
     * 매진 큐 정리가 거부되면 상한을 푸는 길이 막힌다 — 메모리를 줄이는 쪽이 이 정리다.
     *
     * <p>표만 세우는 갈래도 쿠폰마다 수명 있는 키 하나라 사람 수에 안 비례한다 (RD-12).
     */
    @Test
    @DisplayName("메모리_상한에서도_매진_큐를_지운다")
    void 메모리_상한에서도_매진_큐를_지운다() throws Exception {
        redis.opsForZSet().add(RedisKeys.queue(COUPON, 1, 0), "m1", 1).block(WAIT);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);
        // 표가 없으면 안 지운다. 후보로 오른 첫 회차에 서는 것이라 상한 전에 세운다.
        port.sealFences(List.of(COUPON), FENCE).block(WAIT);

        메모리_상한에서(() -> {
            assertThatThrownBy(() -> redis.opsForValue().set("oom-write-probe", "1").block(WAIT))
                    .as("전제 — 상한이 실제로 걸려 메모리를 늘리는 쓰기가 막힌다")
                    .rootCause().hasMessageContaining("OOM");

            assertThat(port.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT))
                    .as("상한 중에도 매진 줄을 지운다").containsExactly(COUPON);
        });
    }

    /** 승계 잠금이 거부되면 게이트가 안 잠긴 채 열려, 쓰기가 풀리는 순간 유령의 지연된 몫이 먼저 들어간다. */
    @Test
    @DisplayName("메모리_상한에서도_입장과_삭제의_문을_잠근다")
    void 메모리_상한에서도_입장과_삭제의_문을_잠근다() throws Exception {
        메모리_상한에서(() -> {
            assertThat(port.sealFences(List.of(COUPON), FENCE).block(WAIT)).isEqualTo(1);

            assertThat(redis.opsForValue().get(RedisKeys.applyFence(COUPON, 1, 0)).block(WAIT))
                    .isEqualTo(Long.toString(FENCE));
            assertThat(redis.opsForValue().get(RedisKeys.dropFence(COUPON, 1, 0)).block(WAIT))
                    .isEqualTo(Long.toString(FENCE));
        });
    }

    @Test
    @DisplayName("메모리_상한에서도_발행의_문을_잠근다")
    void 메모리_상한에서도_발행의_문을_잠근다() throws Exception {
        메모리_상한에서(() -> assertThat(port.sealSnapshotFence(FENCE).block(WAIT)).isEqualTo(1));
    }

    /**
     * <b>상한 중 발행이 거부되면 발행의 문을 다시 잠근다.</b> 그 표는 발행이 매 틱 수명을 새로 거는데, 상한이 수명보다
     * 길면 봉인이 사라진 채 풀려 멎었던 옛 리더의 발행이 새 리더보다 먼저 들어간다.
     */
    @Test
    @DisplayName("메모리_상한에서_발행이_거부되면_발행의_문을_다시_잠근다")
    void 메모리_상한에서_발행이_거부되면_발행의_문을_다시_잠근다() throws Exception {
        // 재봉인 수명(10초)보다 짧게 두되, 상한을 거는 컨테이너 명령 사이에 안 만료될 만큼은 길게 둔다.
        redis.opsForValue().set(RedisKeys.SNAPSHOT_FENCE, Long.toString(FENCE), Duration.ofSeconds(5)).block(WAIT);

        메모리_상한에서(() -> {
            Duration 발행_전 = redis.getExpire(RedisKeys.SNAPSHOT_FENCE).block(WAIT);
            assertThat(발행_전)
                    .as("전제 — 표가 아직 살아 있다. 만료됐으면 다시 거는 것이 아니라 새로 거는 것을 잰다")
                    .isPositive();
            assertThatThrownBy(() -> port.publish(Map.of("f", "v"), FENCE).block(WAIT))
                    .as("발행은 사람 수만큼 쓰는 스크립트라 상한에서 거부된다")
                    .isNotInstanceOf(AllocationRedisPort.FencedOutException.class)
                    .rootCause().hasMessageContaining("OOM");

            // 재봉인은 회차에서 떼어 보낸다. 곧 도착한다. 수명은 줄기만 하므로 늘었으면 다시 건 것이다.
            Awaitility.await().atMost(WAIT).pollInterval(Duration.ofMillis(50)).until(() ->
                    redis.getExpire(RedisKeys.SNAPSHOT_FENCE).block(WAIT).compareTo(발행_전) > 0);
            assertThat(redis.opsForValue().get(RedisKeys.SNAPSHOT_FENCE).block(WAIT)).isEqualTo(Long.toString(FENCE));
        });
    }
}

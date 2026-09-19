package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.LeaderLock;
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
                RedisKeys.SNAPSHOT_FENCE, RedisKeys.LEADER, RedisKeys.LEADER_GENERATION).block(WAIT);
    }

    /**
     * 상한에서 리더를 못 잡으면 배분이 영영 멎는다 — 메모리를 줄이는 청소를 도는 것이 리더다.
     *
     * <p>획득 스크립트의 {@code allow-oom} 이 그것을 막는다. 그 선언이 사라지는 것을 무는 자리가
     * 여기 말고 없다.
     */
    @Test
    @DisplayName("메모리_상한에서도_리더를_잡는다")
    void 메모리_상한에서도_리더를_잡는다() throws Exception {
        LeaderRedisPort leader = new LeaderRedisPort(redis, Duration.ofSeconds(2));

        메모리_상한에서(() -> {
            assertThatThrownBy(() -> redis.opsForValue().set("oom-write-probe", "1").block(WAIT))
                    .as("전제 — 상한이 실제로 걸려 메모리를 늘리는 쓰기가 막힌다")
                    .rootCause().hasMessageContaining("OOM");

            LeaderLock 잡은_것 = leader.acquire("oom-owner").block(WAIT);

            assertThat(잡은_것.acquired()).as("상한 중에도 잡는다").isTrue();
            assertThat(잡은_것.fence()).as("임기를 매긴다").isPositive();
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

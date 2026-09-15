package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * <b>메모리 상한에서도 해제 조건이 시스템 안에 있다</b> (CY-932).
 *
 * <p>배선은 `noeviction` 이라 상한에 닿으면 메모리를 늘리는 쓰기가 거부된다. 리더는 CY-296 에서 서게 했지만 새 리더의
 * 봉인과 매진 큐 삭제가 첫 쓰기에서 같이 거부되면, 문이 안 잠긴 채 열리고 메모리를 줄일 길이 막힌다.
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
                RedisKeys.queue(COUPON, 1, 0), RedisKeys.alive(COUPON, 1, 0), RedisKeys.stock(COUPON),
                RedisKeys.SNAPSHOT_FENCE).block(WAIT);
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

    /** 매진 큐 삭제는 메모리를 줄이는 길이다. 막히면 풀리는 길이 수명 만료와 운영자뿐이다. */
    @Test
    @DisplayName("메모리_상한에서도_매진_큐를_지운다")
    void 메모리_상한에서도_매진_큐를_지운다() throws Exception {
        redis.opsForZSet().add(RedisKeys.queue(COUPON, 1, 0), "m1", 100).block(WAIT);
        redis.opsForZSet().add(RedisKeys.alive(COUPON, 1, 0), "m1", 200).block(WAIT);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);
        port.claimSoldOutQueues(List.of(COUPON), FENCE).block(WAIT);

        메모리_상한에서(() -> {
            assertThat(port.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT)).containsExactly(COUPON);

            assertThat(redis.hasKey(RedisKeys.queue(COUPON, 1, 0)).block(WAIT)).isFalse();
        });
    }
}

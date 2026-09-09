package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.chaos.RedisFaults;
import io.lettuce.core.RedisURI;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

/**
 * 승계가 <b>세 문을 다 잠그는지</b>를 배선에서 잰다 (CY-911).
 *
 * <p>글자로만 보면 못 잡는 것이 있다. 잠금은 게을러서, 만들어 놓고 구독을 안 하면
 * 이름은 그대로 남은 채 아무 일도 안 일어난다.
 */
@Tag("integration")
class SealFencesWiringTest {

    private static final String COUPON = "seal-wiring";

    private static final int SHARDS = 1;

    private static final Duration 기다림 = Duration.ofSeconds(10);

    private static final Duration 리스 = Duration.ofSeconds(2);

    /** 시도 상한은 리스의 1/5 이하여야 한다. 넘기면 리더십이 안 만들어진다. */
    private static final Duration 시도 = Duration.ofMillis(200);

    private static final long 임기 = 1_770_000_000_123_456L;

    private static RedisFaults faults;

    private static LettuceConnectionFactory factory;

    private static ReactiveStringRedisTemplate redis;

    private AllocationRedisPort port;

    @BeforeAll
    static void 띄운다() {
        faults = RedisFaults.시작한다();
        RedisURI uri = RedisURI.create(faults.주소());
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(uri.getHost(), uri.getPort()),
                LettuceClientConfiguration.builder().commandTimeout(기다림).build());
        factory.afterPropertiesSet();
        redis = new ReactiveStringRedisTemplate(factory);
    }

    @AfterAll
    static void 내린다() {
        factory.destroy();
        faults.close();
    }

    @BeforeEach
    void 비운다() {
        redis.delete(RedisKeys.ACTIVE_COUPONS, RedisKeys.SNAPSHOT_FENCE,
                RedisKeys.applyFence(COUPON, SHARDS, 0),
                RedisKeys.dropFence(COUPON, SHARDS, 0)).block(기다림);
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(기다림);
        port = AllocationRedisPort.of(redis, SHARDS);
    }

    @Test
    @DisplayName("승계가_입장과_삭제와_발행의_문을_다_잠근다")
    void 승계가_입장과_삭제와_발행의_문을_다_잠근다() {
        SealGate gate = SealGate.of(() -> true);

        new ControlPlaneConfig().sealFences(port, 리더가_된다(), gate).run();

        Awaitility.await().atMost(기다림).until(gate::getAsBoolean);
        // 발행 잠금은 게이트가 안 기다린다. 그 문의 목적이 배분을 세우는 것이 아니다.
        Awaitility.await().atMost(기다림).until(() -> 표(RedisKeys.SNAPSHOT_FENCE) == 임기);
        assertThat(표(RedisKeys.applyFence(COUPON, SHARDS, 0))).isEqualTo(임기);
        assertThat(표(RedisKeys.dropFence(COUPON, SHARDS, 0))).isEqualTo(임기);
        assertThat(표(RedisKeys.SNAPSHOT_FENCE))
                .as("안 잠그면 첫 발행 전까지 유령의 발행이 통과하고 청소가 따라 나간다")
                .isEqualTo(임기);
    }

    /** 문을 못 잠가도 회차는 연다. 여기서 멈추면 아무도 배분을 안 돌아 줄이 통째로 멎는다. */
    @Test
    @DisplayName("리더가_아니면_안_잠그고도_문을_연다")
    void 리더가_아니면_안_잠그고도_문을_연다() {
        SealGate gate = SealGate.of(() -> true);
        Leadership 강등된_노드 = Leadership.of("node-2", 리스, 시도,
                () -> Mono.just(LeaderLock.heldBy("node-1", 리스.toMillis())), Mono::empty);

        new ControlPlaneConfig().sealFences(port, 강등된_노드, gate).run();

        Awaitility.await().atMost(기다림).until(gate::getAsBoolean);
        assertThat(redis.hasKey(RedisKeys.SNAPSHOT_FENCE).block(기다림))
                .as("리더가 아닌 번호가 서면 그 뒤의 모든 발행이 통과한다").isFalse();
    }

    private Leadership 리더가_된다() {
        Leadership leadership = Leadership.of("node-1", 리스, 시도,
                () -> Mono.just(LeaderLock.mine("node-1", 리스.toMillis(), 임기)), Mono::empty);
        leadership.renew().block(기다림);
        assertThat(leadership.fence()).as("임기를 못 쥐면 그 뒤 판정이 전부 무의미하다")
                .isEqualTo(임기);
        return leadership;
    }

    private long 표(String key) {
        String raw = redis.opsForValue().get(key).block(기다림);
        return raw == null ? 0 : Long.parseLong(raw);
    }
}

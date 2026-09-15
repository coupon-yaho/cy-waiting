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
import reactor.core.scheduler.Schedulers;
import reactor.test.scheduler.VirtualTimeScheduler;

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

        new ControlPlaneConfig().sealFences(port, 리더가_된다(), gate, 기다림, Schedulers.parallel()).run();

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

        new ControlPlaneConfig().sealFences(port, 강등된_노드, gate, 기다림,
                Schedulers.parallel()).run();

        Awaitility.await().atMost(기다림).until(gate::getAsBoolean);
        assertThat(redis.hasKey(RedisKeys.SNAPSHOT_FENCE).block(기다림))
                .as("리더가 아닌 번호가 서면 그 뒤의 모든 발행이 통과한다").isFalse();
    }

    /**
     * <b>잠금이 시한을 넘기면 문을 연다.</b> 문이 승계 첫 회차를 세우므로 잠금에 끝이 없으면
     * 레디스가 매달린 동안 새 리더가 배분을 안 돈다. 못 잠근 쿠폰은 적용이 다시 막는다.
     */
    @Test
    @DisplayName("잠금이_시한을_넘기면_문을_연다")
    void 잠금이_시한을_넘기면_문을_연다() {
        SealGate gate = SealGate.of(() -> true);
        Leadership leadership = 리더가_된다();
        // **잠금 시한은 가상 시간으로 넘긴다.** 레디스가 끊겨 명령은 명령 시한(기다림)까지
        // 매달리므로, 그 전에 문이 열리면 잠금 시한이 연 것이다.
        VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
        Duration 시한 = Duration.ofSeconds(1);
        faults.끊는다();
        try {
            new ControlPlaneConfig().sealFences(port, leadership, gate, 시한, 시계).run();
            assertThat(gate.getAsBoolean()).as("시한 전에는 잠그는 중이다").isFalse();

            시계.advanceTimeBy(시한);

            assertThat(gate.getAsBoolean()).as("시한이 지나면 명령 시한을 안 기다리고 연다").isTrue();
        } finally {
            faults.붙인다();
        }
    }

    /**
     * <b>승계가 앞 리더의 마지막 적용에서 한 틱을 잇는다</b> (CY-933). 승계 대기는 발행 나이만 봐서, 발행이 잘리고 적용만
     * 들어간 뒤 넘겨받으면 새 리더의 첫 적용이 1초 안에 겹쳐 두 틱 몫이 들어간다.
     */
    @Test
    @DisplayName("승계가_앞_리더의_마지막_적용에서_간격을_잇는다")
    void 승계가_앞_리더의_마지막_적용에서_간격을_잇는다() {
        // 앞 리더가 이 노드(1시간)보다 긴 수명으로 표를 썼다. 나이가 0 으로 잘려 한 틱을 다 기다린다 —
        // 레디스의 실제 시간이 흘러도 값이 안 바뀐다. 나이 변환 자체는 FenceSealTest 가 본다.
        redis.opsForValue().set(RedisKeys.applyFence(COUPON, SHARDS, 0), Long.toString(임기 - 1),
                Duration.ofHours(2)).block(기다림);
        SealGate gate = SealGate.of(() -> true);
        VirtualTimeScheduler 페이서_시계 = VirtualTimeScheduler.create();
        ApplyPacer pacer = ApplyPacer.of(Duration.ofSeconds(1), 페이서_시계);

        new ControlPlaneConfig().sealFences(port, 리더가_된다(), gate, 기다림, Schedulers.parallel(), pacer)
                .run();
        Awaitility.await().atMost(기다림).until(gate::getAsBoolean);

        assertThat(pacer.holdOff()).as("앞 적용이 방금이라 한 틱을 다 기다린다").isEqualTo(Duration.ofSeconds(1));
        페이서_시계.advanceTimeBy(Duration.ofSeconds(1));
        assertThat(pacer.holdOff()).as("앞 적용에서 한 틱이 지났다").isZero();
    }

    @Test
    @DisplayName("앞_적용이_없으면_간격을_안_둔다")
    void 앞_적용이_없으면_간격을_안_둔다() {
        SealGate gate = SealGate.of(() -> true);
        ApplyPacer pacer = ApplyPacer.of(Duration.ofSeconds(1), VirtualTimeScheduler.create());

        new ControlPlaneConfig().sealFences(port, 리더가_된다(), gate, 기다림, Schedulers.parallel(), pacer)
                .run();
        Awaitility.await().atMost(기다림).until(gate::getAsBoolean);

        assertThat(pacer.holdOff()).isZero();
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

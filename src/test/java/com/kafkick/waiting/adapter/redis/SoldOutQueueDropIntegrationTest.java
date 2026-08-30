package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.AllocationRound;
import com.kafkick.waiting.control.QueueSweeper;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.control.SoldOutCleanup;
import com.kafkick.waiting.control.SweepGate;
import com.kafkick.waiting.control.TimedDemands;
import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

/**
 * G7.9 — 매진된 쿠폰의 줄이 <b>실제로 레디스에서 사라진다</b>.
 *
 * <p>배분 판 하나를 레디스에 대고 돌린다. 판정·유예·스크립트가 다 이어져야
 * 키가 없어지므로, 어느 한 곳이 끊기면 여기가 빨개진다.
 */
@Tag("integration")
@SpringBootTest
class SoldOutQueueDropIntegrationTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String SOLD_OUT = "g79-soldout";
    private static final String ALIVE = "g79-alive";
    private static final long FENCE = 1_700_000_000_000_000L;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private AllocationRedisPort port;

    @BeforeEach
    void 준비() {
        port = AllocationRedisPort.of(redis, 1);
        redis.delete(RedisKeys.queue(SOLD_OUT, 1, 0), RedisKeys.alive(SOLD_OUT, 1, 0),
                RedisKeys.stock(SOLD_OUT), RedisKeys.dropFence(SOLD_OUT, 1, 0),
                RedisKeys.admitted(SOLD_OUT, 1, 0),
                RedisKeys.queue(ALIVE, 1, 0), RedisKeys.stock(ALIVE)).block(WAIT);
        줄을_세운다(SOLD_OUT, "0");
        줄을_세운다(ALIVE, "9");
    }

    private void 줄을_세운다(String couponId, String stock) {
        redis.opsForZSet().add(RedisKeys.queue(couponId, 1, 0), "m1", 100).block(WAIT);
        redis.opsForValue().set(RedisKeys.stock(couponId), stock).block(WAIT);
    }

    private boolean 줄이_있나(String couponId) {
        return Boolean.TRUE.equals(redis.hasKey(RedisKeys.queue(couponId, 1, 0)).block(WAIT));
    }

    /** 유예 틱 1 짜리로 판을 만든다. 실제 유예는 폴링 상한보다 길어 시험에 못 쓴다. */
    private AllocationRound 판(long fence) {
        return AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(List.of(
                        new CouponDemand(SOLD_OUT, 30, 0, QueueMode.ADAPTIVE),
                        new CouponDemand(ALIVE, 30, 9, QueueMode.ADAPTIVE)),
                        1_700_000_000L)),
                () -> 100, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> Mono.empty(),
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(CreditSmoother.of(1.0)),
                SnapshotCodec.create(), () -> 0L, Optional::empty,
                SoldOutCleanup.of(1, new SimpleMeterRegistry()),
                ids -> port.dropSoldOutQueues(ids, fence),
                QueueSweeper.of(
                        SweepGate.of(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                        (ids, limit) -> Mono.just(QueueSweeper.SweepResult.NOTHING)),
                () -> false);
    }

    /**
     * <b>G7.9.</b> 매진된 쿠폰의 줄이 유예를 넘기면 키가 없어진다.
     *
     * <p>살아 있는 쿠폰을 나란히 둔다. 둘 다 지우는 구현도 매진 쪽만 보면
     * 통과하므로, 안 지워야 하는 것이 남는지까지 봐야 판정이 된다.
     */
    @Test
    @DisplayName("매진된_줄은_유예_뒤_레디스에서_사라진다")
    void 매진된_줄은_유예_뒤_레디스에서_사라진다() {
        AllocationRound round = 판(FENCE);

        round.run().block(WAIT);
        assertThat(줄이_있나(SOLD_OUT)).as("유예 안에서는 안 지운다").isTrue();

        round.run().block(WAIT);

        assertThat(줄이_있나(SOLD_OUT)).as("유예를 넘기면 사라진다").isFalse();
        assertThat(줄이_있나(ALIVE)).as("살아 있는 쿠폰은 그대로").isTrue();
    }

    /**
     * <b>리더가 아니면 아무것도 안 지운다.</b> 판 번호 0 은 강등된 노드가
     * 들고 나가는 값이다 — 울타리가 전부 거절해야 한다.
     */
    @Test
    @DisplayName("판_번호가_없으면_안_지운다")
    void 판_번호가_없으면_안_지운다() {
        AllocationRound round = 판(0);

        round.run().block(WAIT);
        round.run().block(WAIT);

        assertThat(줄이_있나(SOLD_OUT)).as("울타리가 막는다").isTrue();
    }
}

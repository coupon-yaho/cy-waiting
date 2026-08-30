package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.AllocationRound;
import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.control.Leadership;
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
@SpringBootTest(properties = "waiting.scheduler.enabled=true")
class SoldOutQueueDropIntegrationTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String SOLD_OUT = "g79-soldout";
    private static final String ALIVE = "g79-alive";
    private static final long FENCE = 1_700_000_000_000_000L;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    /**
     * <b>실제로 배선된 판을 돌린다.</b> 손으로 조립하면 삭제를 스텁으로 되돌린
     * 변경이 그대로 통과한다 — 이 티켓이 이은 바로 그 자리가 시험 밖에 남는다.
     */
    @Autowired
    private AllocationRound wired;

    @Autowired
    private Leadership leadership;

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
                ids -> Mono.just(List.of()),
                QueueSweeper.of(
                        SweepGate.of(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                        (ids, limit) -> Mono.just(QueueSweeper.SweepResult.NOTHING)),
                () -> false);
    }

    /**
     * <b>G7.9 — 배선까지 포함해 잰다.</b>
     *
     * <p>실제 빈을 돌린다. 삭제를 스텁으로 되돌리면 여기가 빨개진다.
     */
    @Test
    @DisplayName("배선된_판이_매진_줄을_지운다")
    void 배선된_판이_매진_줄을_지운다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, SOLD_OUT).block(WAIT);
        // **락을 비우고 잡는다.** 리더 시험들이 같은 키를 남기므로, 안 비우면
        // 이 판이 리더가 못 되어 정리가 아예 안 돈다.
        redis.delete(RedisKeys.LEADER).block(WAIT);
        leadership.renew().block(WAIT);
        assertThat(leadership.isLeader()).as("전제 — 이 노드가 리더다").isTrue();

        // **설정한 유예만큼 돈다.** 설정을 시험에서 못 줄인다 — 속성 빈이
        // 기본값을 박아 만든다. 유예 자체는 폴링 최대 간격에 묶여 있어
        // 짧게 잡을 수도 없다.
        int 유예 = ControlPlaneProperties.defaults().scheduler().soldOutGraceTicks();
        for (int i = 0; i <= 유예; i++) {
            wired.run().block(WAIT);
        }

        assertThat(줄이_있나(SOLD_OUT)).as("배선이 이어져야 사라진다").isFalse();
        redis.opsForSet().remove(RedisKeys.ACTIVE_COUPONS, SOLD_OUT).block(WAIT);
    }

    /**
     * <b>G7.9.</b> 매진된 쿠폰의 줄이 유예를 넘기면 키가 없어진다.
     *
     * <p>살아 있는 쿠폰을 나란히 둔다. 다만 그 대조는 <b>스크립트의 재고
     * 재확인이 먼저 막아 준다</b> — 판정 계층이 모든 쿠폰을 후보로 올려도
     * 여기는 통과한다. 판정 자체는 {@code SoldOutCleanupTest} 가 잰다.
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

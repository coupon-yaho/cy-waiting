package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.ClockSkewTracker;
import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.coupon.QueueMode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 이월 지표가 <b>제 값을 읽는가</b> (CY-865). 이름이 스크레이프에 나오는지만 보면 결과
 * 셋의 읽기 함수를 서로 바꿔 꽂아도 통과한다.
 */
class InvariantMetricsTest {

    private static final long 읽은_시각 = 1_700_000_000L;

    @Test
    @DisplayName("이월_지표가_결과별_값을_낸다")
    void 이월_지표가_결과별_값을_낸다() {
        AtomicBoolean 터진다 = new AtomicBoolean(true);
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(
                        List.of(new CouponDemand("c1", 5, 100, QueueMode.ADAPTIVE)), 읽은_시각)),
                () -> 1_000, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> 터진다.get() ? Mono.error(new IllegalStateException("흔들린다")) : Mono.empty(),
                () -> Instant.ofEpochSecond(읽은_시각),
                () -> 터진다.get() ? Mono.error(new IllegalStateException("흔들린다"))
                        : Mono.just(CreditSmoother.restore(0.3,
                                new CreditSmoother.Snapshot(200.0, true))),
                SnapshotCodec.create(), () -> 0L);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        InvariantMetrics.bind(round, ClockSkewTracker.create(), meters);

        round.run().onErrorResume(e -> Mono.empty()).block();
        round.run().onErrorResume(e -> Mono.empty()).block();
        터진다.set(false);
        round.run().block();

        assertThat(meters.get("waiting.allocation.carryover.failures").functionCounter().count())
                .isEqualTo(2);
        assertThat(meters.get("waiting.allocation.carryover").tag("outcome", "restored")
                .functionCounter().count()).isEqualTo(1);
        assertThat(meters.get("waiting.allocation.carryover").tag("outcome", "empty")
                .functionCounter().count()).isZero();
        assertThat(meters.get("waiting.allocation.carryover").tag("outcome", "replaced")
                .functionCounter().count()).isZero();
        // 0.3 × 1,000 + 0.7 × 200
        assertThat(meters.get("waiting.allocation.smoothed.credit").gauge().value())
                .isEqualTo(440.0);
    }
}

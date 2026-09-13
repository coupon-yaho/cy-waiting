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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 이월 지표가 <b>제 값을 읽는가</b> (CY-865). 이름이 스크레이프에 나오는지만 보면 결과
 * 셋의 읽기 함수를 서로 바꿔 꽂아도 통과한다.
 */
class InvariantMetricsTest {

    private static final long 읽은_시각 = 1_700_000_000L;

    private static void 돈다(AllocationRound round) {
        round.run().onErrorResume(e -> Mono.empty()).block();
    }

    @Test
    @DisplayName("이월_지표가_결과별_값을_낸다")
    void 이월_지표가_결과별_값을_낸다() {
        Mono<CreditSmoother> 흔들림 = Mono.error(new IllegalStateException("흔들린다"));
        AtomicReference<Mono<CreditSmoother>> 이월 = new AtomicReference<>(흔들림);
        AtomicBoolean 발행이_터진다 = new AtomicBoolean(true);
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(
                        List.of(new CouponDemand("c1", 5, 100, QueueMode.ADAPTIVE)), 읽은_시각)),
                () -> 1_000, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> 발행이_터진다.get()
                        ? Mono.error(new IllegalStateException("흔들린다")) : Mono.empty(),
                () -> Instant.ofEpochSecond(읽은_시각),
                이월::get,
                SnapshotCodec.create(), () -> 0L);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        InvariantMetrics.bind(round, ClockSkewTracker.create(), meters);

        돈다(round);
        이월.set(Mono.just(CreditSmoother.restore(0.3, new CreditSmoother.Snapshot(200.0, true))));
        발행이_터진다.set(false);
        돈다(round);

        // 매번 새로 만든다. 한 벌을 돌려 쓰면 앞 임기가 관측한 스무더가 값 있는 이월로 돌아온다.
        이월.set(Mono.fromSupplier(() -> CreditSmoother.of(0.3)));
        for (int i = 0; i < 2; i++) {
            round.leadershipAcquired();
            돈다(round);
        }
        이월.set(흔들림);
        for (int i = 0; i < 3; i++) {
            round.leadershipAcquired();
            돈다(round);
        }

        // 넷의 값을 서로 달리 둔다 — 같으면 읽기 함수를 서로 바꿔 꽂아도 통과한다.
        assertThat(meters.get("waiting.allocation.carryover.failures").functionCounter().count())
                .isEqualTo(4);
        assertThat(결과(meters, "restored")).isEqualTo(1);
        assertThat(결과(meters, "empty")).isEqualTo(2);
        assertThat(결과(meters, "replaced")).isEqualTo(3);
        // 마지막 임기는 콜드에서 관측 1,000 을 처음 봤다
        assertThat(meters.get("waiting.allocation.smoothed.credit").gauge().value())
                .isEqualTo(1_000.0);
    }

    private static double 결과(SimpleMeterRegistry meters, String outcome) {
        return meters.get("waiting.allocation.carryover").tag("outcome", outcome)
                .functionCounter().count();
    }
}

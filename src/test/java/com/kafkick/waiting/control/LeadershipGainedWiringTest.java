package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * 리더가 된 순간에 셈을 처음부터 주는가 (CY-822).
 *
 * <p>람다에 묻어 두면 한 줄을 빠뜨려도 <b>전 시험이 초록인 채로</b> 그 셈만
 * 얼어 있던 값을 이어 쓴다. 그래서 그 목록 자체를 못 박는다.
 */
class LeadershipGainedWiringTest {

    private static final Map<String, CouponState> 줄이_선_쿠폰 =
            Map.of("c1", CouponStates.queueing(10, 1_000, 100));

    /**
     * <b>이탈자 청소의 재개 유예를 처음부터 준다.</b>
     */
    // 그 표시는 리더 메모리라 승계에서 사라진다. 이 배선이 없으면 같은
    // 프로세스가 리더십을 되찾을 때 방어가 통째로 빠진다 — 그때 틱은 이미
    // 유예를 훌쩍 넘어 있다.
    @Test
    @DisplayName("리더가_되면_청소_유예를_다시_건다")
    void 리더가_되면_청소_유예를_다시_건다() {
        SweepGate gate = SweepGates.warmed(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl());
        QueueSweeper sweeper = QueueSweeper.of(gate,
                (ids, limit, removeFront) -> Mono.just(QueueSweeper.SweepResult.NOTHING),
                new SimpleMeterRegistry());
        assertThat(gate.sweepable(줄이_선_쿠폰, false)).as("전제 — 유예가 이미 풀렸다")
                .containsExactly("c1");

        onLeadershipGained(sweeper).run();

        assertThat(gate.removalHeld()).isTrue();
        assertThat(gate.sweepable(줄이_선_쿠폰, false)).isEmpty();
    }

    private Runnable onLeadershipGained(QueueSweeper sweeper) {
        ControlPlaneProperties.Capacity 설정 = ControlPlaneProperties.defaults().capacity();
        CapacityCollector collector = CapacityCollector.of(설정.rampUp(), 설정.freshness(),
                설정.floor(), 설정.perInstanceCap());
        return ControlPlaneConfig.onLeadershipGained(collector,
                CapacityRefresh.of(Mono::empty, collector, () -> 1, Duration.ofSeconds(1),
                        Schedulers.immediate(), new SimpleMeterRegistry()),
                SoldOutCleanup.of(1, new SimpleMeterRegistry()),
                sweeper);
    }
}

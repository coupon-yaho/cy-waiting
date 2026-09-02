package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.coupon.QueueMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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

        onLeadershipGained(sweeper, 이월을_기록하는_회차(new ArrayList<>(),
                new AtomicReference<>(0.0))).run();

        assertThat(gate.removalHeld()).isTrue();
        assertThat(gate.sweepable(줄이_선_쿠폰, false)).isEmpty();
    }

    /**
     * <b>리더가 되면 평활화 이월을 다시 받는다</b> (F9 · CY-859).
     *
     * <p>배분 회차는 리더일 때만 돌므로 그 안에서 버리려 하면 비리더 구간을 한
     * 번도 못 본다. 되찾은 노드가 남이 움직인 값을 못 보고 옛 값을 이어 쓴다.
     */
    @Test
    @DisplayName("리더가_되면_평활화_이월을_다시_받는다")
    void 리더가_되면_평활화_이월을_다시_받는다() {
        List<Double> 이월_요청 = new ArrayList<>();
        AtomicReference<Double> 저장된_이월 = new AtomicReference<>(7_300.0);
        AllocationRound round = 이월을_기록하는_회차(이월_요청, 저장된_이월);
        round.run().block();
        assertThat(이월_요청).as("전제 — 첫 회차에 한 번 받는다").containsExactly(7_300.0);

        저장된_이월.set(800.0);
        onLeadershipGained(안_걷는_스위퍼(), round).run();
        round.run().block();

        assertThat(이월_요청).as("되찾은 뒤에는 그때의 값에서 이어 간다")
                .containsExactly(7_300.0, 800.0);
    }

    /** 재료를 읽은 시각. 회차가 나이를 이 값으로 매긴다. */
    private static final long 읽은_시각 = 1_700_000_000L;

    private QueueSweeper 안_걷는_스위퍼() {
        return QueueSweeper.of(
                SweepGates.warmed(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                (ids, limit, removeFront) -> Mono.just(QueueSweeper.SweepResult.NOTHING),
                new SimpleMeterRegistry());
    }

    private AllocationRound 이월을_기록하는_회차(List<Double> 이월_요청,
            AtomicReference<Double> 저장된_이월) {
        return AllocationRound.of(
                () -> true,
                () -> Mono.just(new TimedDemands(List.of(
                        new CouponDemand("c1", 5, 100, QueueMode.ADAPTIVE)), 읽은_시각)),
                () -> 1_000, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> Mono.empty(),
                () -> Instant.ofEpochSecond(읽은_시각),
                () -> {
                    이월_요청.add(저장된_이월.get());
                    return Mono.just(CreditSmoother.restore(0.3,
                            new CreditSmoother.Snapshot(저장된_이월.get(), true)));
                },
                SnapshotCodec.create(), () -> 0L);
    }

    private Runnable onLeadershipGained(QueueSweeper sweeper, AllocationRound round) {
        ControlPlaneProperties.Capacity 설정 = ControlPlaneProperties.defaults().capacity();
        CapacityCollector collector = CapacityCollector.of(설정.rampUp(), 설정.freshness(),
                설정.floor(), 설정.perInstanceCap());
        return ControlPlaneConfig.onLeadershipGained(collector,
                CapacityRefresh.of(Mono::empty, collector, () -> 1, Duration.ofSeconds(1),
                        Schedulers.immediate(), new SimpleMeterRegistry()),
                SoldOutCleanup.of(1, new SimpleMeterRegistry()),
                sweeper, round);
    }
}

package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 제어 평면 게이트를 실행으로 판정한다.
 *
 * <p>계획서에 적힌 문장만으로는 "통과했다" 에 근거가 없다. 여기서는 여러 노드를
 * 세워 <b>실제로 그 성질이 성립하는지</b> 본다.
 *
 * <p>레디스가 필요한 게이트는 통합·카오스 계층의 다른 시험이 맡는다. 여기는
 * 노드 여럿의 상호작용이 필요한 것만 모은다.
 */
@Tag("chaos")
class ControlPlaneGateTest {

    private static final Duration TICK = Duration.ofSeconds(1);
    private static final Duration LEASE = Duration.ofSeconds(2);
    private static final Duration ATTEMPT = Duration.ofMillis(300);

    /** 락 하나를 나눠 쓰는 노드들. 실제 레디스 없이 소유권 규칙만 재현한다. */
    private static final class SharedLock {

        private final AtomicReference<String> owner = new AtomicReference<>();
        private final AtomicLong 만료 = new AtomicLong();
        private final AtomicLong 시계 = new AtomicLong();

        Mono<LeaderLock> acquire(String ownerId) {
            return Mono.fromSupplier(() -> {
                long now = 시계.get();
                if (만료.get() <= now) {
                    owner.set(null);
                }
                String 현재 = owner.get();
                if (현재 == null) {
                    owner.set(ownerId);
                    만료.set(now + LEASE.toNanos());
                    return LeaderLock.mine(ownerId, LEASE.toMillis());
                }
                if (현재.equals(ownerId)) {
                    만료.set(now + LEASE.toNanos());
                    return LeaderLock.mine(ownerId, LEASE.toMillis());
                }
                return LeaderLock.heldBy(현재, (만료.get() - now) / 1_000_000);
            });
        }

        Mono<Void> release(String ownerId) {
            return Mono.fromRunnable(() -> owner.compareAndSet(ownerId, null));
        }

        void 흘린다(Duration 만큼) {
            시계.addAndGet(만큼.toNanos());
        }

        void 죽인다(String ownerId) {
            // 곱게 안 내린다. 락은 남고 리스 만료로만 풀린다.
            owner.compareAndSet(ownerId, ownerId);
        }
    }

    private Leadership 노드(SharedLock 락, String id) {
        return Leadership.of(id, LEASE, ATTEMPT,
                () -> 락.acquire(id), () -> 락.release(id), 락.시계::get);
    }

    @Test
    @DisplayName("리더가_죽으면_세_틱_안에_승계된다")
    void 리더가_죽으면_세_틱_안에_승계된다() {
        // 리스가 두 틱이라 승계는 두 틱에 끝난다. 상한이 세 틱이므로 한 틱이
        // 여유다 — 상한에 딱 맞추면 측정 오차 한 번에 게이트가 깨진다.
        SharedLock 락 = new SharedLock();
        Leadership a = 노드(락, "node-a");
        Leadership b = 노드(락, "node-b");
        a.renew().block();
        b.renew().block();
        assertThat(a.isLeader()).isTrue();
        assertThat(b.isLeader()).isFalse();

        락.죽인다("node-a");

        int 승계까지 = 0;
        for (int tick = 1; tick <= 3; tick++) {
            락.흘린다(TICK);
            b.renew().block();
            if (b.isLeader()) {
                승계까지 = tick;
                break;
            }
        }

        assertThat(승계까지).isBetween(1, 3);
        // 죽은 노드는 자기 리스가 지나면 스스로 내려온다.
        assertThat(a.isLeader()).isFalse();
    }

    @Test
    @DisplayName("배분은_정확히_한_대만_돈다")
    void 배분은_정확히_한_대만_돈다() {
        // 둘이 동시에 돌면 총합이 전역 크레딧을 넘는다. 이미 나간 통과는 못 물린다.
        SharedLock 락 = new SharedLock();
        List<Leadership> 노드들 = List.of(노드(락, "n1"), 노드(락, "n2"), 노드(락, "n3"),
                노드(락, "n4"), 노드(락, "n5"));

        for (int tick = 0; tick < 5; tick++) {
            노드들.forEach(node -> node.renew().block());
            long 리더 = 노드들.stream().filter(Leadership::isLeader).count();
            assertThat(리더).as("%d 틱째", tick).isEqualTo(1);
            락.흘린다(TICK);
        }
    }

    @Test
    @DisplayName("노드가_늘어도_총합이_전역_크레딧을_안_넘는다")
    void 노드가_늘어도_총합이_전역_크레딧을_안_넘는다() {
        // **분모가 늦게 늘면 각 노드가 큰 몫을 쓴다.** 노드 수는 즉시 올리고
        // 내릴 때만 미룬다 — 과다 배분은 사고고 과소 배분은 지연일 뿐이다.
        GatewayRegistry registry = GatewayRegistry.of(3, 1);
        long 전역_크레딧 = 100;

        for (int 노드_수 : new int[] {1, 2, 5, 10}) {
            registry.observed(노드_수);
            long 노드당 = 전역_크레딧 / registry.count();

            assertThat(노드당 * registry.count())
                    .as("노드 %d 대", 노드_수)
                    .isLessThanOrEqualTo(전역_크레딧);
            assertThat(registry.count()).isEqualTo(노드_수);
        }
    }

    @Test
    @DisplayName("리더가_바뀌어도_평활화가_이어진다")
    void 리더가_바뀌어도_평활화가_이어진다() {
        // 이월이 없으면 새 리더의 첫 관측이 그대로 초기값이 된다. 하필 회복
        // 직후가 진동하기 가장 쉬운 구간이라, 가장 나쁠 때 흔들린다.
        SnapshotCodec codec = SnapshotCodec.create();
        CreditSmoother 앞선_리더 = CreditSmoother.of(0.3);
        앞선_리더.observe(100);
        앞선_리더.observe(40);

        Map<String, String> 실린_것 = codec.encode(
                new GatewaySnapshot(Map.of(),
                        GatewaySnapshot.EMPTY.meta(),
                        Instant.ofEpochSecond(1_700_000_000L)),
                앞선_리더.snapshot());

        CreditSmoother 새_리더 = CreditSmoother.restore(0.3, codec.smoothing(실린_것));
        CreditSmoother 이월_없는_리더 = CreditSmoother.of(0.3);

        double 이어받은_값 = 새_리더.observe(40);
        double 처음부터_값 = 이월_없는_리더.observe(40);

        assertThat(이어받은_값).isEqualTo(앞선_리더.observe(40));
        assertThat(이어받은_값).isNotEqualTo(처음부터_값);
    }

    @Test
    @DisplayName("틱_지연이_임계_안에_있다")
    void 틱_지연이_임계_안에_있다() {
        // 이 값이 임계에 붙으면 스케줄러를 따로 떼야 한다는 신호다. 안 재면
        // 떼야 할 시점을 사고로 알게 된다.
        Duration 임계 = Duration.ofMillis(100);
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        List<Long> 잰_지연 = new CopyOnWriteArrayList<>();
        AtomicInteger 쿠폰_수 = new AtomicInteger(2_000);

        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(수요(쿠폰_수.get())),
                () -> 10_000L, () -> 10,
                grant -> Mono.just(grant.credit()),
                hash -> Mono.empty(),
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(CreditSmoother.of(0.3)),
                SnapshotCodec.create());
        AllocationScheduler scheduler = AllocationScheduler.of(TICK, Duration.ZERO,
                () -> true, round::run, 잰_지연::add, timer);
        scheduler.start();

        timer.advanceTimeBy(TICK.multipliedBy(3));
        scheduler.stop(() -> { });

        assertThat(잰_지연).isNotEmpty()
                .allSatisfy(잰 -> assertThat(Duration.ofNanos(잰)).isLessThan(임계));
    }

    private List<CouponDemand> 수요(int 개수) {
        List<CouponDemand> 수요 = new ArrayList<>(개수);
        for (int i = 0; i < 개수; i++) {
            수요.add(new CouponDemand("c" + i, 50, 1_000));
        }
        return 수요;
    }
}

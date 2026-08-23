package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import com.kafkick.waiting.domain.allocation.FairShareAllocator;
import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
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

    /** 통합 시험으로 만들 수 있는 규모. 계획서가 정한 값이다. */
    private static final int 쿠폰_수 = 2_000;

    /** 락 하나를 나눠 쓰는 노드들. 실제 레디스 없이 소유권 규칙만 재현한다. */
    private static final class SharedLock {

        private final AtomicReference<String> owner = new AtomicReference<>();
        private final Set<String> 죽은 = ConcurrentHashMap.newKeySet();
        private final AtomicLong 만료 = new AtomicLong();
        private final AtomicLong 시계 = new AtomicLong();

        Mono<LeaderLock> acquire(String ownerId) {
            if (죽은.contains(ownerId)) {
                // 죽은 프로세스는 응답이 없다. 오류도 아니다 — 그냥 안 온다.
                return Mono.never();
            }
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

        /**
         * 프로세스가 죽는다. 곱게 안 내리므로 락은 남고 리스 만료로만 풀린다.
         *
         * <p><b>실제로 못 부르게 만든다.</b> 호출부가 안 부르는 것에 기대면,
         * 누가 전 노드를 한 번에 도는 형태로 고칠 때 죽음이 조용히 사라진다.
         */
        void 죽인다(String ownerId) {
            죽은.add(ownerId);
        }

        boolean 죽었나(String ownerId) {
            return 죽은.contains(ownerId);
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

        // **하네스가 정말 죽였는지 본다.** 조작이 무동작이면 아래가 다 통과해도
        // 아무것도 안 잰 것이다 — 호출부가 안 부르는 것에 기댄 셈이 된다.
        a.renew().block(Duration.ofSeconds(1));
        assertThat(락.죽었나("node-a")).isTrue();

        int 승계까지 = 0;
        for (int tick = 1; tick <= 3; tick++) {
            락.흘린다(TICK);
            b.renew().block();
            if (b.isLeader()) {
                승계까지 = tick;
                break;
            }
        }

        // 리스가 두 틱이므로 승계는 두 틱에 끝난다. 상한은 세 틱이라 한 틱이
        // 여유다 — 상한에 딱 맞추면 측정 오차 한 번에 게이트가 깨진다.
        assertThat(승계까지).isEqualTo(2);
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
        //
        // 나눗셈만 재면 항진명제가 된다. **실제로 나눠 본 몫**을 합산해야
        // 분모가 늦을 때 총합이 넘는 것을 잡는다.
        GatewayRegistry registry = GatewayRegistry.of(3, 1);
        FairShareAllocator allocator = FairShareAllocator.create();
        long 전역_크레딧 = 100;
        List<CouponDemand> 수요 = List.of(new CouponDemand("c1", 40, 1_000),
                new CouponDemand("c2", 40, 1_000), new CouponDemand("c3", 40, 1_000));

        for (int 노드_수 : new int[] {1, 2, 5, 10}) {
            registry.observed(노드_수);
            long 노드당 = 전역_크레딧 / registry.count();
            long 한_노드의_합 = allocator.allocate(노드당, 수요).stream()
                    .mapToLong(Grant::credit).sum();

            assertThat(한_노드의_합 * 노드_수)
                    .as("노드 %d 대가 함께 낸 몫", 노드_수)
                    .isLessThanOrEqualTo(전역_크레딧);
        }
    }

    @Test
    @DisplayName("노드가_줄어도_총합이_전역_크레딧을_안_넘는다")
    void 노드가_줄어도_총합이_전역_크레딧을_안_넘는다() {
        // 감소는 미룬다. 그동안 분모가 실제보다 커서 각 노드가 작은 몫을 쓴다 —
        // 지연이지 사고가 아니다. 반대로 즉시 줄이면 그 순간 총합이 넘는다.
        GatewayRegistry registry = GatewayRegistry.of(3, 1);
        FairShareAllocator allocator = FairShareAllocator.create();
        long 전역_크레딧 = 100;
        List<CouponDemand> 수요 = List.of(new CouponDemand("c1", 40, 1_000));
        registry.observed(10);

        for (int 관측 = 1; 관측 <= 4; 관측++) {
            registry.observed(2);
            long 노드당 = 전역_크레딧 / registry.count();
            long 한_노드의_합 = allocator.allocate(노드당, 수요).stream()
                    .mapToLong(Grant::credit).sum();

            assertThat(한_노드의_합 * 2)
                    .as("%d 번째 감소 관측", 관측)
                    .isLessThanOrEqualTo(전역_크레딧);
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
    @DisplayName("리더가_바뀌어도_히스테리시스가_이어진다")
    void 리더가_바뀌어도_히스테리시스가_이어진다() {
        // **평활화만 이월하면 반쪽이다.** 붙잡고 있던 대기열이 교체마다 한 틱
        // 꺼졌다 켜지면, 사람에게는 "대기 없음 → 500명" 이 반복해 보인다 —
        // 히스테리시스가 막으려던 진동이 교체 때마다 나는 셈이다.
        SnapshotCodec codec = SnapshotCodec.create();
        // 최소 유지 틱을 다 쓰기 전에 넘겨야 갈린다. 다 쓴 뒤에 넘기면 이월
        // 여부와 무관하게 둘 다 놓아 버려 아무것도 못 잰다.
        QueueingHysteresis 앞선_리더 = QueueingHysteresis.of(0.8, 0.5, 3);
        앞선_리더.shouldQueue(90, 100);
        assertThat(앞선_리더.shouldQueue(40, 100)).isTrue();

        Map<String, String> 실린_것 = codec.encode(
                new GatewaySnapshot(Map.of(), GatewaySnapshot.EMPTY.meta(),
                        Instant.ofEpochSecond(1_700_000_000L)),
                CreditSmoother.Snapshot.empty(), 앞선_리더.snapshot());

        QueueingHysteresis 새_리더 =
                QueueingHysteresis.restore(0.8, 0.5, 3, codec.hysteresis(실린_것));
        QueueingHysteresis 이월_없는_리더 = QueueingHysteresis.of(0.8, 0.5, 3);

        // 이월받았으면 최소 유지 틱이 남아 계속 붙잡는다.
        assertThat(새_리더.shouldQueue(40, 100)).isTrue();
        // 못 받았으면 그 자리에서 놓아 버린다 — 그게 진동이다.
        assertThat(이월_없는_리더.shouldQueue(40, 100)).isFalse();
    }

    @Test
    @DisplayName("틱_지연이_임계_안에_있다")
    void 틱_지연이_임계_안에_있다() {
        // 이 값이 임계에 붙으면 스케줄러를 따로 떼야 한다는 신호다.
        //
        // **가상 시계로는 못 잰다.** 판이 아무리 오래 걸려도 가상 시계는 안
        // 움직이므로 잰 값이 항상 0 이다 — 그러면 이 시험은 공전한다.
        //
        // RULE-EXCEPTION(TS-4): 실제 경과가 곧 이 게이트의 판정 대상이다.
        Duration 임계 = Duration.ofMillis(100);
        AllocationRound round = AllocationRound.of(
                () -> true,
                () -> Mono.just(수요(쿠폰_수)),
                () -> 10_000L, () -> 10,
                grant -> Mono.just(grant.credit()),
                hash -> Mono.empty(),
                () -> Instant.ofEpochSecond(1_700_000_000L),
                () -> Mono.just(CreditSmoother.of(0.3)),
                SnapshotCodec.create());

        long 시작 = System.nanoTime();
        round.run().block();
        Duration 한_판 = Duration.ofNanos(System.nanoTime() - 시작);

        assertThat(한_판)
                .as("쿠폰 %d 개를 엮는 한 판", 쿠폰_수)
                .isLessThan(임계);
    }

    private List<CouponDemand> 수요(int 개수) {
        List<CouponDemand> 수요 = new ArrayList<>(개수);
        for (int i = 0; i < 개수; i++) {
            수요.add(new CouponDemand("c" + i, 50, 1_000));
        }
        return 수요;
    }
}

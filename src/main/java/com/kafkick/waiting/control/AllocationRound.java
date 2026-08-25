package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.FairShareAllocator;
import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 한 판. 수요를 모아 크레딧을 나누고 적용한 뒤 발행한다.
 *
 * <p><b>대기 수를 한 번만 읽는다.</b> 크레딧을 산출한 뒤 다시 읽으면 그 사이에
 * 사람이 빠져 도메인이 막는 조합이 발행되고, 코덱이 그 쿠폰만 떨군다. 떨어진
 * 쿠폰은 판정에서 없는 쿠폰 — 즉 매진으로 보인다.
 */
public final class AllocationRound {

    private static final Logger log = LoggerFactory.getLogger(AllocationRound.class);

    /** 이월을 못 받았을 때의 계수. 이월받으면 그쪽 계수를 따른다. */
    private static final double DEFAULT_ALPHA = 0.3;

    private final BooleanSupplier stillLeader;
    private final Supplier<Mono<List<CouponDemand>>> demands;
    private final LongSupplier globalCredit;
    private final LongSupplier creditFloor;
    private final IntSupplier gatewayCount;
    private final Function<Grant, Mono<Long>> apply;
    private final Function<Map<String, String>, Mono<Void>> publish;
    private final Supplier<Instant> clock;
    private final SnapshotCodec codec;

    /**
     * 평활화 상태. <b>첫 판에서 이월받는다.</b>
     *
     * <p>빈을 만들 때 읽으면 레디스가 안 뜬 상태에서 앱이 통째로 안 뜬다.
     * 이월은 있으면 좋은 것이지 기동의 전제가 아니다.
     */
    private final AtomicReference<CreditSmoother> smoother = new AtomicReference<>();
    private final Supplier<Mono<CreditSmoother>> restore;
    private final FairShareAllocator allocator = FairShareAllocator.create();
    private final FailureWindow failures;

    private AllocationRound(BooleanSupplier stillLeader,
            Supplier<Mono<List<CouponDemand>>> demands, LongSupplier globalCredit,
            IntSupplier gatewayCount, Function<Grant, Mono<Long>> apply,
            Function<Map<String, String>, Mono<Void>> publish, Supplier<Instant> clock,
            Supplier<Mono<CreditSmoother>> restore, SnapshotCodec codec,
            LongSupplier creditFloor) {
        this.stillLeader = Objects.requireNonNull(stillLeader, "stillLeader 는 필수다");
        this.demands = Objects.requireNonNull(demands, "demands 는 필수다");
        this.globalCredit = Objects.requireNonNull(globalCredit, "globalCredit 은 필수다");
        this.creditFloor = Objects.requireNonNull(creditFloor, "creditFloor 는 필수다");
        this.gatewayCount = Objects.requireNonNull(gatewayCount, "gatewayCount 는 필수다");
        this.apply = Objects.requireNonNull(apply, "apply 는 필수다");
        this.publish = Objects.requireNonNull(publish, "publish 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.restore = Objects.requireNonNull(restore, "restore 는 필수다");
        this.codec = Objects.requireNonNull(codec, "codec 은 필수다");
        // **한 번만 읽는다.** 두 번 읽으면 그 사이에 초 경계를 넘을 수 있고,
        // 나노와 초가 다른 순간에서 와 합이 뒤로 간다 — 지속 시간이 음수가 된다.
        this.failures = FailureWindow.of(() -> {
            Instant at = clock.get();
            return at.getEpochSecond() * 1_000_000_000L + at.getNano();
        });
    }

    public static AllocationRound of(BooleanSupplier stillLeader,
            Supplier<Mono<List<CouponDemand>>> demands,
            LongSupplier globalCredit, IntSupplier gatewayCount, Function<Grant, Mono<Long>> apply,
            Function<Map<String, String>, Mono<Void>> publish, Supplier<Instant> clock,
            Supplier<Mono<CreditSmoother>> restore, SnapshotCodec codec,
            LongSupplier creditFloor) {
        return new AllocationRound(stillLeader, demands, globalCredit, gatewayCount, apply, publish,
                clock, restore, codec, creditFloor);
    }

    public Mono<Void> run() {
        return seeded().then(demands.get().flatMap(this::allocate));
    }

    /**
     * 이월은 <b>한 번만</b> 받는다. 매 판 받으면 방금 쓴 값을 되읽어 평활화가
     * 아무 일도 안 하게 된다. 못 받아도 판은 돈다.
     */
    private Mono<Void> seeded() {
        if (smoother.get() != null) {
            return Mono.empty();
        }
        return Mono.defer(restore)
                // **실패하면 다음 판에 다시 시도한다.** 리더가 되는 순간은 대개
                // 직전 리더가 죽은 직후라 레디스가 가장 흔들려 있을 때다. 여기서
                // 포기하면 이월 장치가 정확히 필요한 조건에서만 꺼진다.
                .doOnError(e -> log.warn("평활화 이월 실패 — 다음 판에 다시 받는다", e))
                .onErrorResume(e -> Mono.just(CreditSmoother.of(DEFAULT_ALPHA))
                        .doOnNext(fallback -> smoother.compareAndSet(null, fallback))
                        .then(Mono.empty()))
                .doOnNext(restored -> {
                    if (smoother.compareAndSet(null, restored)) {
                        log.info("평활화 이월 완료");
                    }
                })
                .then();
    }

    /**
     * <b>쓰기 직전에 다시 묻는다.</b> 판 시작에서만 보면 리스가 10ms 남은 상태로
     * 시작한 판이 한 틱을 꽉 채워 돌고, 그 사이 다음 리더가 자기 판을 돈다.
     *
     * <p>묻는 비용은 메모리 읽기 하나다 — 안 물어볼 이유가 없다.
     */
    private boolean lostLeadership() {
        if (stillLeader.getAsBoolean()) {
            return false;
        }
        log.warn("판 도중에 리더십을 잃었다 — 쓰지 않고 접는다");
        return true;
    }

    private Mono<Void> allocate(List<CouponDemand> collected) {
        CreditSmoother current = smoother.get();
        // **하한은 평활 뒤에 건다.** 하한은 관측이 아니라 정책이다. 평활을 거치면
        // 앞선 낮은 값에서 올라오는 데 열 틱이 넘고, 그동안 노드당 몫이 유휴 비율
        // 아래에 머물러 한산 통과 상한이 0 이다 — 하한을 둔 이유가 사라진다 (R1).
        long smoothed = Math.round(current.observe(Math.max(0, globalCredit.getAsLong())));
        long credit = Math.max(smoothed, Math.max(0, creditFloor.getAsLong()));
        Map<String, Long> granted = new LinkedHashMap<>();
        allocator.allocate(credit, collected).forEach(g -> granted.put(g.couponId(), g.credit()));

        if (lostLeadership()) {
            return Mono.empty();
        }
        AtomicBoolean anyFailed = new AtomicBoolean();
        return Flux.fromIterable(collected)
                .concatMap(demand -> applyOne(demand, granted, anyFailed))
                .reduce(0L, Long::sum)
                // **실제로 들어온 수는 나눠 준 수와 다르다.** 큐가 몫보다 짧으면
                // 남고, 적용이 실패하면 0 이다. 안 남기면 크레딧이 어디서 새는지
                // 사후에 못 가린다.
                .doOnNext(admitted -> {
                    // **판이 통째로 성공해야 걷힌 것이다.** 쿠폰 하나가 계속
                    // 실패하고 다른 쿠폰이 성공하는 동안 매 판 복귀를 찍으면,
                    // 실패도 복귀도 아닌 두 줄이 영원히 반복된다.
                    if (!anyFailed.get()) {
                        failures.exited().ifPresent(recovered ->
                                log.info("배분 적용 복귀 — {}초 만에, 그동안 {}건 실패",
                                        recovered.elapsedSeconds(), recovered.swallowed()));
                    }
                    // 세는 값이라 지표 자리다. 초당 한 줄이면 진단이 필요한
                    // 순간에 다른 로그가 여기 묻힌다.
                    log.debug("배분 한 판 — 크레딧 {}, 들인 인원 {}, 쿠폰 {}개",
                            credit, admitted, collected.size());
                })
                .then(Mono.defer(() -> lostLeadership()
                        ? Mono.<Void>empty()
                        // **히스테리시스는 아직 빈 값을 싣는다.** 제품이 아직
                        // 히스테리시스를 안 돌려서 실을 상태가 없다 (CY-324).
                        // 돌리기 시작하면 여기가 매 틱 이월을 지우는 자리가
                        // 되므로, 기본값에 숨기지 않고 눈에 보이게 둔다.
                        : publish.apply(codec.encode(snapshot(collected, granted, credit),
                                current.snapshot(),
                                QueueingHysteresis.Snapshot.empty()))));
    }

    /**
     * <b>적용이 실패해도 그 쿠폰을 빼지 않는다.</b> 빠지면 판정에서 없는 쿠폰이
     * 되어 매진으로 보이는데, 적용이 안 된 것과 매진은 전혀 다른 상태다.
     */
    private Mono<Long> applyOne(CouponDemand demand, Map<String, Long> granted,
            AtomicBoolean anyFailed) {
        long credit = granted.getOrDefault(demand.couponId(), 0L);
        if (credit <= 0) {
            return Mono.just(0L);
        }
        // **쓰기 직전에 다시 묻는다.** 쿠폰이 많으면 이 루프가 한 틱을 꽉 채우고,
        // 그 사이 다음 리더가 자기 판을 돈다.
        if (lostLeadership()) {
            granted.put(demand.couponId(), 0L);
            return Mono.just(0L);
        }
        return apply.apply(new Grant(demand.couponId(), credit))
                // 쿠폰마다 찍으면 단절 한 번에 쿠폰 수만큼 곱해진다.
                .doOnError(e -> {
                    anyFailed.set(true);
                    // 임계가 안 올라갔으니 몫도 0 으로 접는다. 안 그러면 노드들이
                    // 일어나지 않은 배수율로 대기 시간을 계산한다.
                    granted.put(demand.couponId(), 0L);
                    if (failures.entered()) {
                        log.warn("배분 적용 실패 — 임계는 그대로다, couponId={}",
                                demand.couponId(), e);
                    }
                })
                .onErrorReturn(0L);
    }

    /**
     * 런타임을 <b>발행하는 그 쌍에서</b> 유도한다. 못 박으면 다 뺄 수 있는 줄까지
     * 줄 서는 중이 되어 도메인이 막고, 그 쿠폰만 떨어져 매진으로 보인다.
     *
     * <p>재고가 소진됐으면 매진이다. 이 전이가 없으면 줄이 영영 안 빠진다.
     */
    private CouponState stateOf(CouponDemand demand, Map<String, Long> granted) {
        if (demand.stock() <= 0) {
            return demand.waiting() > 0
                    ? CouponState.closed(demand.waiting())
                    : CouponState.idle(0);
        }
        if (demand.waiting() <= 0) {
            return CouponState.idle(demand.stock());
        }
        // 적용이 실패한 쿠폰은 임계가 안 올라갔다. 의도한 몫을 그대로 실으면
        // 노드들이 일어나지 않은 배수율로 대기 시간을 계산한다.
        return CouponState.offWithQueue(granted.getOrDefault(demand.couponId(), 0L),
                demand.stock(), demand.waiting());
    }

    private GatewaySnapshot snapshot(List<CouponDemand> collected, Map<String, Long> granted,
            long credit) {
        Map<String, CouponState> coupons = new LinkedHashMap<>();
        collected.forEach(demand ->
                coupons.put(demand.couponId(), stateOf(demand, granted)));
        return new GatewaySnapshot(coupons,
                new SnapshotMeta(credit, gatewayCount.getAsInt()), clock.get());
    }
}

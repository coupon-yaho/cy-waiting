package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.FairShareAllocator;
import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.coupon.Tunables;
import com.kafkick.waiting.domain.queue.PollBudgetPlanner;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

    /** 매진 큐 정리 판단 (7.3). 지우는 것은 어댑터가 한다. */
    /** 이탈자 청소 (7.4). <b>멈추는 판단을 안에 들고 있다.</b> */
    private final QueueSweeper sweeper;

    /** 이 노드가 든 재료가 낡았는가. <b>값으로 받는다</b> — 상수로 두면 가드가 안 걸린다. */
    private final BooleanSupplier dataStale;

    private final SoldOutCleanup cleanup;

    /** 지울 쿠폰들을 넘긴다. 지운 키 수를 돌려준다. */
    private final Function<List<String>, Mono<List<String>>> dropQueues;

    private final BooleanSupplier stillLeader;
    private final Supplier<Mono<TimedDemands>> demands;
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
    /**
     * 지금 걸린 운영 값. <b>판 밖에서 읽은 것을 그대로 씁니다.</b>
     *
     * <p>판 안에서 읽으면 발행이 그 왕복에 매달립니다 — 레디스가 500ms 느려지는
     * 것만으로 틱 예산을 넘겨 스냅샷이 아예 안 나갑니다.
     */
    private final Supplier<Optional<Tunables>> tunables;

    private final FailureWindow failures;

    /** 초과 구간. 틱마다 찍으면 정작 조사가 필요한 순간에 묻힌다 (LG-2). */
    private final FailureWindow overshoot = FailureWindow.create();

    /** 폴링 예산을 넘긴 구간. 진입과 해제를 쌍으로 남긴다 (LG-2). */
    // **쌍이 끊기는 경우가 있다.** 초과 중 리더십을 잃으면 새 리더의 창은
    // 비어 있어 해제가 안 나온다 (CY-735). 창이 리더 메모리라 그렇다.
    private final FailureWindow pollOvershoot = FailureWindow.create();

    /**
     * 폴링 예산을 넘긴 틱 수.
     *
     * <p><b>마지막 배수를 게이지로 안 낸다.</b> 리더십을 잃는 순간 그 값이 굳고,
     * 15초 스크레이프가 짧은 초과 구간을 통째로 놓친다 — 누적이라야 남는다.
     */
    private final AtomicLong pollBudgetOvershootTicks = new AtomicLong();

    /** 뒷단이 받을 수 있다고 한 것보다 더 나눠 준 누적량. */
    private final AtomicLong budgetOvershoot = new AtomicLong();

    /** 예산보다 더 들여보낸 누적 인원. */
    private final AtomicLong enteredOvershoot = new AtomicLong();

    private AllocationRound(BooleanSupplier stillLeader,
            Supplier<Mono<TimedDemands>> demands, LongSupplier globalCredit,
            IntSupplier gatewayCount, Function<Grant, Mono<Long>> apply,
            Function<Map<String, String>, Mono<Void>> publish, Supplier<Instant> clock,
            Supplier<Mono<CreditSmoother>> restore, SnapshotCodec codec,
            LongSupplier creditFloor, Supplier<Optional<Tunables>> tunables,
            SoldOutCleanup cleanup, Function<List<String>, Mono<List<String>>> dropQueues,
            QueueSweeper sweeper, BooleanSupplier dataStale) {
        this.sweeper = Objects.requireNonNull(sweeper, "sweeper 는 필수다");
        this.dataStale = Objects.requireNonNull(dataStale, "dataStale 은 필수다");
        this.cleanup = Objects.requireNonNull(cleanup, "cleanup 은 필수다");
        this.dropQueues = Objects.requireNonNull(dropQueues, "dropQueues 는 필수다");
        this.tunables = Objects.requireNonNull(tunables, "tunables 는 필수다");
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
            Supplier<Mono<TimedDemands>> demands,
            LongSupplier globalCredit, IntSupplier gatewayCount, Function<Grant, Mono<Long>> apply,
            Function<Map<String, String>, Mono<Void>> publish, Supplier<Instant> clock,
            Supplier<Mono<CreditSmoother>> restore, SnapshotCodec codec,
            LongSupplier creditFloor, Supplier<Optional<Tunables>> tunables,
            SoldOutCleanup cleanup, Function<List<String>, Mono<List<String>>> dropQueues,
            QueueSweeper sweeper, BooleanSupplier dataStale) {
        return new AllocationRound(stillLeader, demands, globalCredit, gatewayCount, apply, publish,
                clock, restore, codec, creditFloor, tunables, cleanup, dropQueues, sweeper,
                dataStale);
    }

    /** 정리를 안 붙이는 자리. <b>아무것도 안 지운다</b> — 시험 편의다. */
    public static AllocationRound withoutCleanup(BooleanSupplier stillLeader,
            Supplier<Mono<TimedDemands>> demands,
            LongSupplier globalCredit, IntSupplier gatewayCount, Function<Grant, Mono<Long>> apply,
            Function<Map<String, String>, Mono<Void>> publish, Supplier<Instant> clock,
            Supplier<Mono<CreditSmoother>> restore, SnapshotCodec codec,
            LongSupplier creditFloor, Supplier<Optional<Tunables>> tunables) {
        return new AllocationRound(stillLeader, demands, globalCredit, gatewayCount, apply, publish,
                clock, restore, codec, creditFloor, tunables,
                SoldOutCleanup.of(Integer.MAX_VALUE, new SimpleMeterRegistry()),
                ids -> Mono.just(List.of()),
                QueueSweeper.of(SweepGate.of(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                        (ids, limit) -> Mono.just(QueueSweeper.SweepResult.NOTHING)),
                () -> false);
    }

    /** 튜너블을 안 읽던 자리. 늘 기본값으로 돈다. */
    public static AllocationRound of(BooleanSupplier stillLeader,
            Supplier<Mono<TimedDemands>> demands,
            LongSupplier globalCredit, IntSupplier gatewayCount, Function<Grant, Mono<Long>> apply,
            Function<Map<String, String>, Mono<Void>> publish, Supplier<Instant> clock,
            Supplier<Mono<CreditSmoother>> restore, SnapshotCodec codec,
            LongSupplier creditFloor) {
        return withoutCleanup(stillLeader, demands, globalCredit, gatewayCount, apply, publish,
                clock, restore, codec, creditFloor, Optional::empty);
    }

    public Mono<Void> run() {
        // **재료를 읽은 시각을 재료와 같이 받는다.** 판이 끝난 시각으로 찍으면
        // 나이가 판 지속 시간만큼 어리고, 리더 벽시계로 찍으면 노드마다 다르게
        // 낡는다 — 둘 다 낡음 판정을 흔든다.
        return seeded().then(demands.get()
                .flatMap(read -> allocate(read.demands(),
                        Instant.ofEpochSecond(read.readAt()))));
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

    private Mono<Void> allocate(List<CouponDemand> collected, Instant readAt) {
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
        watchBudget(credit);
        AtomicBoolean anyFailed = new AtomicBoolean();
        return Flux.fromIterable(collected)
                .concatMap(demand -> applyOne(demand, granted, anyFailed))
                .reduce(0L, Long::sum)
                // **실제로 들어온 수는 나눠 준 수와 다르다.** 큐가 몫보다 짧으면
                // 남고, 적용이 실패하면 0 이다. 안 남기면 크레딧이 어디서 새는지
                // 사후에 못 가린다.
                .doOnNext(admitted -> watchEntered(admitted, credit))
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
                        : publishRound(collected, granted, credit, readAt, current)
                        // **발행 뒤에 지운다** (7.3). 앞에 두면 방금 지운 큐가
                        // 이번 재료에는 아직 대기자로 실려, 그 판의 크레딧이
                        // 없는 줄에 나간다.
                        // **미룬다.** 인자로 부르면 자바가 먼저 평가해서, 셈과
                        // 표시와 로그가 발행이 구독되기도 전에 일어난다 —
                        // 발행이 터져도 지운 것으로 기록된다.
                        .then(Mono.defer(() -> cleanUp(collected, granted)))
                        // **정리 뒤에 쓴다.** 앞에 두면 곧 지울 줄을 훑느라
                        // 예산을 쓴다.
                        .then(Mono.defer(() -> sweepUp(collected, granted)))));
    }

    /**
     * 매진된 지 오래된 쿠폰의 줄을 지운다 (7.3).
     *
     * <p><b>정리 실패가 배분을 막지 않는다</b> (7.3.4). 다음 틱에 다시 온다.
     */
    private Mono<Void> cleanUp(List<CouponDemand> collected, Map<String, Long> granted) {
        List<String> due = cleanup.due(couponsOf(collected, granted));
        if (due.isEmpty()) {
            return Mono.empty();
        }
        // **쓰기 직전에 다시 묻는다.** 판 안에서 유일하게 되돌릴 수 없는
        // 쓰기라, 리더가 아닌 채로 내면 남의 줄을 지운다. 묻는 비용은
        // 메모리 읽기 하나다.
        if (lostLeadership()) {
            return Mono.empty();
        }
        // **몇 개인지만 남긴다.** 목록을 통째로 찍으면 대량 매진에서 한 줄에
        // 쿠폰 ID 가 수백 개 들어간다 (LG-3). 어느 쿠폰인지는 지운 뒤에 남긴다.
        log.info("매진 큐 정리 — 쿠폰 {}개를 지운다", due.size());
        return dropQueues.apply(due)
                .doOnNext(dropped -> {
                    // **지운 것만 표시한다.** 요청한 것 전부를 표시하면 실패한
                    // 쿠폰이 다음 틱에 다시 안 오고, 지표는 지웠다고 말한다.
                    cleanup.dropped(dropped);
                    cleanup.failed(due.stream().filter(id -> !dropped.contains(id)).toList());
                    if (!dropped.isEmpty()) {
                        log.info("매진 큐 정리 끝 — 쿠폰 {}개: {}", dropped.size(), dropped);
                    }
                })
                .doOnError(e -> cleanup.failed(due))
                .onErrorResume(e -> {
                    log.warn("매진 큐 정리 실패 — 다음 틱에 다시 한다: {}", e.toString());
                    return Mono.just(List.<String>of());
                })
                .then();
    }

    /** 이탈자를 걷어 낸다 (7.4). 멈춰야 할 구간은 스위퍼가 안다. */
    private Mono<Void> sweepUp(List<CouponDemand> collected, Map<String, Long> granted) {
        if (lostLeadership()) {
            return Mono.empty();
        }
        return sweeper.run(couponsOf(collected, granted),
                // **리더가 신선한 것과 노드들이 신선한 것은 다른 이야기다.**
                // 생존 신호를 갱신하는 것은 노드 쪽 폴링이라, 그쪽이 멎어도
                // 리더의 수요 읽기는 성공할 수 있다. 이 노드도 게이트웨이이므로
                // 자기가 든 재료의 나이가 그 신호에 가장 가깝다.
                dataStale.getAsBoolean()).then();
    }

    /**
     * 나눠 준 예산이 <b>뒷단이 받을 수 있다고 한 것</b>을 넘었는가 (6.9.1).
     *
     * <p>배분기는 준 예산을 안 넘긴다 — 그걸 다시 재면 항등식이다. 넘는 자리는
     * 평활 지연과 하한이다.
     */
    // 뒷단이 1,000 으로 떨어졌다고 보고해도 평활은 열 틱 넘게 7,300 을 나눠 준다.
    // 그 구간이 초과 발급 직전 상태이고, 이 값이 그것을 센다.
    private void watchBudget(long credit) {
        long observed = Math.max(0, globalCredit.getAsLong());
        long over = credit - observed;
        if (over <= 0) {
            overshoot.exited().ifPresent(r -> log.info(
                    "배분 예산 초과 해제 — {}초 동안 {}틱", r.elapsedSeconds(), r.swallowed()));
            return;
        }
        budgetOvershoot.addAndGet(over);
        if (overshoot.entered()) {
            log.warn("뒷단이 받는다는 것보다 많이 나눠 준다 — 관측 {}, 나눠 준 예산 {}. "
                    + "초과 발급의 선행 지표다", observed, credit);
        }
    }

    /**
     * 실제로 들여보낸 수가 예산을 넘었는가 (6.9.1).
     *
     * <p><b>나눠 준 수와 다르다.</b> 동점 score 가 있으면 임계 하나에 여럿이
     * 걸려, 준 몫보다 많이 들어간다 — 이건 항등식이 아니다.
     */
    private void watchEntered(long admitted, long credit) {
        long over = admitted - credit;
        if (over > 0) {
            enteredOvershoot.addAndGet(over);
            log.error("예산보다 많이 들여보냈다 — 예산 {}, 들인 인원 {}. "
                    + "초과 발급의 직접 증거다", credit, admitted);
        }
    }

    /** 뒷단이 받는다는 것보다 더 나눠 준 누적량. 지표가 이 값을 읽는다. */
    public double budgetOvershoot() {
        return budgetOvershoot.get();
    }

    /** 예산보다 더 들여보낸 누적 인원. */
    public double enteredOvershoot() {
        return enteredOvershoot.get();
    }

    /** 폴링 예산을 넘긴 누적 틱 수. 0 이면 배수가 한 번도 안 걸렸다. */
    public double pollBudgetOvershootTicks() {
        return pollBudgetOvershootTicks.get();
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
                    ? CouponState.closed(demand.mode(), demand.waiting())
                    : CouponState.noQueue(demand.mode(), 0);
        }
        // **운영자가 정한 모드를 그대로 싣는다.** 여기서 바꿔 실으면 그 설정이
        // 한 틱을 못 넘긴다 — 판정 사다리에 분기가 있어도 발행자가 그 입력을
        // 못 만들면 없는 것과 같다.
        if (demand.waiting() <= 0) {
            return CouponState.noQueue(demand.mode(), demand.stock());
        }
        // 적용이 실패한 쿠폰은 임계가 안 올라갔다. 의도한 몫을 그대로 실으면
        // 노드들이 일어나지 않은 배수율로 대기 시간을 계산한다.
        return CouponState.withQueue(demand.mode(),
                granted.getOrDefault(demand.couponId(), 0L),
                demand.stock(), demand.waiting());
    }

    /**
     * 쿠폰별 상태만. <b>정리와 청소는 이것만 쓴다.</b>
     *
     * <p>스냅샷을 통째로 만들면 배수 계산과 그 계측이 한 판에 세 번 돈다 —
     * 누적 틱이 3배로 오르고 해제 로그의 지속 시간도 3배가 된다.
     */
    private Map<String, CouponState> couponsOf(
            List<CouponDemand> collected, Map<String, Long> granted) {
        Map<String, CouponState> coupons = new LinkedHashMap<>();
        collected.forEach(demand -> coupons.put(demand.couponId(), stateOf(demand, granted)));
        return coupons;
    }

    /** 발행할 재료 한 판. <b>순수하다</b> — 몇 번을 만들어도 세는 값이 안 는다. */
    private GatewaySnapshot snapshot(List<CouponDemand> collected, Map<String, Long> granted,
            long credit, Instant readAt, Tunables applied, double pollScale) {
        return new GatewaySnapshot(couponsOf(collected, granted),
                meta(credit, applied).withPollScale(pollScale), readAt);
    }

    // **노드 수 방어를 여기서 다시 쓰지 않는다.** 사본이 생기면 둘 중 하나만
    // 시험이 붙고, 하트비트가 다 만료돼 0 으로 보이는 순간 — 즉 클러스터가
    // 흔들리는 바로 그 순간 — 예산이 0 이 되어 배수가 통째로 꺼진다.
    private SnapshotMeta meta(long credit, Tunables applied) {
        return SnapshotMeta.withoutPollScale(credit, gatewayCount.getAsInt(), applied);
    }

    /**
     * 배수를 실어 발행한다.
     *
     * <p><b>센 것은 발행이 끝난 뒤에 남긴다.</b> 인자로 부르면 자바가 먼저
     * 평가해서, 발행이 터져도 배수를 걸었다고 기록한다 — 스냅샷 샤드만 죽은
     * 구간이 정확히 그렇다. 그때 전 노드는 옛 재료로 배수 1.0 을 쓰고 있다.
     */
    private Mono<Void> publishRound(List<CouponDemand> collected, Map<String, Long> granted,
            long credit, Instant readAt, CreditSmoother current) {
        PollBudget budget = pollBudget(collected, granted, credit);
        // **히스테리시스는 아직 빈 값을 싣는다.** 제품이 아직 히스테리시스를
        // 안 돌려서 실을 상태가 없다 (CY-324). 돌리기 시작하면 여기가 매 틱
        // 이월을 지우는 자리가 되므로, 기본값에 숨기지 않고 눈에 보이게 둔다.
        return publish.apply(codec.encode(
                        snapshot(collected, granted, credit, readAt,
                                tunables.get().orElse(null), budget.scale()),
                        current.snapshot(), QueueingHysteresis.Snapshot.empty()))
                .doOnSuccess(done -> watchPollBudget(budget));
    }

    /** 이번 틱의 폴링 예산과 그 결과. <b>순수하다</b> — 세는 것은 발행 뒤다. */
    private record PollBudget(double expected, double budget, double scale, int nodes) {
    }

    /**
     * 이번 틱의 전역 폴링 배수.
     *
     * <p>예산은 도메인이 소유한다 — 밴드도 하한도 거기 있는데 예산만 제어
     * 평면에 두면, 운영자가 실제로 만질 유일한 숫자만 시험이 안 닿는 곳에 남는다.
     */
    private PollBudget pollBudget(List<CouponDemand> collected, Map<String, Long> granted,
            long credit) {
        int nodes = meta(credit, null).effectiveGatewayCount();
        double expected = PollBudgetPlanner.expectedPollRps(collected,
                couponId -> granted.getOrDefault(couponId, 0L));
        double budget = PollBudgetPlanner.budgetRps(nodes);
        return new PollBudget(expected, budget,
                PollBudgetPlanner.pollScale(expected, budget), nodes);
    }

    /**
     * 배수가 1 을 넘는 것은 상태 전이다.
     *
     * <p>전 대기자의 다음 폴링이 한꺼번에 늘어나는데, 남기지 않으면 운영자
     * 눈에는 원인 없이 폴링이 뜸해진 것으로만 보인다.
     */
    private void watchPollBudget(PollBudget round) {
        double scale = round.scale();
        if (scale <= 1.0) {
            pollOvershoot.exited().ifPresent(r -> log.info(
                    "폴링 예산 초과 해제 — {}초 동안 {}틱", r.elapsedSeconds(), r.swallowed()));
            return;
        }
        pollBudgetOvershootTicks.incrementAndGet();
        if (pollOvershoot.entered()) {
            log.warn("폴링 예산 초과 — 예상 {}rps, 노드 {}대 예산 {}rps, 배수 {}. "
                    + "죽은 큐 정리와 노드 증설을 검토하세요",
                    Math.round(round.expected()), round.nodes(), Math.round(round.budget()),
                    // 같은 줄의 형제 값이 다 정수다. 여기만 17.583333333333332 가
                    // 나오면 읽는 사람이 그 자릿수에 의미가 있다고 읽는다.
                    Math.round(scale * 10) / 10.0);
        }
    }

}

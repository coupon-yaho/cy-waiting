package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.admission.CircuitState;
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
import java.util.concurrent.atomic.AtomicInteger;
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

    /** 뒷단 서킷. <b>판마다 한 번 읽는다</b> — 두 번 읽으면 한 판이 자기모순이 된다. */
    private final Supplier<CircuitState> circuit;

    private final SoldOutCleanup cleanup;

    /** 지울 쿠폰들을 넘긴다. 지운 키 수를 돌려준다. */
    private final Function<List<String>, Mono<List<String>>> dropQueues;

    /** 세기 시작한 줄에 울타리 표만 세운다. <b>선 것을 돌려준다</b> (CY-766). */
    private final Function<List<String>, Mono<List<String>>> claimQueues;

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
     * 평활화 상태. <b>리더가 된 뒤 첫 판에서 이월받는다.</b>
     *
     * <p>빈을 만들 때 읽으면 레디스가 안 뜬 상태에서 앱이 통째로 안 뜬다.
     * 이월은 있으면 좋은 것이지 기동의 전제가 아니다.
     */
    private final AtomicReference<CreditSmoother> smoother = new AtomicReference<>();

    /** 이월을 이어서 몇 판 못 받았나. 임기가 바뀌면 0 부터 다시 센다. */
    private final AtomicInteger carryoverMisses = new AtomicInteger();

    /** 이만큼 이어서 못 받으면 한 번 경고한다. 판마다 찍으면 로그가 뒤덮인다. */
    private static final int CARRYOVER_WARN_AFTER = 3;

    /** 못 받다가 받았으면 그 사실을 한 번 남긴다. 억제한 판 수를 같이 싣는다. */
    private void carryoverReturned() {
        int missed = carryoverMisses.getAndSet(0);
        if (missed > 0) {
            log.info("평활화 이월이 돌아왔다 — {}판 못 받았다", missed);
        }
    }
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

    /** 서킷 때문에 배분을 조인 구간. <b>진입과 해제를 쌍으로 남긴다</b> (LG-2). */
    private final FailureWindow paused = FailureWindow.create();

    /** 예산보다 더 들여보낸 누적 인원. */
    private final AtomicLong enteredOvershoot = new AtomicLong();

    /**
     * 차례를 준 누적 인원.
     *
     * <p><b>크레딧 낭비의 분모다</b> (G7.5). 실제로 받아 간 인원은 판정 지표가
     * 세는데 이 값이 없으면 그 비율을 못 낸다.
     */
    // **응답을 잃으면 적게 센다.** 스크립트가 임계를 올린 뒤 응답이 유실되면
    // 적용이 0 을 돌려주고, 다시 불러도 임계가 이미 올라가 있어 0 이다. 그
    // 판의 입장은 영영 이 값에 안 들어온다.
    //
    // 되찾는 길은 임계의 증분을 레디스에서 직접 읽는 것인데, 그러면 이 값이
    // 리더 메모리가 아니라 왕복이 된다. 대신 **틀리는 방향이 안전하다** —
    // 분모가 작아지면 낭비율이 커져 게이트가 통과가 아니라 미달 쪽으로 기운다.
    private final AtomicLong admitted = new AtomicLong();

    /**
     * 재고를 못 읽은 채 발행한 누적 쿠폰·틱. <b>0 이 아니면 재고 키를 잃었다.</b>
     *
     * <p>안 세면 그 쿠폰이 매진 판정을 아슬아슬하게 비켜 가는 것을 아무도 모른다.
     */
    private final AtomicLong stockUnknownTicks = new AtomicLong();

    private AllocationRound(BooleanSupplier stillLeader,
            Supplier<Mono<TimedDemands>> demands, LongSupplier globalCredit,
            IntSupplier gatewayCount, Function<Grant, Mono<Long>> apply,
            Function<Map<String, String>, Mono<Void>> publish, Supplier<Instant> clock,
            Supplier<Mono<CreditSmoother>> restore, SnapshotCodec codec,
            LongSupplier creditFloor, Supplier<Optional<Tunables>> tunables,
            SoldOutCleanup cleanup, Function<List<String>, Mono<List<String>>> dropQueues,
            Function<List<String>, Mono<List<String>>> claimQueues,
            QueueSweeper sweeper, BooleanSupplier dataStale, Supplier<CircuitState> circuit) {
        this.circuit = Objects.requireNonNull(circuit, "circuit 은 필수다");
        this.sweeper = Objects.requireNonNull(sweeper, "sweeper 는 필수다");
        this.dataStale = Objects.requireNonNull(dataStale, "dataStale 은 필수다");
        this.cleanup = Objects.requireNonNull(cleanup, "cleanup 은 필수다");
        this.dropQueues = Objects.requireNonNull(dropQueues, "dropQueues 는 필수다");
        this.claimQueues = Objects.requireNonNull(claimQueues, "claimQueues 는 필수다");
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
            Function<List<String>, Mono<List<String>>> claimQueues,
            QueueSweeper sweeper, BooleanSupplier dataStale, Supplier<CircuitState> circuit) {
        return new AllocationRound(stillLeader, demands, globalCredit, gatewayCount, apply, publish,
                clock, restore, codec, creditFloor, tunables, cleanup, dropQueues, claimQueues,
                sweeper, dataStale, circuit);
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
                ids -> Mono.just(List.of()),
                QueueSweeper.of(SweepGate.of(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                        (ids, limit, removeFront) -> Mono.just(QueueSweeper.SweepResult.NOTHING)),
                () -> false, () -> CircuitState.CLOSED);
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
     * 이월은 <b>임기마다 한 번</b> 받는다. 매 판 받으면 방금 쓴 값을 되읽어
     * 평활화가 아무 일도 안 하게 되고, 프로세스당 한 번만 받으면 남이 리더였던
     * 동안 움직인 값을 못 보고 제 옛 값을 이어 쓴다 (F9 · CY-859).
     */
    private Mono<Void> seeded() {
        if (smoother.get() != null) {
            return Mono.empty();
        }
        return Mono.defer(restore)
                // **실패하면 다음 판에 다시 시도한다.** 리더가 되는 순간은 대개
                // 직전 리더가 죽은 직후라 레디스가 가장 흔들려 있을 때다. 여기서
                // 포기하면 이월 장치가 정확히 필요한 조건에서만 꺼진다.
                // **판마다 안 찍는다.** 재시도로 바꾸면서 한 번뿐이던 로그가
                // 흔들림이 이어지는 내내 매 틱 나오게 됐다. 처음과, 한동안
                // 못 받았다는 사실 한 번만 남긴다.
                .doOnError(e -> {
                    if (carryoverMisses.get() == 0) {
                        log.warn("평활화 이월 실패 — 다음 판에 다시 받는다", e);
                    }
                })
                // **실패하자마자 포기하지 않는다.** 폴백을 그 자리에 설치하면
                // 다음 판이 이월을 아예 안 시도해, 주석이 약속한 재시도가 없다.
                // 게다가 그 폴백은 미관측이라 첫 관측치를 평활 없이 그대로
                // 발행한다 — 승계 직후에 뒷단이 감당 못 할 수가 나간다.
                //
                // 임기 내내 다시 시도한다. 그동안 판은 콜드 스무더로 돌되
                // 그것을 저장하지 않으므로, 흔들림이 지나가면 바로 이어받는다.
                .onErrorResume(e -> {
                    if (carryoverMisses.incrementAndGet() == CARRYOVER_WARN_AFTER) {
                        log.warn("평활화 이월을 {}판 못 받았다 — 그동안 콜드로 돈다",
                                CARRYOVER_WARN_AFTER);
                    }
                    return Mono.empty();
                })
                .doOnNext(restored -> carryoverReturned())
                .doOnNext(restored -> {
                    if (smoother.compareAndSet(null, restored)) {
                        log.info("평활화 이월 완료");
                    }
                })
                .then();
    }

    /**
     * 리더가 됐다. <b>평활화 이월을 버린다</b> — 다음 판이 그때의 스냅샷에서
     * 다시 받는다. 안 버리면 남이 리더였던 동안 움직인 값을 못 보고 제 옛 값을
     * 이어 쓴다 (F9 · CY-859). 판 도중에 잃어 발행 안 된 채 전진한 값도 여기서
     * 정리된다.
     */
    public void leadershipAcquired() {
        smoother.set(null);
        carryoverMisses.set(0);
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

    /**
     * 서킷이 열린 동안 배분을 조인다 (F3 · CY-787).
     *
     * <p>임계를 올리면 큐에서 빠져나온 사람이 토큰을 쥐고 503 을 받는다 —
     * 자리는 이미 없다. 반쯤 열렸을 때 <b>0 으로 막지는 않는다</b>: 뒷단에 닿는
     * 호출이 없으면 서킷이 표본을 못 채워 영영 안 닫힌다.
     */
    private long gated(long credit, CircuitState now) {
        if (now == CircuitState.CLOSED) {
            paused.exited().ifPresent(r -> log.info(
                    "서킷 회복 — {}초 동안 {}틱을 배분 없이 보냈다. 임계가 그만큼 안 올라갔다",
                    r.elapsedSeconds(), r.swallowed()));
            return credit;
        }
        if (paused.entered()) {
            // **진입을 남긴다** (LG-2). 안 남기면 배분이 왜 멎었는지 알 방법이
            // 서킷 로그뿐인데, 그건 리더가 아닌 노드에서 날 수도 있다.
            log.warn("서킷 때문에 배분을 조인다 — 상태 {}, 원래 몫 {}. "
                    + "임계를 올리면 큐에서 나온 사람이 503 을 받고 자리를 잃는다", now, credit);
        }
        // 노드당 초당 한 건. 서킷이 제 창을 채울 만큼이면서, 약한 뒷단이 그
        // 수만큼만 맞는다. 0 으로 막으면 표본이 없어 영영 안 닫힌다.
        return now == CircuitState.OPEN
                ? 0
                : Math.min(credit, Math.max(1, gatewayCount.getAsInt()));
    }

    private Mono<Void> allocate(List<CouponDemand> collected, Instant readAt) {
        // **이월을 아직 못 받았어도 판은 돈다.** 이월은 있으면 좋은 것이지
        // 배분의 전제가 아니다 — 여기서 멈추면 레디스가 흔들릴 때 배분이
        // 통째로 안 시작한다.
        //
        // 다만 그 스무더를 **저장하지는 않는다.** 저장하면 다음 판이 이월을
        // 아예 안 시도해, 흔들림이 지나가도 그 임기 내내 콜드로 남는다.
        CreditSmoother carried = smoother.get();
        CreditSmoother current = carried == null ? CreditSmoother.of(DEFAULT_ALPHA) : carried;
        // **하한은 평활 뒤에 건다.** 하한은 관측이 아니라 정책이다. 평활을 거치면
        // 앞선 낮은 값에서 올라오는 데 열 틱이 넘고, 그동안 노드당 몫이 유휴 비율
        // 아래에 머물러 한산 통과 상한이 0 이다 — 하한을 둔 이유가 사라진다 (R1).
        long observed = Math.max(0, globalCredit.getAsLong());
        long smoothed = Math.round(current.observe(observed));
        // **서킷은 평활과 하한 뒤에 건다.** 앞에 걸면 두 번 샌다 — 평활이 0 을
        // 천천히 내려 첫 판에 수천이 그대로 나가고, 하한이 그 뒤로도 계속
        // 임계를 올린다. 하한은 관측이 아니라 정책이라 평활 뒤인데, 서킷은
        // 관측이 아니라 **사실**이라 그보다도 뒤라야 한다.
        // **판마다 한 번 읽는다.** 두 번 읽으면 그 사이에 상태가 뒤집혀 같은
        // 판이 자기모순인 값 둘로 판단한다 — 5초 창에 1초 틱이면 실제로 걸린다.
        CircuitState circuitNow = circuit.get();
        long credit = gated(Math.max(smoothed, Math.max(0, creditFloor.getAsLong())), circuitNow);
        Map<String, Long> granted = new LinkedHashMap<>();
        allocator.allocate(credit, collected).forEach(g -> granted.put(g.couponId(), g.credit()));

        if (lostLeadership()) {
            return Mono.empty();
        }
        watchBudget(credit, observed);
        AtomicBoolean anyFailed = new AtomicBoolean();
        return Flux.fromIterable(collected)
                .concatMap(demand -> applyOne(demand, granted, anyFailed))
                .reduce(0L, Long::sum)
                // **실제로 들어온 수는 나눠 준 수와 다르다.** 큐가 몫보다 짧으면
                // 남고, 적용이 실패하면 0 이다. 안 남기면 크레딧이 어디서 새는지
                // 사후에 못 가린다.
                .doOnNext(entered -> {
                    // **나눠 준 몫이 아니라 실제로 들인 수를 센다.** 줄이 몫보다
                    // 짧으면 남고, 그 남은 몫은 차례를 준 것이 아니다 — 세면
                    // 낭비율의 분모가 부풀어 실제보다 좋아 보인다.
                    admitted.addAndGet(entered);
                    watchEntered(entered, credit);
                })
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
        List<String> claimed = cleanup.claimed();
        if (due.isEmpty() && claimed.isEmpty()) {
            return Mono.empty();
        }
        // **쓰기 직전에 다시 묻는다.** 판 안에서 유일하게 되돌릴 수 없는
        // 쓰기라, 리더가 아닌 채로 내면 남의 줄을 지운다. 묻는 비용은
        // 메모리 읽기 하나다.
        if (lostLeadership()) {
            return Mono.empty();
        }
        // **세기 시작한 줄에 먼저 표를 세운다** (CY-766). 표는 지웠을 때만
        // 생기므로 한 번도 안 지운 줄에는 표가 없고, 그건 울타리가 지키려던
        // 바로 그 경우다 — 얼었다 깨어난 옛 리더가 새 리더가 아직 지울 생각이
        // 없는 줄을 지운다.
        Mono<Void> claim = claimQueues.apply(claimed)
                // **선 것만 확인으로 친다.** 실패한 것을 확인으로 치면 그 줄은
                // 표 없이 유예를 보내고, 옛 판이 그대로 지운다.
                .doOnNext(cleanup::fenceConfirmed)
                .then();
        if (due.isEmpty()) {
            return claim;
        }
        // **몇 개인지만 남긴다.** 목록을 통째로 찍으면 대량 매진에서 한 줄에
        // 쿠폰 ID 가 수백 개 들어간다 (LG-3). 어느 쿠폰인지는 지운 뒤에 남긴다.
        log.info("매진 큐 정리 — 쿠폰 {}개를 지운다", due.size());
        return claim.then(dropQueues.apply(due)
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
                .then());
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
    // **관측치를 인자로 받는다.** 여기서 다시 읽으면 한 판이 두 값을 보게 되고,
    // 그 사이에 가용량이 바뀌면 나눠 준 몫과 비교 대상이 서로 다른 판의 것이 된다.
    private void watchBudget(long credit, long observed) {
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

    /** 차례를 준 누적 인원. 크레딧 낭비의 분모다 (G7.5). */
    public double admitted() {
        return admitted.get();
    }

    /** 재고를 못 읽은 채 발행한 누적 쿠폰·틱. 0 이 아니면 재고 키를 잃었다. */
    public double stockUnknownTicks() {
        return stockUnknownTicks.get();
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
        // **못 읽은 재고를 매진으로 안 접는다** (3.1). 접으면 그 쿠폰이 종결되고
        // 정리가 유예 틱을 채운 뒤 큐를 지운다 — 자동으로 안 낫는 오판이
        // 되돌릴 수 없는 삭제가 된다. 진짜 상한은 뒷단이 원자적으로 지킨다.
        if (!demand.stockKnown()) {
            return CouponState.stockUnknown(demand.mode(),
                    granted.getOrDefault(demand.couponId(), 0L), demand.waiting());
        }
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
        PollBudgetPlanner.Scale budget = pollBudget(collected, granted, credit);
        // **판마다 한 번 센다.** 상태를 만드는 자리에서 세면 정리·청소·발행이
        // 같은 판을 세 번 훑어 셋으로 부푼다.
        long unknown = collected.stream().filter(d -> !d.stockKnown()).count();
        // **히스테리시스는 아직 빈 값을 싣는다.** 제품이 아직 히스테리시스를
        // 안 돌려서 실을 상태가 없다 (CY-324). 돌리기 시작하면 여기가 매 틱
        // 이월을 지우는 자리가 되므로, 기본값에 숨기지 않고 눈에 보이게 둔다.
        return publish.apply(codec.encode(
                        snapshot(collected, granted, credit, readAt,
                                tunables.get().orElse(null), budget.scale()),
                        current.snapshot(), QueueingHysteresis.Snapshot.empty()))
                // **발행이 끝난 뒤에 센다.** 앞에서 세면 스냅샷 샤드가 죽어
                // 발행이 매 틱 터지는 구간 — 재고 키를 잃기 가장 쉬운 구간 —
                // 에서 발행 안 된 판이 발행된 것으로 잡힌다.
                .doOnSuccess(done -> {
                    watchPollBudget(budget);
                    stockUnknownTicks.addAndGet(unknown);
                });
    }

    /**
     * 이번 틱의 전역 폴링 배수.
     *
     * <p>예산은 도메인이 소유한다 — 밴드도 하한도 거기 있는데 예산만 제어
     * 평면에 두면, 운영자가 실제로 만질 유일한 숫자만 시험이 안 닿는 곳에 남는다.
     */
    private PollBudgetPlanner.Scale pollBudget(List<CouponDemand> collected,
            Map<String, Long> granted, long credit) {
        // **조립은 도메인이 쥔다.** 여기서 다시 조립하면 분모를 바꾸는 날 시나리오와
        // 갈라져, 낡은 값을 초록으로 단언하는 시험이 남는다.
        return PollBudgetPlanner.scaleFor(meta(credit, null), collected,
                couponId -> granted.getOrDefault(couponId, 0L));
    }

    /**
     * 배수가 1 을 넘는 것은 상태 전이다.
     *
     * <p>전 대기자의 다음 폴링이 한꺼번에 늘어나는데, 남기지 않으면 운영자
     * 눈에는 원인 없이 폴링이 뜸해진 것으로만 보인다.
     */
    private void watchPollBudget(PollBudgetPlanner.Scale round) {
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

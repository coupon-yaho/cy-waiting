package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.admission.CircuitState;
import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.FairShareAllocator;
import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.allocation.ReleaseRamp;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.routing.InstanceRouting;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.coupon.Tunables;
import com.kafkick.waiting.domain.queue.PollBudgetPlanner;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 한 회차. 수요를 모아 크레딧을 나누고 적용한 뒤 발행한다. <b>대기 수는 한 번만 읽는다</b>
 * — 다시 읽으면 그 사이에 사람이 빠져 도메인이 막는 조합이 나가고, 코덱이 떨군 그 쿠폰은
 * 판정에서 매진으로 보인다.
 */
public final class AllocationRound {

    private static final Logger log = LoggerFactory.getLogger(AllocationRound.class);

    /** 동시 적용 상한. 레디스 어댑터의 쓰기 상한과 같다. */
    private static final int MAX_CONCURRENT_APPLIES = 16;

    /** 이번 회차가 되감기를 쟀는가. 잰 회차는 목록을 안 버린다. */
    private final AtomicBoolean rewindMeasured = new AtomicBoolean();

    /**
     * 매진이 발행에 실려 나간 쿠폰. 발행이 못 나간 회차는 이 줄만 지운다 (CY-935).
     *
     * <p><b>동시성 집합이다</b> — 채우는 곳은 발행 응답(레디스 스레드)이고 비우는 곳은 승계 콜백이다.
     */
    private final Set<String> announced = ConcurrentHashMap.newKeySet();

    /** 이탈자 청소. <b>멈추는 판단을 안에 들고 있다.</b> */
    private final QueueSweeper sweeper;

    /** 이 노드가 든 재료가 낡았는가. <b>값으로 받는다</b> — 상수로 두면 가드가 안 걸린다. */
    private final BooleanSupplier dataStale;

    /** 뒷단 서킷. <b>회차마다 한 번 읽는다</b> — 두 번 읽으면 한 회차가 자기모순이 된다. */
    private final Supplier<CircuitState> circuit;

    /** 매진 큐 정리 판단. 지우는 것은 어댑터가 한다. */
    private final SoldOutCleanup cleanup;

    /** 지울 쿠폰들을 넘긴다. 지운 키 수를 돌려준다. */
    private final Function<List<String>, Mono<List<String>>> dropQueues;

    /** 세기 시작한 줄에 울타리 표만 세운다. <b>실제로 선 것만 돌려준다.</b> */
    private final Function<List<String>, Mono<List<String>>> claimQueues;

    private final BooleanSupplier stillLeader;
    private final Supplier<Mono<TimedDemands>> demands;
    /**
     * 라우팅에 쓸 뒷단 목록. <b>발행에 실어 전 노드에 보낸다</b> — 요청 경로에서는
     * 레디스를 치지 않으므로 이 길 말고는 닿을 방법이 없다.
     */
    private final Supplier<List<InstanceRouting>> routable;

    private final LongSupplier globalCredit;
    private final LongSupplier creditFloor;
    private final IntSupplier gatewayCount;

    private final Function<Grant, Mono<Long>> apply;
    private final Function<Map<String, String>, Mono<Void>> publish;
    private final Supplier<Instant> clock;
    private final SnapshotCodec codec;

    /**
     * 평활화 상태. <b>리더가 된 뒤 첫 회차에서 이월받는다.</b> 빈을 만들 때 읽으면
     * 레디스가 안 뜬 상태에서 앱이 통째로 안 뜬다 — 이월은 기동의 전제가 아니다.
     */
    private final AtomicReference<CreditSmoother> smoother = new AtomicReference<>();

    /** 이월을 못 받는 동안 이어 쓰는 평활. 이월이 오거나 임기가 바뀌면 버린다. */
    private final AtomicReference<CreditSmoother> interim = new AtomicReference<>();

    /** 임기마다 한 번 오르는 이월 결과. 받음·없음·대신함을 가르면 승계 뒤 계단의 원인이 읽힌다. */
    private final AtomicLong carryoverRestored = new AtomicLong();
    private final AtomicLong carryoverEmpty = new AtomicLong();
    private final AtomicLong carryoverReplaced = new AtomicLong();

    /** 이월 읽기가 실패한 시도 수. 임기 단위 결과와 세는 단위가 달라 따로 둔다. */
    private final AtomicLong carryoverFailures = new AtomicLong();

    /** 마지막 회차의 평활값. 지표 스레드가 읽는다. 임기가 바뀌면 비운다. */
    private volatile double smoothedCredit = Double.NaN;

    /**
     * 저장소가 뒤로 감긴 사실을 잡는 신호 (CY-856). <b>실패 뒤 첫 회차에만 센다</b> — 평시에 재면 쿠폰마다 왕복이
     * 늘고, 되감기는 재접속 직후에만 드러난다. 배선 전에는 안 잰다.
     */
    private volatile Function<List<String>, Mono<RewindCheck>> rewound;

    /** 활성에서 빠진 쿠폰의 기준을 버리는 자리. 측정이 밀린 동안에는 안 버린다. */
    private volatile Consumer<List<String>> forgetInactive = ids -> { };

    /** 직전 회차가 터졌는가. 터진 다음 회차가 신호를 재는 자리다. */
    private final AtomicBoolean roundFailed = new AtomicBoolean();

    /** 마지막으로 센 되감긴 쿠폰 수. */
    private volatile double rewoundCoupons = Double.NaN;

    /** 신호를 못 잰 누적 횟수. 0 이 아니면 그 장애의 되감기 여부를 모른다. */
    private final AtomicLong rewindUnmeasured = new AtomicLong();

    /** 되감기를 본 회차의 누적 수. */
    private final AtomicLong rewoundEvents = new AtomicLong();

    /** 견줄 기준이 없어 못 잰 누적 횟수. 읽기 실패와 뜻이 달라 따로 센다 — 이쪽은 다시 안 잰다. */
    private final AtomicLong rewindNoBaseline = new AtomicLong();

    /** 되감기 신호를 못 재는 구간. 회복 구간이 이 읽기가 가장 잘 실패하는 구간이다. */
    private final FailureWindow rewindFailures = FailureWindow.create();

    /**
     * 이 노드가 본 임기의 세대. <b>늦게 온 측정을 버리는 표다</b> — 읽기는 임기가 갈려도 안 끊기고, 그 결과가 새 임기의
     * 지표로 들어가면 지나간 사건이 지금 값처럼 보인다.
     */
    private final AtomicLong term = new AtomicLong();

    /** 로그에 싣는 쿠폰 수의 상한. 다 실으면 한 줄이 수천 자가 된다. */
    private static final int REWOUND_LOG_LIMIT = 10;

    /** 적용 간격. 배선 전에는 안 둔다. */
    private volatile ApplyPacer pacer = ApplyPacer.none();

    /**
     * 읽은 이월을 자리에 앉힌다. <b>값이 없으면 이 임기의 평활을 앉힌다</b> — 콜드를 앉히면
     * 데워진 임시 평활이 버려져 다음 관측이 다시 생으로 나간다.
     */
    private void carried(CreditSmoother restored) {
        CreditSmoother warm = interim.get();
        CreditSmoother chosen = restored.snapshot().seeded() || warm == null ? restored : warm;
        if (!smoother.compareAndSet(null, chosen)) {
            return;
        }
        interim.set(null);
        if (restored.snapshot().seeded()) {
            carryoverRestored.incrementAndGet();
            log.info("평활화 이월 완료 — {} 에서 잇는다", restored.snapshot().value());
        } else {
            carryoverEmpty.incrementAndGet();
            log.info("평활화 이월할 값이 없다 — {}", warm == null
                    ? "첫 관측에서 시작한다" : "이 임기의 평활로 잇는다");
        }
    }

    /**
     * 이월을 못 받은 채 발행이 됐다. <b>그 발행이 이월 자리를 덮었다</b> — 앞 리더의 값은
     * 이제 없고, 다시 읽으면 제 값을 앞 리더의 것으로 센다. 이 임기의 평활을 앉힌다.
     */
    private void replacedByPublish(CreditSmoother current) {
        if (current != interim.get() || !smoother.compareAndSet(null, current)) {
            return;
        }
        interim.set(null);
        carryoverReplaced.incrementAndGet();
        log.info("평활화 이월을 못 받은 채 발행이 자리를 덮었다 — {}회차 못 받았다. "
                + "이 임기의 평활로 잇는다", carryoverMisses.getAndSet(0));
    }

    /** 이월을 이어서 몇 회차 못 받았나. 임기가 바뀌면 0 부터 다시 센다. */
    private final AtomicInteger carryoverMisses = new AtomicInteger();

    /** 이만큼 이어서 못 받으면 한 번 경고한다. 회차마다 찍으면 로그가 뒤덮인다. */
    private static final int CARRYOVER_WARN_AFTER = 3;

    /** 못 받다가 받았으면 그 사실을 한 번 남긴다. 억제한 회차 수를 같이 싣는다. */
    private void carryoverReturned() {
        int missed = carryoverMisses.getAndSet(0);
        if (missed > 0) {
            log.info("평활화 이월이 돌아왔다 — {}회차 못 받았다", missed);
        }
    }
    private final Supplier<Mono<CreditSmoother>> restore;
    private final FairShareAllocator allocator = FairShareAllocator.create();
    /**
     * 지금 걸린 운영 값. <b>회차 밖에서 읽은 것을 그대로 쓴다.</b> 회차 안에서 읽으면
     * 레디스가 500ms 느려지는 것만으로 틱 예산을 넘겨 스냅샷이 아예 안 나간다.
     */
    private final Supplier<Optional<Tunables>> tunables;

    private final FailureWindow failures;

    /** 초과 구간. 틱마다 찍으면 정작 조사가 필요한 순간에 묻힌다. */
    private final FailureWindow overshoot = FailureWindow.create();

    /**
     * 폴링 예산을 넘긴 구간. 진입과 해제를 쌍으로 남긴다. 창이 리더 메모리라
     * <b>쌍이 끊길 수 있다</b> — 초과 중 리더십을 잃으면 새 리더의 창은 비어 있어 해제가
     * 안 나온다.
     */
    private final FailureWindow pollOvershoot = FailureWindow.create();

    /**
     * 폴링 예산을 넘긴 틱 수. <b>마지막 배수를 게이지로 안 낸다</b> — 리더십을 잃는 순간
     * 값이 굳고, 15초 스크레이프가 짧은 초과 구간을 통째로 놓친다.
     */
    private final AtomicLong pollBudgetOvershootTicks = new AtomicLong();

    /** 뒷단이 받을 수 있다고 한 것보다 더 나눠 준 누적량. */
    private final AtomicLong budgetOvershoot = new AtomicLong();

    /** 서킷 때문에 배분을 조인 구간. <b>진입과 해제를 쌍으로 남긴다.</b> */
    private final FailureWindow paused = FailureWindow.create();

    /**
     * 조임을 푸는 속도. <b>평활이 조여진 값을 못 보므로</b> 서킷이 닫히는 한
     * 틱에 배분이 원래 몫으로 그대로 돌아간다. 그 계단을 회차당 배수로 나눈다.
     */
    private final ReleaseRamp releaseRamp = ReleaseRamp.of(ReleaseRamp.DEFAULT_STEP);

    /** 램프가 걸린 구간. <b>진입과 해제를 쌍으로 남긴다.</b> */
    private final FailureWindow ramping = FailureWindow.create();

    /** 예산보다 더 들여보낸 누적 인원. */
    private final AtomicLong enteredOvershoot = new AtomicLong();

    /**
     * 차례를 준 누적 인원. <b>크레딧 낭비의 분모다.</b> 응답이 유실되면 적용이 0 을
     * 돌려줘 적게 세지만 <b>틀리는 방향이 안전하다</b> — 분모가 작으면 게이트가 미달로
     * 기운다.
     */
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
            QueueSweeper sweeper, BooleanSupplier dataStale, Supplier<CircuitState> circuit,
            Supplier<List<InstanceRouting>> routable) {
        this.routable = Objects.requireNonNull(routable, "routable 은 필수다");
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
        // 라우팅 목록을 안 싣던 자리. 발행은 그대로 돌고 목록만 빈다.
        return of(stillLeader, demands, globalCredit, gatewayCount, apply, publish,
                clock, restore, codec, creditFloor, tunables, cleanup, dropQueues, claimQueues,
                sweeper, dataStale, circuit, List::of);
    }

    /** 라우팅 목록을 함께 싣는 자리. */
    public static AllocationRound of(BooleanSupplier stillLeader,
            Supplier<Mono<TimedDemands>> demands,
            LongSupplier globalCredit, IntSupplier gatewayCount, Function<Grant, Mono<Long>> apply,
            Function<Map<String, String>, Mono<Void>> publish, Supplier<Instant> clock,
            Supplier<Mono<CreditSmoother>> restore, SnapshotCodec codec,
            LongSupplier creditFloor, Supplier<Optional<Tunables>> tunables,
            SoldOutCleanup cleanup, Function<List<String>, Mono<List<String>>> dropQueues,
            Function<List<String>, Mono<List<String>>> claimQueues,
            QueueSweeper sweeper, BooleanSupplier dataStale, Supplier<CircuitState> circuit,
            Supplier<List<InstanceRouting>> routable) {
        return new AllocationRound(stillLeader, demands, globalCredit, gatewayCount, apply, publish,
                clock, restore, codec, creditFloor, tunables, cleanup, dropQueues, claimQueues,
                sweeper, dataStale, circuit, routable);
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
                () -> false, () -> CircuitState.CLOSED, List::of);
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

    /** 되감기 신호를 잴 자리. 배선이 한 번 건다. */
    public void measuringRewindWith(Function<List<String>, Mono<RewindCheck>> rewound,
            Consumer<List<String>> forgetInactive) {
        this.rewound = Objects.requireNonNull(rewound, "rewound 는 필수다");
        this.forgetInactive = Objects.requireNonNull(forgetInactive, "forgetInactive 는 필수다");
    }

    /**
     * 실패 뒤 첫 회차에 센 되감긴 쿠폰 수. <b>리더가 아니면 NaN 이다</b> — 강등된 노드가 옛 값을 계속 내면 장애가
     * 끝난 뒤에도 대시보드에 그 값이 붙는다. 아직 안 쟀어도 NaN 이다.
     */
    public double rewoundCoupons() {
        return stillLeader.getAsBoolean() ? rewoundCoupons : Double.NaN;
    }

    /** 적용 간격을 둔다. 배선이 한 번 건다. */
    public void pacedBy(ApplyPacer pacer) {
        this.pacer = Objects.requireNonNull(pacer, "pacer 는 필수다");
    }

    public Mono<Void> run() {
        return run(Mono.empty());
    }

    /**
     * 운영값 읽기와 <b>동시에</b> 수요를 읽고, 두 읽기가 다 끝난 뒤에 나눈다. 앞에 차례로 두면 레디스가 느린 날 그
     * 왕복이 틱을 먹고, 나누기를 먼저 하면 방금 바꾼 운영값이 한 틱 늦게 나간다.
     */
    public Mono<Void> run(Mono<Void> reads) {
        // **재료를 읽은 시각을 재료와 같이 받는다.** 회차가 끝난 시각으로 찍으면
        // 나이가 회차 지속 시간만큼 어리고, 리더 벽시계로 찍으면 노드마다 다르게
        // 낡는다 — 둘 다 낡음 판정을 흔든다.
        return Mono.defer(() -> {
            pacer.roundStarted();
            Mono<TimedDemands> read = seeded().then(Mono.defer(demands)).cache();
            // 수요 읽기의 실패로 운영값 읽기를 취소하지 않는다. 실패는 두 읽기가 끝난 뒤에 올린다.
            // **되감기도 여기서 같이 낸다** (CY-939). 적용 앞이라는 순서는 지키면서 왕복을 겹친다 —
            // 차례로 두면 레디스가 느린 날 그 왕복이 틱을 먹어 발행이 한 틱을 통째로 쉰다.
            return Mono.when(reads, read.then().onErrorResume(e -> Mono.empty()), watchRewind())
                    .then(read)
                    .flatMap(timed -> allocate(timed.demands(), Instant.ofEpochSecond(timed.readAt())))
                    // 터진 회차를 적어 둔다. 재접속 뒤 첫 회차가 되감기 신호를 재는 자리다.
                    .doOnError(e -> roundFailed.set(true))
                    // **틱에서 잘린 회차도 실패다.** 느려진 레디스는 오류가 아니라 취소로 끝나는데,
                    // 되감기를 만드는 failover 가 바로 그 갈래다.
                    .doOnCancel(() -> roundFailed.set(true));
        });
    }

    /**
     * 이월은 <b>임기마다 한 번</b> 받는다. 매 회차 받으면 방금 쓴 값을 되읽어
     * 평활화가 아무 일도 안 하게 되고, 프로세스당 한 번만 받으면 남이 리더였던
     * 동안 움직인 값을 못 보고 제 옛 값을 이어 쓴다 — 리더가 바뀐 직후가 진동하기
     * 가장 쉬운 구간이다.
     */
    private Mono<Void> seeded() {
        if (smoother.get() != null) {
            return Mono.empty();
        }
        return Mono.defer(restore)
                // 리더가 되는 순간은 직전 리더가 죽은 직후라 레디스가 가장 흔들린다.
                // 여기서 포기하면 이월이 정확히 필요한 조건에서만 꺼진다. 흔들림이
                // 이어지는 내내 찍히지 않게 처음 한 번만 남긴다.
                .doOnError(e -> {
                    if (carryoverMisses.get() == 0) {
                        log.warn("평활화 이월 실패 — 다음 회차에 다시 받는다", e);
                    }
                })
                // **폴백을 여기 설치하지 않는다.** 다음 회차가 이월을 아예 안 시도하게
                // 되고, 미관측 폴백은 첫 관측치를 평활 없이 발행한다. 임기 내내 다시
                // 시도하되 콜드 스무더는 저장하지 않아, 흔들림이 지나가면 이어받는다.
                .onErrorResume(e -> {
                    carryoverFailures.incrementAndGet();
                    if (carryoverMisses.incrementAndGet() == CARRYOVER_WARN_AFTER) {
                        log.warn("평활화 이월을 {}회차 못 받았다 — 그동안 이 임기의 평활로 돈다",
                                CARRYOVER_WARN_AFTER);
                    }
                    return Mono.empty();
                })
                .doOnNext(restored -> carryoverReturned())
                .doOnNext(this::carried)
                .then();
    }

    /**
     * 리더가 됐다. <b>평활화 이월을 버린다</b> — 안 버리면 남이 리더였던 동안 움직인
     * 값을 못 보고 제 옛 값을 이어 쓴다. 회차 도중에 잃어 발행 안 된 채
     * 전진한 값도 여기서 정리된다.
     */
    public void leadershipAcquired() {
        leadershipAcquired(-1);
    }

    /**
     * 리더십을 잃었다. <b>창은 여기서 닫아야 지속 시간이 리더 구간만 담는다</b> — 되찾는 자리에서 닫으면 비리더
     * 구간이 섞여 장애가 실제보다 길게 읽힌다.
     */
    public void leadershipLost() {
        overshoot.exited().ifPresent(r -> log.info(
                "리더십을 잃었다 — 배분 예산 초과 창을 닫는다. {}초 동안 {}틱 넘겼다",
                r.elapsedSeconds(), r.swallowed()));
        pollOvershoot.exited().ifPresent(r -> log.info(
                "리더십을 잃었다 — 폴링 예산 초과 창을 닫는다. {}초 동안 {}틱 넘겼다",
                r.elapsedSeconds(), r.swallowed()));
        failures.exited().ifPresent(r -> log.info(
                "리더십을 잃었다 — 적용 실패 창을 닫는다. {}초 동안 {}건 실패했다",
                r.elapsedSeconds(), r.swallowed()));
        // 되감기 표시도 임기를 안 넘긴다. 넘기면 되찾은 노드가 옛 사건을 지금 값처럼 낸다.
        roundFailed.set(false);
        rewindFailures.exited().ifPresent(r -> log.info(
                "리더십을 잃었다 — 되감기 측정 실패 창을 닫는다. {}초 동안 {}회차 못 쟀다",
                r.elapsedSeconds(), r.swallowed()));
        term.incrementAndGet();
    }

    /**
     * 리더가 됐다. 조인 적 없는 노드는 램프가 안 걸려 첫 회차가 목표까지 뛴다.
     *
     * @param publishedCredit 마지막으로 본 발행 몫. 모르면 음수 — 앞 임기 기준을 잇는다
     */
    public void leadershipAcquired(long publishedCredit) {
        leadershipAcquired(publishedCredit, List.of());
    }

    /**
     * 승계한다. <b>발행된 스냅샷의 매진 쿠폰을 씨앗으로 받는다</b> (CY-935) — 표시가 리더 메모리라
     * 승계에서 사라지는데, 상한 중에는 발행이 늘 거부돼 새 리더가 그것을 다시 채울 길이 없다.
     * 그러면 줄을 지워 메모리를 줄일 경로가 승계 한 번에 죽는다.
     *
     * @param publishedSoldOut 발행된 스냅샷이 매진이라고 적은 쿠폰들. 노드가 이미 받아 간 사실이다
     */
    public void leadershipAcquired(long publishedCredit, Collection<String> publishedSoldOut) {
        if (publishedCredit >= 0) {
            releaseRamp.resumeFrom(publishedCredit);
            // **원인을 적어 둔다.** 안 적으면 다음 회차의 램프 진입 로그가
            // "몫 올림" 으로만 나가, 승계를 원인에서 못 읽는다.
            log.info("승계 — 램프를 발행 몫 {} 에서 다시 세운다", publishedCredit);
        } else {
            // **위험한 쪽이 무음이면 안 된다.** 앞 임기의 기준이 남아
            // 있으면 램프는 걸린 채다 — 그 구분까지 실어야 없는 계단을 안 찾는다.
            log.warn("승계 — 발행 몫을 모른다. 앞 임기 기준이 있으면 그것을 이어 쓴다");
        }
        smoother.set(null);
        interim.set(null);
        smoothedCredit = Double.NaN;
        // 나간 매진 표시는 발행된 스냅샷에서 다시 세운다. 앞 임기의 메모리를 이어 쓰면 아직 아무
        // 노드도 못 받은 매진의 줄을 지우고, 통째로 비우면 상한 중 승계에서 정리가 영영 안 돈다.
        announced.clear();
        announced.addAll(publishedSoldOut);

        // 되감기 신호도 임기마다 비운다. 앞 임기의 값이 이번 임기 것으로 읽힌다.
        rewoundCoupons = Double.NaN;
        roundFailed.set(false);
        term.incrementAndGet();
        // **이월 실패 창도 닫는다.** 조용히 버리면 찍힌 진입 경고에 해제가 영영 없다.
        int missed = carryoverMisses.getAndSet(0);
        if (missed > 0) {
            log.info("리더십이 갈렸다 — 이월 실패 창을 닫는다. {}회차 못 받았다", missed);
        }
        // **조임 창도 닫는다.** 안 닫으면 회복 로그가 비리더 구간까지 포함한 지속
        // 시간을 찍는다. 버렸다는 것은 남긴다 — 조용히 버리면 찍힌 진입 경고 하나에
        // 해제가 영영 안 생긴다.
        paused.exited().ifPresent(r -> log.info(
                "리더십이 갈렸다 — 조임 창을 닫는다. 그동안 {}틱 조였다", r.swallowed()));
        ramping.exited().ifPresent(r -> log.info(
                "리더십이 갈렸다 — 램프 창을 닫는다. 그동안 {}틱 올렸다", r.swallowed()));
        // **나머지 창도 같이 닫는다** (CY-824). 강등에서 이미 닫았으면 여기서는 아무 일도 안 한다 — 강등 콜백을
        // 못 받고 넘어온 경우에만 남는 그물이다. 지속 시간은 비리더 구간이 섞여 강등 쪽에서만 적는다.
        overshoot.exited().ifPresent(r -> log.info(
                "리더십이 갈렸다 — 배분 예산 초과 창을 닫는다. 그동안 {}틱 넘겼다", r.swallowed()));
        pollOvershoot.exited().ifPresent(r -> log.info(
                "리더십이 갈렸다 — 폴링 예산 초과 창을 닫는다. 그동안 {}틱 넘겼다", r.swallowed()));
        failures.exited().ifPresent(r -> log.info(
                "리더십이 갈렸다 — 적용 실패 창을 닫는다. 그동안 {}건 실패했다", r.swallowed()));
        // **램프 기준은 안 버린다.** 브레이크라서 그렇다 — 모른다는 것이 놓을 이유가
        // 되면 회복 도중 승계가 끼는 순간 계단이 복원된다. 회차 타임아웃과 레디스
        // 압박이 겹치는 구간이 곧 리더가 바뀌기 가장 쉬운 구간이라 드물지도 않다.
    }

    /**
     * <b>쓰기 직전에 다시 묻는다.</b> 회차 시작에서만 보면 리스가 10ms 남은 채 시작한
     * 회차가 한 틱을 꽉 채워 돌고, 그 사이 다음 리더가 자기 회차를 돈다. 묻는 비용은
     * 메모리 읽기 하나다.
     */
    private boolean lostLeadership() {
        if (stillLeader.getAsBoolean()) {
            return false;
        }
        log.warn("회차 도중에 리더십을 잃었다 — 쓰지 않고 접는다");
        return true;
    }

    /**
     * 회차가 접혔다. <b>램프 기준을 되돌린다</b> — 발행 안 된 회차가 기준을
     * 전진시키면 다음 발행이 실제로 나간 값의 배수에서 시작한다.
     */
    private Mono<Void> fold(ReleaseRamp.State before) {
        releaseRamp.restore(before);
        return Mono.empty();
    }

    /**
     * 램프의 진입과 해제를 쌍으로 남긴다. <b>발행하는 회차에서만 부른다</b> —
     * 접힌 회차가 진입 자리를 먹으면 다음 회복에 진입 로그가 아예 안 나온다.
     */
    private void watchRamp(boolean gatedNow, long credit, long target) {
        // **창은 실제로 푸는 회차에 연다.** 걸렸다는 것만 보고 열면 해제의 지속 시간에
        // 장애 구간이 섞인다. 회복 도중 다시 조이면 거기서 끊고, 원인은 못 박지 않는다
        // — 배수 제한은 서킷이 내내 닫힌 채 뒷단이 늘어난 회차에도 걸린다.
        if (gatedNow) {
            ramping.exited().ifPresent(r -> log.info(
                    "몫 올림 램프 중단 — {}틱 올리다 다시 조인다", r.swallowed()));
        } else if (releaseRamp.ramping()) {
            if (ramping.entered()) {
                log.info("몫 올림 램프 진입 — 몫을 {} 부터 회차당 {}배로 올린다, 목표 {}",
                        credit, String.format("%.1f", ReleaseRamp.DEFAULT_STEP), target);
            }
        } else {
            ramping.exited().ifPresent(r -> log.info(
                    "몫 올림 램프 종료 — {}틱 걸려 {} 로 돌아왔다", r.swallowed(), credit));
        }
    }

    /**
     * 서킷이 열린 동안 배분을 조인다. 임계를 올리면 큐에서 나온 사람이 토큰을 쥐고
     * 503 을 받아 자리를 잃는다. half-open 에서 <b>0 으로 막지는 않는다</b> — 뒷단에
     * 닿는 호출이 없으면 서킷이 표본을 못 채워 영영 안 닫힌다.
     */
    private long gated(long credit, CircuitState now) {
        if (now == CircuitState.CLOSED) {
            paused.exited().ifPresent(r -> log.info(
                    "서킷 회복 — {}초 동안 {}틱을 배분 없이 보냈다. 임계가 그만큼 안 올라갔다",
                    r.elapsedSeconds(), r.swallowed()));
            return credit;
        }
        if (paused.entered()) {
            // **진입을 남긴다.** 안 남기면 배분이 왜 멎었는지 알 방법이
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
        // **이월을 못 받았어도 회차는 돈다.** 여기서 멈추면 레디스가 흔들릴 때 배분이
        // 통째로 안 시작한다. 그 스무더는 이월 자리에 저장하지 않는다 — 저장하면 흔들림이
        // 지나가도 그 임기 내내 콜드로 남는다. 대신 임시로 이어 쓴다 — 회차마다 새로
        // 만들면 실패가 이어지는 내내 관측치가 생으로 나간다.
        CreditSmoother carried = smoother.get();
        CreditSmoother current = carried != null ? carried : interim.updateAndGet(
                s -> s == null ? CreditSmoother.of(CreditSmoother.DEFAULT_ALPHA) : s);
        // **하한은 평활 뒤에 건다.** 하한은 관측이 아니라 정책이다. 평활을 거치면
        // 앞선 낮은 값에서 올라오는 데 열 틱이 넘고, 그동안 노드당 몫이 유휴 비율
        // 아래에 머물러 한산 통과 상한이 0 이다 — 한산한 쿠폰은 줄 없이 통과해야 한다.
        long observed = Math.max(0, globalCredit.getAsLong());
        double smoothedValue = current.observe(observed);
        smoothedCredit = smoothedValue;
        long smoothed = Math.round(smoothedValue);
        // **서킷은 평활과 하한 뒤에 건다.** 앞에 걸면 평활이 0 을 천천히 내리는 사이
        // 첫 회차에 수천이 그대로 나간다. 서킷은 관측이 아니라 사실이라 정책인 하한보다
        // 뒤다. 회차마다 한 번만 읽는다 — 두 번 읽으면 한 회차가 자기모순이 된다.
        CircuitState circuitNow = circuit.get();
        long floorNow = Math.max(0, creditFloor.getAsLong());
        long target = Math.max(smoothed, floorNow);
        long allowed = gated(target, circuitNow);
        // **램프에 정책 하한을 같이 넘긴다.** 하한 아래로 눌린 회차는 노드당 몫이 유휴
        // 비율 아래라, 줄 설 이유가 없는 쿠폰이 전 노드에서 줄을 선다. `creditFloor`
        // 는 회복 구간에 0 이라, 이 최소가 유효 하한 이하인 안전한 어림이다.
        long r1Minimum = CapacityCollector.idleMinimum(gatewayCount.getAsInt());
        // **푸는 쪽에도 제약이 있어야 한다.** 조이는 동안 평활은 조여진 값을 한 번도 안
        // 봐서, 서킷이 닫히는 한 틱에 배분이 1 에서 원래 몫으로 돌아간다. 조였는지는 값을
        // 견주지 않고 서킷에게 묻는다 — 보고가 0 인 회차가 가장 큰 계단을 만든다.
        boolean gatedNow = circuitNow != CircuitState.CLOSED;
        // **접힌 회차가 기준을 올리면 안 된다.** 발행이 안 된 회차가 기준을
        // 전진시키면 다음 발행이 실제로 나간 값의 배수에서 시작한다.
        ReleaseRamp.State before = releaseRamp.snapshot();
        long credit = releaseRamp.next(allowed, Math.max(floorNow, r1Minimum), gatedNow);
        // 적용이 동시에 돌며 실패한 몫을 접으므로 잠근다. 순서는 발행이 쓰므로 그대로 둔다.
        Map<String, Long> granted = Collections.synchronizedMap(new LinkedHashMap<>());
        allocator.allocate(credit, collected).forEach(g -> granted.put(g.couponId(), g.credit()));

        if (lostLeadership()) {
            releaseRamp.restore(before);
            return Mono.empty();
        }
        watchBudget(credit, observed);
        AtomicBoolean anyFailed = new AtomicBoolean();
        AtomicBoolean published = new AtomicBoolean();
        boolean anyCredit = granted.values().stream().anyMatch(c -> c > 0);
        rememberIds(collected);
        return (anyCredit ? pacer.turn() : Mono.<Void>empty())
                .thenMany(Flux.fromIterable(collected))
                // **동시에 보낸다.** 차례로 보내면 왕복이 쿠폰 수만큼 쌓여 레디스가 느린 날 회차가 틱을
                // 넘기고 발행이 잘린다. 옛 임기의 쓰기는 적용 스크립트의 펜스가 막는다.
                .flatMap(demand -> applyOne(demand, granted, anyFailed), MAX_CONCURRENT_APPLIES)
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
                    // **회차가 통째로 성공해야 걷힌 것이다.** 쿠폰 하나가 계속
                    // 실패하고 다른 쿠폰이 성공하는 동안 매 회차 복귀를 찍으면,
                    // 실패도 복귀도 아닌 두 줄이 영원히 반복된다.
                    if (!anyFailed.get()) {
                        failures.exited().ifPresent(recovered ->
                                log.info("배분 적용 복귀 — {}초 만에, 그동안 {}건 실패",
                                        recovered.elapsedSeconds(), recovered.swallowed()));
                    }
                    // 세는 값이라 지표 자리다. 초당 한 줄이면 진단이 필요한
                    // 순간에 다른 로그가 여기 묻힌다.
                    log.debug("배분 한 회차 — 크레딧 {}, 들인 인원 {}, 쿠폰 {}개",
                            credit, admitted, collected.size());
                })
                .then(Mono.defer(() -> lostLeadership()
                        ? fold(before)
                        // 램프 창은 나간 회차에서만 연다. 앞에 두면 안 나간 회차가
                        // 진입 자리를 먹어 다음 회복에 로그가 안 나온다.
                        : publishRound(collected, granted, credit, readAt, current)
                        .doOnSuccess(done -> {
                            published.set(true);
                            watchRamp(gatedNow, credit, target);
                            // **나간 매진을 적어 둔다.** 노드가 모르는 채로 줄이 사라지면 낡은
                            // 스냅샷을 든 노드가 그 사람을 맨 뒤에 세운다 — 순번 역행이다.
                            announce(couponsOf(collected, granted));
                        })
                        // **발행 뒤에 지운다.** 앞에 두면 방금 지운 큐가 이번
                        // 재료에 아직 대기자로 실려 없는 줄에 크레딧이 나간다. 미루지
                        // 않으면 발행이 구독되기 전에 셈과 로그가 먼저 일어난다.
                        .then(Mono.defer(() -> cleanUp(collected, granted)))
                        // **정리 뒤에 쓴다.** 앞에 두면 곧 지울 줄을 훑느라
                        // 예산을 쓴다.
                        .then(Mono.defer(() -> sweepUp(collected, granted)))
                        // **발행이 못 나가도 이미 나간 매진은 정리한다.** 상한에 닿으면 발행의
                        // 첫 쓰기가 거부되는데, 거기 묶어 두면 줄을 지워 메모리를 줄일 유일한
                        // 경로가 같이 막혀 운영자가 한도를 올려야만 풀린다.
                        .onErrorResume(e -> cleanUp(announced(couponsOf(collected, granted)))
                                .then(Mono.error(e)))))
                // 발행까지 못 간 회차가 기준을 올리면 다음 성공이 그 배수의
                // 배수에서 시작한다. 틱을 넘겨 잘린 회차는 오류가 아니라 취소다.
                .doOnError(e -> restoreUnpublished(published, before))
                .doOnCancel(() -> restoreUnpublished(published, before));
    }

    /**
     * 되감기 신호를 잰다. <b>터진 회차 다음, 적용 앞에서 읽는다</b> — 적용이 임계를 다시 쓰면 기준이 방금 쓴 값이 되고,
     * 되감기가 해를 끼치는 쿠폰이 정확히 그 쿠폰들이다. 못 재면 표시를 되돌려 다음 회차가 다시 잰다.
     */
    private Mono<Void> watchRewind() {
        Function<List<String>, Mono<RewindCheck>> reader = rewound;
        // **회차의 목록을 안 넘긴다.** 읽는 쪽이 자기가 쓴 임계와 합쳐 보므로, 이 회차의 쿠폰은 보탤 것이
        // 없다 — 안 넘기면 수요를 기다리지 않아도 돼 읽기를 처음부터 나란히 낼 수 있다 (CY-939).
        List<String> ids = List.of();
        if (reader == null || !roundFailed.compareAndSet(true, false)) {
            rewindMeasured.set(false);
            return Mono.empty();
        }
        rewindMeasured.set(true);
        long startedTerm = term.get();
        return reader.apply(ids)
                .filter(seen -> sameTerm(startedTerm))
                .doOnNext(this::rewindSeen)
                .doOnSuccess(done -> {
                    // **닫는 것도 같은 임기만 한다.** 걸러진 완료가 새 임기의 창을 닫으면 그 구간의 해제 로그가 사라진다.
                    if (!sameTerm(startedTerm)) {
                        return;
                    }
                    rewindFailures.exited().ifPresent(recovered -> log.info(
                            "되감기 신호를 다시 잰다 — {}초 만에, 그동안 {}회차 못 쟀다",
                            recovered.elapsedSeconds(), recovered.swallowed()));
                })
                .onErrorResume(e -> {
                    if (!sameTerm(startedTerm)) {
                        return Mono.empty();
                    }
                    // 다음 회차가 다시 잰다. 조용히 버리면 그 장애의 되감기 여부를 영영 모른다.
                    roundFailed.set(true);
                    rewindUnmeasured.incrementAndGet();
                    // 회복 구간이 이 읽기가 가장 잘 실패하는 구간이다. 구간의 첫 건만 남긴다.
                    if (rewindFailures.entered()) {
                        log.warn("되감기 신호를 못 쟀다 — 다음 회차에 다시 잰다: {}", e.toString());
                    }
                    return Mono.empty();
                })
                .then();
    }

    /**
     * 활성에서 빠진 쿠폰의 기준을 버린다. <b>잰 회차에는 안 버린다</b> — 측정이 밀린 동안 버리면 기준째로
     * 사라지고, 되감기가 활성 목록을 비운 회차가 증거를 지우면 다음 실패에 잴 기준이 없다.
     */
    private void rememberIds(List<CouponDemand> collected) {
        List<String> ids = collected.stream().map(CouponDemand::couponId).toList();
        if (!ids.isEmpty() && !rewindMeasured.get()) {
            forgetInactive.accept(ids);
        }
    }

    /** 읽는 사이에 임기가 갈렸으면 버린다. 지나간 임기의 사건이 지금 값으로 들어간다. */
    private boolean sameTerm(long startedTerm) {
        return term.get() == startedTerm;
    }

    /** 본 것을 남긴다. <b>기준이 없으면 깨끗한 것이 아니라 못 잰 것이다.</b> */
    private void rewindSeen(RewindCheck seen) {
        if (seen.measured() == 0) {
            rewoundCoupons = Double.NaN;
            rewindNoBaseline.incrementAndGet();
            log.info("되감기를 견줄 기준이 없다 — 이 노드가 아직 아무 쿠폰에도 안 들였다");
            return;
        }
        rewoundCoupons = seen.rewound().size();
        if (seen.rewound().isEmpty()) {
            log.debug("직전 회차 실패 뒤 첫 회차 — 되감긴 쿠폰 없다. 쿠폰 {}개를 견줬다", seen.measured());
            return;
        }
        rewoundEvents.incrementAndGet();
        log.warn("저장소가 뒤로 감겼다 — rewound={}, measured={}, coupons={}. 이미 통과한 사람이 다시 "
                        + "들어올 수 있다. 초과 발급과 순번 역행을 그 쿠폰들에서 확인하라",
                seen.rewound().size(), seen.measured(),
                seen.rewound().stream().limit(REWOUND_LOG_LIMIT).toList());
    }

    /** 되감긴 회차의 누적 수. 게이지는 마지막 값을 붙들고 있어 지나간 사건을 이걸로 센다. */
    public double rewoundEvents() {
        return rewoundEvents.get();
    }

    /** 견줄 기준이 없어 못 잰 누적 횟수. */
    public double rewindNoBaseline() {
        return rewindNoBaseline.get();
    }

    /** 되감기 신호를 못 잰 누적 횟수. */
    public double rewindUnmeasured() {
        return rewindUnmeasured.get();
    }

    /**
     * 발행이 나간 회차는 안 되돌린다 — 그 몫은 노드에 실제로 닿았다. <b>단위 시험이
     * 못 닿는 갈래다</b>: 발행 뒤의 정리·걷기는 오류를 스스로 삼켜, 남는 것은 틱을
     * 넘겨 잘리는 회차뿐이다.
     */
    private void restoreUnpublished(AtomicBoolean published, ReleaseRamp.State before) {
        if (!published.get()) {
            releaseRamp.restore(before);
        }
    }

    /**
     * 매진된 지 오래된 쿠폰의 줄을 지운다.
     *
     * <p><b>정리 실패가 배분을 막지 않는다.</b> 다음 틱에 다시 온다.
     */
    private Mono<Void> cleanUp(List<CouponDemand> collected, Map<String, Long> granted) {
        return cleanUp(couponsOf(collected, granted));
    }

    /** 이미 나간 매진만 남긴다. 발행이 못 나간 회차는 이것만 본다. */
    private Map<String, CouponState> announced(Map<String, CouponState> coupons) {
        Map<String, CouponState> left = new LinkedHashMap<>();
        coupons.forEach((couponId, state) -> {
            if (announced.contains(couponId)) {
                left.put(couponId, state);
            }
        });
        return left;
    }

    /** 발행에 실린 매진을 적고, 재입고된 쿠폰은 지운다. */
    private void announce(Map<String, CouponState> coupons) {
        coupons.forEach((couponId, state) -> {
            if (state.soldOut()) {
                announced.add(couponId);
            } else {
                announced.remove(couponId);
            }
        });
        announced.retainAll(coupons.keySet());
    }

    private Mono<Void> cleanUp(Map<String, CouponState> coupons) {
        List<String> due = cleanup.due(coupons);
        List<String> claimed = cleanup.claimed();
        if (due.isEmpty() && claimed.isEmpty()) {
            return Mono.empty();
        }
        // **쓰기 직전에 다시 묻는다.** 회차 안에서 유일하게 되돌릴 수 없는
        // 쓰기라, 리더가 아닌 채로 내면 남의 줄을 지운다. 묻는 비용은
        // 메모리 읽기 하나다.
        if (lostLeadership()) {
            return Mono.empty();
        }
        // **세기 시작한 줄에 먼저 표를 세운다.** 표는 지웠을 때만 생겨 한 번도
        // 안 지운 줄에는 없는데, 그게 울타리가 지키려던 경우다 — 얼었다 깨어난 옛 리더가
        // 새 리더는 아직 지울 생각도 없는 줄을 지운다.
        Mono<Void> claim = claimQueues.apply(claimed)
                // **선 것만 확인으로 친다.** 실패한 것을 확인으로 치면 그 줄은
                // 표 없이 유예를 보내고, 옛 임기가 그대로 지운다.
                .doOnNext(cleanup::fenceConfirmed)
                .then();
        if (due.isEmpty()) {
            return claim;
        }
        // **몇 개인지만 남긴다.** 목록을 통째로 찍으면 대량 매진에서 한 줄에
        // 쿠폰 ID 가 수백 개 들어간다. 어느 쿠폰인지는 지운 뒤에 남긴다.
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

    /** 이탈자를 걷어 낸다. 멈춰야 할 구간은 스위퍼가 안다. */
    private Mono<Void> sweepUp(List<CouponDemand> collected, Map<String, Long> granted) {
        if (lostLeadership()) {
            return Mono.empty();
        }
        return sweeper.run(couponsOf(collected, granted),
                // **리더가 신선한 것과 노드들이 신선한 것은 다르다.** 생존 신호는 노드
                // 쪽 폴링이 갱신하므로 그쪽이 멎어도 리더의 수요 읽기는 성공한다. 이
                // 노드도 게이트웨이라 자기 재료의 나이가 그 신호에 가장 가깝다.
                dataStale.getAsBoolean()).then();
    }

    /**
     * 나눠 준 예산이 <b>뒷단이 받는다고 한 것</b>을 넘었는가. 넘는 자리는 배분기가
     * 아니라 평활 지연과 하한이다 — 뒷단이 1,000 으로 떨어져도 평활은 열 틱 넘게 7,300 을
     * 나눠 준다. 관측치를 인자로 받아야 한 회차가 서로 다른 두 값을 견주지 않는다.
     */
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
     * 실제로 들여보낸 수가 예산을 넘었는가. <b>나눠 준 수와 다르다</b> — 동점
     * score 가 있으면 임계 하나에 여럿이 걸려 준 몫보다 많이 들어간다.
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

    /** 차례를 준 누적 인원. 크레딧 낭비의 분모다. */
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

    /** 이월을 값째 받은 누적 임기 수. */
    public double carryoverRestored() {
        return carryoverRestored.get();
    }

    /** 이월을 읽었는데 이을 값이 없던 누적 임기 수. */
    public double carryoverEmpty() {
        return carryoverEmpty.get();
    }

    /** 못 받은 채 이 임기의 발행이 이월 자리를 덮은 누적 임기 수. */
    public double carryoverReplaced() {
        return carryoverReplaced.get();
    }

    /** 이월 읽기가 실패한 누적 시도 수. 한 임기에서 여러 번 오를 수 있다. */
    public double carryoverFailures() {
        return carryoverFailures.get();
    }

    /** 마지막 회차의 평활값. <b>리더가 아니면 NaN 이다</b> — 굳은 값이 섞이면 못 읽는다. */
    public double smoothedCredit() {
        return stillLeader.getAsBoolean() ? smoothedCredit : Double.NaN;
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
        // 그 사이 다음 리더가 자기 회차를 돈다.
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
     * 런타임을 <b>발행하는 그 쌍에서</b> 유도한다. 못 박으면 다 뺄 수 있는 줄까지 줄 서는
     * 중이 되어 도메인이 막고, 그 쿠폰만 떨어져 매진으로 보인다. 재고가 소진되면 매진으로
     * 넘긴다 — 이 전이가 없으면 줄이 영영 안 빠진다.
     */
    private CouponState stateOf(CouponDemand demand, Map<String, Long> granted) {
        // **못 읽은 재고를 매진으로 안 접는다.** 접으면 그 쿠폰이 종결되고
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
     * 쿠폰별 상태만. <b>정리와 청소는 이것만 쓴다</b> — 스냅샷을 통째로 만들면 배수
     * 계산과 그 계측이 한 회차에 세 번 돌아, 누적 틱과 해제 로그가 3배로 부푼다.
     */
    private Map<String, CouponState> couponsOf(
            List<CouponDemand> collected, Map<String, Long> granted) {
        Map<String, CouponState> coupons = new LinkedHashMap<>();
        collected.forEach(demand -> coupons.put(demand.couponId(), stateOf(demand, granted)));
        return coupons;
    }

    /** 발행할 재료 한 회차. <b>순수하다</b> — 몇 번을 만들어도 세는 값이 안 는다. */
    private GatewaySnapshot snapshot(List<CouponDemand> collected, Map<String, Long> granted,
            long credit, Instant readAt, Tunables applied, double pollScale) {
        return new GatewaySnapshot(couponsOf(collected, granted),
                meta(credit, applied).withPollScale(pollScale), readAt, routable.get());
    }

    // **노드 수 방어를 여기서 다시 쓰지 않는다.** 사본이 생기면 둘 중 하나만
    // 시험이 붙고, 하트비트가 다 만료돼 0 으로 보이는 순간 — 즉 클러스터가
    // 흔들리는 바로 그 순간 — 예산이 0 이 되어 배수가 통째로 꺼진다.
    private SnapshotMeta meta(long credit, Tunables applied) {
        return SnapshotMeta.withoutPollScale(credit, gatewayCount.getAsInt(), applied);
    }

    /**
     * 배수를 실어 발행한다. <b>센 것은 발행이 끝난 뒤에 남긴다</b> — 인자로 부르면 자바가
     * 먼저 평가해서, 스냅샷 샤드만 죽어 발행이 터진 구간을 배수를 걸었다고 기록한다.
     * 그때 전 노드는 옛 재료로 배수 1.0 을 쓰고 있다.
     */
    private Mono<Void> publishRound(List<CouponDemand> collected, Map<String, Long> granted,
            long credit, Instant readAt, CreditSmoother current) {
        PollBudgetPlanner.Scale budget = pollBudget(collected, granted, credit);
        // **회차마다 한 번 센다.** 상태를 만드는 자리에서 세면 정리·청소·발행이
        // 같은 회차를 세 번 훑어 셋으로 부푼다.
        long unknown = collected.stream().filter(d -> !d.stockKnown()).count();
        // **히스테리시스는 아직 빈 값을 싣는다.** 제품이 아직 히스테리시스를 안 돌려
        // 실을 상태가 없다. 돌리기 시작하면 여기가 매 틱 이월을 지우는 자리가 되므로,
        // 기본값에 숨기지 않고 눈에 보이게 둔다.
        return publish.apply(codec.encode(
                        snapshot(collected, granted, credit, readAt,
                                tunables.get().orElse(null), budget.scale()),
                        current.snapshot(), QueueingHysteresis.Snapshot.empty()))
                // **발행이 끝난 뒤에 센다.** 앞에서 세면 스냅샷 샤드가 죽어
                // 발행이 매 틱 터지는 구간 — 재고 키를 잃기 가장 쉬운 구간 —
                // 에서 발행 안 된 회차가 발행된 것으로 잡힌다.
                .doOnSuccess(done -> {
                    watchPollBudget(budget);
                    stockUnknownTicks.addAndGet(unknown);
                    replacedByPublish(current);
                });
    }

    /**
     * 이번 틱의 전역 폴링 배수. 예산은 도메인이 소유한다 — 밴드도 하한도 거기 있는데
     * 예산만 제어 평면에 두면, 운영자가 만질 유일한 숫자가 시험이 안 닿는 곳에 남는다.
     */
    private PollBudgetPlanner.Scale pollBudget(List<CouponDemand> collected,
            Map<String, Long> granted, long credit) {
        // **조립은 도메인이 쥔다.** 여기서 다시 조립하면 분모를 바꾸는 날 시나리오와
        // 갈라져, 낡은 값을 초록으로 단언하는 시험이 남는다.
        return PollBudgetPlanner.scaleFor(meta(credit, null), collected,
                couponId -> granted.getOrDefault(couponId, 0L));
    }

    /**
     * 배수가 1 을 넘는 것은 상태 전이다. 전 대기자의 다음 폴링이 한꺼번에 늘어나는데,
     * 남기지 않으면 운영자 눈에는 원인 없이 폴링이 뜸해진 것으로만 보인다.
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

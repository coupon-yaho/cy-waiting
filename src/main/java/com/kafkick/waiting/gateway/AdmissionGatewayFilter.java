package com.kafkick.waiting.gateway;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.kafkick.waiting.control.FailureWindow;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.admission.AdmissionRequest;
import com.kafkick.waiting.domain.admission.Bulkhead;
import com.kafkick.waiting.domain.admission.EnqueueLatch;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.HttpHeaders;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 발급 요청을 통과·대기·거절로 가른다. <b>판정 재료는 로컬 스냅샷에서만 읽는다</b>
 * — 요청마다 레디스를 치면 제어 평면을 만든 이유가 사라진다.
 *
 * <p><b>스스로 안 걸린다.</b> 라우트가 이 인스턴스를 받아 붙인다 — 이름으로 적으면
 * 안 풀렸을 때 기동은 되고 판정만 사라진다.
 */
@Component
public final class AdmissionGatewayFilter implements GatewayFilter {

    /** 응답을 쓰는 쪽이 읽는다. 다시 판정하면 두 번 세고 답이 갈릴 수 있다. */
    public static final String DECISION = "waiting.admission.decision";

    private static final Logger log = LoggerFactory.getLogger(AdmissionGatewayFilter.class);

    private static final String COUPON_ID = "couponId";

    /** 판정 결과를 사유별로 센다. <b>요청마다 로그를 남기지 않는다</b> — 낡음
     * 구간에서 로그가 폭주하고, 그때 정작 봐야 할 것이 묻힌다. */
    private static final String METRIC = "waiting.admission";

    /** 받아도 되는 최대 대기 시간. 넘으면 줄을 세우는 것이 되레 나쁘다. */
    static final long MAX_ETA_SEC = 600;

    /** 재시도 안내의 흔들림 폭. 폴링 간격과 같은 정책을 쓴다. */
    private static final PollIntervalPolicy POLL = PollIntervalPolicy.of(0.2);

    private static final String MEMBER_ID = "X-Member-Id";

    /** 발급 계층 명세가 정한 이름. 조회가 준 토큰을 여기 실어 온다. */
    private static final String ENTRY_TOKEN = "Entry-Token";

    /**
     * 장애 개방이 노드 예산에서 가져다 쓰는 비율.
     *
     * <p>상수로 두면 뒷단 가용량과 무관한 양이 나간다. 판정이 쓰는 예산에서
     * 몫을 떼되, 전부는 안 준다 — 그 초에 통과할 사람의 몫이 남아야 한다.
     */
    private static final double FAIL_OPEN_SHARE = 0.5;

    /**
     * 쿠폰별 표를 들고 있는 자리들이 담을 수 있는 쿠폰 수. 2,000개를 상정한 값이다.
     *
     * <p><b>쿠폰 식별자는 밖에서 오는 값이라 가짓수에 상한이 없다.</b> 넘었을 때
     * 무엇을 하는지는 자리마다 다르다 — 래치는 통째로 비우고(판정이 한 틱
     * 헐거워질 뿐이다), 격벽은 새 쿠폰을 안 받는다.
     */
    private static final int MAX_COUPON_KEYS = 10_000;

    /**
     * 한 건이 뒷단에 걸려 있을 수 있는 시간(초). 상한은 초당 예산 × 이 값이다.
     *
     * <p>유입은 같은 예산이 이미 조이므로 걸려 있는 수는 <b>예산 × 지연</b>이고,
     * 이 값이 곧 격벽이 막기 시작하는 지연이다. 서킷의 느림 임계보다 커야
     * 느린 뒷단이 서킷에 집계된 뒤에 막힌다 — 6.8.1 에서 튜너블로 뺀다.
     */
    private static final long MAX_IN_FLIGHT_SEC = 3;

    /**
     * 자리를 놓게 하는 상한의 여유 배수.
     *
     * <p>뒷단 응답 상한(12초)보다 뒤여야 합니다. 여기가 먼저 끊으면 서킷에 가는
     * 것이 오류가 아니라 취소가 되고, 취소는 창에 안 쌓입니다 — 멎은 뒷단의
     * 서킷이 영영 안 열립니다. 여기는 그 상한이 안 걸렸을 때의 마지막 그물입니다.
     */
    private static final long IN_FLIGHT_GRACE = 5;

    /** 자리를 놓게 하는 시한. 시험이 손으로 베끼지 않게 여기서 한 번만 정한다. */
    static final Duration MAX_IN_FLIGHT =
            Duration.ofSeconds(MAX_IN_FLIGHT_SEC * IN_FLIGHT_GRACE);

    private final SnapshotHolder holder;
    private final AdmissionDecider decider;
    private final Clock clock;
    private final MeterRegistry meters;
    private final DoubleSupplier random;
    private final QueuePort queue;
    private final QueueToken tokens;
    private final EntryToken entryTokens;
    private final SecondWindowLimiter limiter;
    private final EnqueueLatch latch;

    /** 뒷단의 멱등성이 작동할 근거. 같은 시도에 같은 값을 준다 (A-10). */
    private final IdempotencyKey idempotency;

    /** 동시에 걸려 있는 건수를 센다. 리미터가 세는 초당 건수와 단위가 다르다. */
    private final Bulkhead bulkhead = Bulkhead.withMaxKeys(MAX_COUPON_KEYS);
    private final ApiError error;
    private final QueueResponse waiting = QueueResponse.create();

    /** 설정 오류를 한 번만 알린다. 라우트가 틀렸으면 늘 틀리다. */
    private final AtomicBoolean misconfigured = new AtomicBoolean();

    /**
     * fail-open 구간의 진입과 해제 (LG-2).
     *
     * <p>이 전이가 로그에 없으면 사후에 <b>추월이 언제 열렸는지</b>를 못 짚는다.
     * 지표는 초 단위로 뭉개져 남고 보존 기간도 짧아, 사고 조사에서 필요한
     * "몇 시 몇 분에 열려 얼마나 갔는가" 를 답하지 못한다.
     */
    private final FailureWindow failOpenWindow;

    /**
     * 보호 장치가 끊는 구간.
     *
     * <p>카운터만 두면 사후에 "몇 시부터 몇 시까지, 몇 건을 끊었나" 를 못 답합니다.
     * 그 답이 필요한 때는 늘 사고가 끝난 뒤입니다 (LG-2).
     */
    private final FailureWindow shedWindow;

    private AdmissionGatewayFilter(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters, DoubleSupplier random,
            QueuePort queue, QueueToken tokens, SecondWindowLimiter limiter,
            EntryToken entryTokens, IdempotencyKey idempotency, LongSupplier ticker) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
        this.failOpenWindow = FailureWindow.of(ticker);
        this.shedWindow = FailureWindow.of(ticker);
        this.decider = Objects.requireNonNull(decider, "decider 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.random = Objects.requireNonNull(random, "random 은 필수다");
        this.queue = Objects.requireNonNull(queue, "queue 는 필수다");
        this.tokens = Objects.requireNonNull(tokens, "tokens 는 필수다");
        this.entryTokens = Objects.requireNonNull(entryTokens, "entryTokens 는 필수다");
        this.limiter = Objects.requireNonNull(limiter, "limiter 는 필수다");

        // **래치 수명을 여기서 정하지 않는다.** 스냅샷을 아직 믿는 한계보다 짧으면
        // 그 차이가 그대로 추월 창이 된다. 두 값이 다른 클래스에 있으면 조용히
        // 갈라지므로, 한계를 정한 쪽에서 끌어온다.
        this.latch = EnqueueLatch.covering(MAX_COUPON_KEYS, holder.dataStaleAfter());
        this.idempotency = Objects.requireNonNull(idempotency, "idempotency 는 필수다");
        this.error = ApiError.of(clock);
    }

    /** 흔들림의 난수원은 스레드마다 따로 둔다 — 공유하면 그 자체가 경합점이다. */
    @Autowired
    AdmissionGatewayFilter(SnapshotHolder holder, AdmissionDecider decider, Clock clock,
            MeterRegistry meters, QueuePort queue, QueueToken tokens,
            SecondWindowLimiter limiter, EntryToken entryTokens,
            IdempotencyKey idempotency) {
        this(holder, decider, clock, meters,
                () -> ThreadLocalRandom.current().nextDouble(), queue, tokens, limiter,
                entryTokens, idempotency, System::nanoTime);
    }

    public static AdmissionGatewayFilter of(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters, QueuePort queue, QueueToken tokens,
            SecondWindowLimiter limiter, EntryToken entryTokens,
            IdempotencyKey idempotency) {
        return new AdmissionGatewayFilter(holder, decider, clock, meters, queue, tokens, limiter,
                entryTokens, idempotency);
    }

    /** 난수원을 받는다. 고정하지 못하면 흔들림이 실제로 붙었는지 못 잰다 (TS-4). */
    public static AdmissionGatewayFilter of(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters, DoubleSupplier random,
            QueuePort queue, QueueToken tokens, SecondWindowLimiter limiter,
            EntryToken entryTokens, IdempotencyKey idempotency) {
        return new AdmissionGatewayFilter(holder, decider, clock, meters, random, queue,
                tokens, limiter, entryTokens, idempotency, System::nanoTime);
    }

    /**
     * 구간 시계를 받는다. <b>요청 시계와 따로다</b> — 구간 길이는 단조 시계로 재야
     * NTP 가 시각을 되돌릴 때 음수가 안 된다.
     *
     * <p>고정하지 못하면 fail-open 이 얼마나 이어졌는지를 재는 계산 자체가
     * 시험에서 늘 0 이 되어, 단위를 틀려도 통과한다 (TS-4).
     */
    public static AdmissionGatewayFilter of(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters, DoubleSupplier random,
            QueuePort queue, QueueToken tokens, SecondWindowLimiter limiter,
            EntryToken entryTokens, IdempotencyKey idempotency, LongSupplier ticker) {
        return new AdmissionGatewayFilter(holder, decider, clock, meters, random, queue,
                tokens, limiter, entryTokens, idempotency, ticker);
    }

    /**
     * 사유별로 센다. <b>쿠폰 ID 를 라벨에 안 넣는다</b> — 인증이 없어 아무 문자열이나
     * 들어오고, 그러면 지표 하나가 메모리를 밀어낸다.
     */
    private void count(String outcome) {
        count(outcome, FailureCause.NONE);
    }

    /**
     * <b>태그 키 집합을 늘 같게 둔다.</b> 같은 이름에 키 집합이 둘이면
     * 프로메테우스 레지스트리가 등록을 거절한다 — 지금은 단순 레지스트리라 안
     * 터지고, 6.5 에서 붙이는 순간 터진다.
     */
    private void count(String outcome, String cause) {
        meters.counter(METRIC, "outcome", outcome, "cause", cause).increment();
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String couponId = pathVariable(exchange);
        if (couponId == null) {
            // 라우트에서 변수 이름을 빼면 판정할 쿠폰이 없다. 그대로 흘리면
            // 판정이 사라진 채로 기동만 성공한다.
            //
            // **이건 설정 오류라 요청마다 나지 않는다** — 라우트가 틀렸으면 늘
            // 틀리므로 한 번 알리면 된다. 계속 찍으면 그게 곧 로그 폭주다.
            if (misconfigured.compareAndSet(false, true)) {
                log.error("라우트에 {} 경로변수가 없다 — 판정할 대상을 못 정한다", COUPON_ID);
            }
            count("no-path-variable");
            return error.write(exchange, ApiError.Code.INVALID_REQUEST);
        }

        SnapshotHolder.View view = holder.view();
        CouponState state = view.snapshot().coupons().get(couponId);
        if (state == null) {
            return unknownCoupon(exchange, chain, view, couponId);
        }

        // **지금 시각이다.** 스냅샷 발행 시각을 넘기면 배분이 멎는 순간 윈도가
        // 영영 안 넘어가고, 상한만큼 쓴 뒤부터 전부 막힌다.
        long nowSec = clock.instant().getEpochSecond();
        AdmissionDecision decision = decider.decide(new AdmissionRequest(
                couponId, state, view.snapshot().meta(),
                // **방금 줄로 보낸 쿠폰인가.** 스냅샷은 한 틱 늦어 아직 한산하다고
                // 말한다 — 그대로 두면 다음 창의 신규 유입이 방금 선 사람을 넘는다.
                // **차례가 온 증거를 들고 왔는가.** 없으면 줄과 무관하게 통과해
                // 기다린 사람과 안 기다린 사람이 같아진다.
                holder.isDataStale(view), hasEntryToken(exchange, couponId), 
                latch.latched(couponId, nowSec),
                nowSec, MAX_ETA_SEC));
        exchange.getAttributes().put(DECISION, decision);
        count(decision.name());
        return route(exchange, chain, decision, couponId, state, view.snapshot().meta());
    }

    /**
     * 스냅샷에 없는 쿠폰. <b>여기서 끝내는 것이 레디스 키 무한 생성을 막는다</b> —
     * 그대로 흘리면 아무 문자열이나 큐를 하나씩 만든다.
     */
    private Mono<Void> unknownCoupon(ServerWebExchange exchange, GatewayFilterChain chain,
            SnapshotHolder.View view, String couponId) {
        // 기동 직후 재료가 없다고 전면 404 를 내면 뜨자마자 모든 쿠폰이 없는 것이
        // 된다. **여기는 상한을 못 건다** — 예산의 근거인 메타 자체가 아직 없어
        // 상한이 0 이 되고, 그건 전면 차단이다. 이 구간은 준비성 판정이 막는다.
        if (view.isBeforeFirstTick()) {
            count("deferred-no-material");
            // 재료가 없어 크레딧을 모른다. 폴백으로 최소 배수 속도를 가정한다.
            return forward(exchange, chain, couponId, 0, view.snapshot().meta());
        }
        // **모른다는 것이 무제한의 사유는 아니다.** 사다리 4번은 같은 무지에서
        // 노드 몫 안에서만 여는데, 여기만 열어 두면 아무 문자열 쿠폰이나 그
        // 상한 밖으로 나간다. 같은 예산에 태운다.
        if (holder.isDataStale(view)) {
            count("deferred-stale-material");
            return failOpen(exchange, chain, view.snapshot().meta(), couponId);
        }
        count("unknown-coupon");
        return error.write(exchange, ApiError.Code.UNKNOWN_COUPON);
    }

    private Mono<Void> route(ServerWebExchange exchange, GatewayFilterChain chain,
            AdmissionDecision decision, String couponId, CouponState state, SnapshotMeta meta) {
        if (decision.isPass()) {
            // **판정이 쓴 예산을 그대로 받는다.** 여기서 credit 을 다시 꺼내면
            // 한산 통과가 0 을 받고, 0 은 상한으로 쓰이는 순간 전면 차단이다 (I1).
            return forward(exchange, chain, couponId,
                    decider.admittedRatePerSec(decision, state, meta), meta);
        }
        if (decision.isEnqueue()) {
            return enqueue(exchange, chain, couponId, state, meta);
        }
        return error.write(exchange, codeOf(decision), retryAfterSec(decision, random));
    }

    /**
     * 줄에 세운다. <b>여기가 요청 경로에서 레디스를 치는 유일한 자리다</b> (RD-4) —
     * 통과한 사람은 여기 안 온다.
     */
    private Mono<Void> enqueue(ServerWebExchange exchange, GatewayFilterChain chain,
            String couponId, CouponState state, SnapshotMeta meta) {
        String memberId = exchange.getRequest().getHeaders().getFirst(MEMBER_ID);
        if (memberId == null) {
            // 형식 검증이 앞에서 걸렀어야 한다. 여기 오면 배선이 틀린 것이다.
            count("no-member");
            return error.write(exchange, ApiError.Code.INVALID_REQUEST);
        }
        // **판정에 쓴 상태를 그대로 쓴다.** 여기서 다시 읽으면 그 사이 틱이
        // 지나 판정과 답이 어긋난다.
        // **같은 것은 MAX_ETA_SEC 인자뿐이다** — 상한 함수는 6번과 일부러 다르다.
        // 인자까지 갈라지면 6번이 건 상한과 실제 등록 상한의 근거가 어긋난다.
        long capacity = AdmissionDecider.queueCapacity(state, MAX_ETA_SEC);
        return queue.enqueue(couponId, memberId, capacity, clock.instant())
                // **여기까지만 열어 준다.** 뒤에 붙이면 줄에 선 사람이 응답을
                // 못 써서 뒷단까지 가고, 자리를 쥔 채로 재고까지 먹는다.
                .onErrorResume(e -> {
                    // **요청마다 안 찍는다.** 레디스 장애는 순간이 아니라 구간으로
                    // 오므로, 여기서 찍으면 초당 수천 줄이 스택트레이스째 쌓인다.
                    //
                    // 대신 예외 종류를 라벨로 센다. 레디스가 죽은 것과 인자가
                    // 틀린 것은 다르게 다뤄야 하는데, 한 숫자로는 못 가린다.
                    count("enqueue-error", FailureCause.of(e));
                    return Mono.empty();
                })
                .switchIfEmpty(Mono.defer(() ->
                        failOpen(exchange, chain, meta, couponId).then(Mono.empty())))
                .flatMap(entry -> {
                    // 이 노드가 방금 이 쿠폰의 줄을 봤다. 다음 창의 신규 유입이
                    // 여기 선 사람을 넘지 않게 한 구간 붙잡는다.
                    //
                    // **거절도 관측이다.** 상한에 걸렸다는 것은 그 줄이 가득
                    // 찼다는 뜻이다. 여기서 안 찍으면 줄이 차는 순간 래치가
                    // 표식을 못 받고, 사다리 4번이 켜져 이 노드가 fail-open 으로
                    // 뒤집힌다 — 방금 줄 선 사람을 전원이 추월한다.
                    //
                    // **스냅샷이 줄을 보고 있어도 찍는다.** 그 스냅샷은 방금 넣은
                    // 이 사람을 아직 모른다 — 다음 판에 줄이 다 빠져 한산으로
                    // 뒤집히면 그 사람이 통째로 추월당한다. 계획서가 "줄이 보이면
                    // 바로 풀어도 된다" 고 적은 것은 그 한 명을 안 센 것이다.
                    latch.mark(couponId, clock.instant().getEpochSecond());
                    // 등록이 다시 되면 fail-open 구간이 끝난 것이다. 쌍으로 안
                    // 남기면 로그에 진입만 있고 언제 닫혔는지가 없다 (LG-2).
                    failOpenWindow.exited().ifPresent(r -> log.info(
                            "fail-open 해제 — {}초 동안 {}건 통과시켰다",
                            NANOSECONDS.toSeconds(r.elapsedNanos()), r.swallowed()));
                    if (!entry.accepted()) {
                        // 2차 방어에 걸렸다. 판정은 자리가 있다고 봤지만 실제로는 없다.
                        count(AdmissionDecision.REJECT_QUEUE_FULL.name());
                        return error.write(exchange, ApiError.Code.QUEUE_FULL,
                                retryAfterSec(AdmissionDecision.REJECT_QUEUE_FULL, random));
                    }
                    double etaSec = EtaPolicy.etaSec(entry.rank(), state.credit());
                    return waiting.waiting(exchange,
                            tokens.issue(couponId, memberId, clock.instant()),
                            entry.rank(), EtaPolicy.reportSec(etaSec),
                            state.mode().name(),
                            POLL.intervalSec(etaSec, random));
                });
    }

    /**
     * 줄을 못 세웠다.
     *
     * <p><b>상한을 두고 열어 준다.</b> 전부 막으면 레디스 장애가 곧 전면 장애이고,
     * 전부 열면 뒷단이 그대로 무너진다. 상한을 넘은 몫은 되돌려 보낸다.
     */
    private Mono<Void> failOpen(ServerWebExchange exchange, GatewayFilterChain chain,
            SnapshotMeta meta, String couponId) {
        // **판정과 같은 리미터·같은 키다.** 따로 들면 한 초에 두 예산이 겹쳐
        // 나가고, 리미터를 하나로 두라는 규칙이 막으려던 버스트가 그대로 난다.
        long cap = (long) (AdmissionDecider.globalCap(meta) * FAIL_OPEN_SHARE);
        if (limiter.tryAcquire(AdmissionDecider.GLOBAL_KEY, cap,
                clock.instant().getEpochSecond())) {
            // 매 요청 찍으면 정작 조사가 필요한 순간에 묻힌다. 구간의 시작만 찍는다.
            if (failOpenWindow.entered()) {
                log.warn("fail-open 진입 — 줄 등록이 안 돼 통과시킨다, 상한={}", cap);
            }
            count("enqueue-failed-open");
            // **연 예산이 곧 격벽의 밑변이다.** 여기서 0 을 넘기면 최소 배수
            // 속도로 떨어져, 상한을 두고 연 몫의 대부분이 격벽에서 다시 막힌다.
            return forward(exchange, chain, couponId, cap, meta);
        }
        count("enqueue-failed-shed");
        return error.write(exchange, ApiError.Code.TEMPORARILY_UNAVAILABLE,
                retryAfterSec(AdmissionDecision.REJECT_OVERLOAD, random));
    }

    /**
     * 거절의 봉투. <b>전부 열거한다</b> — 빠짐없이 적어야 새 판정값이 생겼을 때
     * 컴파일이 깨진다. {@code default} 로 두면 새 사유가 조용히 매진으로 나간다.
     */
    static ApiError.Code codeOf(AdmissionDecision decision) {
        return switch (decision) {
            case REJECT_SOLD_OUT -> ApiError.Code.SOLD_OUT;
            case REJECT_QUEUE_FULL -> ApiError.Code.QUEUE_FULL;
            case REJECT_OVERLOAD -> ApiError.Code.TEMPORARILY_UNAVAILABLE;
            // 차례가 온 사람을 큐 뒤로 안 돌린다. 되돌리면 허가가 "아마도" 가 된다.
            case RETRY_TOKEN -> ApiError.Code.RETRY_TOKEN;
            case PASS_TOKEN, PASS_BYPASS, PASS_FAIL_OPEN, PASS_UNDER_CAP,
                 ENQUEUE_STALE, ENQUEUE_ALWAYS, ENQUEUE_BACKLOG,
                 ENQUEUE_RATE_COUPON, ENQUEUE_RATE_GLOBAL, ENQUEUE_KEY_SATURATED ->
                    throw new IllegalArgumentException("거절이 아니다: " + decision);
        };
    }

    /**
     * 다시 와도 되는 때. <b>같은 값을 주면 다 같이 돌아온다</b> — 흔들어서
     * 되돌아오는 파도를 흩는다.
     */
    static int retryAfterSec(AdmissionDecision decision, DoubleSupplier random) {
        return switch (decision) {
            // 차례가 온 사람이다. 멀리 보내면 그 사이 몫이 남에게 간다.
            //
            // 가장 가까운 밴드(1초)라 흔들림은 반올림에 통째로 흡수된다. 여기서
            // 흩을 대상은 몇 초 뒤에 몰릴 사람들이 아니라 30초 뒤의 파도다.
            case RETRY_TOKEN -> (int) POLL.intervalSec(0, random);
            case REJECT_QUEUE_FULL, REJECT_OVERLOAD ->
                    (int) POLL.intervalSec(EtaPolicy.UNKNOWN, random);
            // 매진은 안 싣는다. 다시 와도 소용없는데 시각을 주면 재시도를 부른다.
            case REJECT_SOLD_OUT -> ApiError.NO_RETRY;
            case PASS_TOKEN, PASS_BYPASS, PASS_FAIL_OPEN, PASS_UNDER_CAP,
                 ENQUEUE_STALE, ENQUEUE_ALWAYS, ENQUEUE_BACKLOG,
                 ENQUEUE_RATE_COUPON, ENQUEUE_RATE_GLOBAL, ENQUEUE_KEY_SATURATED ->
                    throw new IllegalArgumentException("거절이 아니다: " + decision);
        };
    }

    /**
     * <b>사유를 나누지 않는다.</b> 없는 토큰과 만료된 토큰을 갈라 주면 어느 쪽을
     * 고쳐야 하는지 알려 주는 셈이다.
     */
    private boolean hasEntryToken(ServerWebExchange exchange, String couponId) {
        String presented = exchange.getRequest().getHeaders().getFirst(ENTRY_TOKEN);
        String memberId = exchange.getRequest().getHeaders().getFirst(MEMBER_ID);
        // **토큰이 가리키는 사람과 같아야 한다.** 안 보면 남의 토큰을 주워 와도
        // 통하고, 발급은 주워 온 사람 앞으로 나간다.
        return entryTokens.verify(presented, couponId, clock.instant())
                .filter(owner -> owner.equals(memberId))
                .isPresent();
    }

    /**
     * 뒷단으로 넘긴다. <b>통과하는 모든 길이 여기를 지난다.</b>
     *
     * <p>한 갈래만 키를 실으면 나머지에서는 클라이언트가 준 값이 그대로 뒷단에
     * 닿는다. 그러면 매 시도 다른 값을 넣어 멱등성을 우회하거나, 남의 키를 주워
     * 먼저 태워 그 사람의 진짜 시도를 재생으로 버리게 만들 수 있다.
     */
    private Mono<Void> forward(ServerWebExchange exchange, GatewayFilterChain chain,
            String couponId, long ratePerSec, SnapshotMeta meta) {
        HttpHeaders headers = exchange.getRequest().getHeaders();
        String memberId = headers.getFirst(MEMBER_ID);
        if (memberId == null) {
            // 신원 필터가 앞에서 막으므로 여기 오면 배선이 바뀐 것이다.
            // "null" 로 뭉개면 전원이 같은 키를 받아 서로의 발급을 지운다.
            return error.write(exchange, ApiError.Code.INVALID_REQUEST);
        }
        // **초당 건수로는 못 막는 것이 있다.** 초당 100건이어도 각각 10초 걸리면
        // 동시 1,000건이다. 느려진 뒷단이 커넥션을 다 붙잡으면 한산한 쿠폰의
        // 통과 경로까지 같이 죽는다.
        if (!bulkhead.tryEnter(couponId, inFlightCap(ratePerSec, meta))) {
            count("bulkhead-full");
            return shed(exchange);
        }
        String key = idempotency.of(couponId, memberId,
                headers.getFirst(IdempotencyKey.HEADER));
        // 뒷단으로 넘어가는 건이 생겼으면 끊던 구간이 끝난 것이다. 쌍으로 안
        // 남기면 로그에 진입만 있고 언제 닫혔는지가 없다 (LG-2).
        shedWindow.exited().ifPresent(r -> log.warn(
                "보호 차단 해제 — {}초 동안 {}건 끊었다",
                NANOSECONDS.toSeconds(r.elapsedNanos()), r.swallowed()));
        return chain.filter(exchange.mutate()
                        .request(r -> r.headers(h -> h.set(IdempotencyKey.HEADER, key)))
                        .build())
                // **안 끝나는 요청을 끝내 준다.** `doFinally` 는 끝나는 것만
                // 돌려주지 끝나지 않는 것을 끝내지 못한다. 멈춘 뒷단 하나가 그
                // 쿠폰의 격벽을 영구히 닫는 것을 이 상한이 막는다.
                //
                // 뒷단 응답 타임아웃(6.2)과는 다른 자리다. 그쪽은 응답을 얼마나
                // 기다릴지이고, 여기는 자리를 얼마나 쥐고 있게 둘지다.
                .timeout(MAX_IN_FLIGHT)
                // 헤더가 이미 나간 뒤라면 ApiError 가 조용히 비켜선다 — 그
                // 판단을 여기서 한 번 더 하면 두 곳이 갈릴 수 있다.
                .onErrorResume(TimeoutException.class, e -> {
                    count("bulkhead-timeout", "timeout");
                    return shed(exchange);
                })
                // **어느 쪽으로 끝나도 돌려준다.** 안 돌려주면 격벽이 한 번 차고
                // 나서 영영 안 열리고, 그 쿠폰은 뒷단이 멀쩡해져도 계속 막힌다.
                .doFinally(signal -> bulkhead.exit(couponId));
    }

    /**
     * 보호 장치가 끊는다. <b>판정도 같이 고쳐 적는다.</b>
     *
     * <p>사다리가 통과라고 적어 둔 값을 그대로 두면, 응답을 쓰는 쪽과 뒤이어
     * 읽는 계층에는 이 요청이 통과로 보인다. 실제로 나가는 것은 503 이다.
     */
    private Mono<Void> shed(ServerWebExchange exchange) {
        // 매 요청 찍으면 정작 조사가 필요한 순간에 묻힌다. 구간의 시작만 찍는다.
        if (shedWindow.entered()) {
            log.warn("보호 차단 진입 — 뒷단이 못 받아 끊는다");
        }
        // **차례가 온 사람은 가까운 밴드로 부른다.** 그는 이미 줄에서 빠졌고
        // 손에 든 것은 수명이 있는 입장 토큰뿐이다. 30초 뒤로 보내면 그 사이
        // 그의 몫이 남에게 가고, 토큰이 죽으면 줄 맨 뒤로 다시 선다.
        // 폴백이 같은 장애에 쓰는 갈래와 같아야 한다 (BackendFallback).
        boolean hasToken = exchange.<AdmissionDecision>getAttribute(DECISION)
                == AdmissionDecision.PASS_TOKEN;
        exchange.getAttributes().put(DECISION, AdmissionDecision.REJECT_OVERLOAD);
        return error.write(exchange, ApiError.Code.TEMPORARILY_UNAVAILABLE,
                (int) POLL.intervalSec(hasToken ? 0 : EtaPolicy.UNKNOWN, random));
    }

    /**
     * 이 쿠폰이 동시에 걸어 둘 수 있는 건수.
     *
     * <p>이 통과를 낸 <b>초당 예산</b>에 한 건이 걸려 있을 수 있는 시간을 곱한다.
     * 예산이 줄면 격벽도 같이 조여진다 (6.3.3).
     *
     * <p><b>예산이 0 인 구간에는 폴백을 쓴다.</b> 재료가 아직 없는 기동 직후가
     * 그렇고, 0 을 상한으로 쓰면 전면 차단이다 — 등록 경로와 같은 폴백이다.
     */
    private long inFlightCap(long ratePerSec, SnapshotMeta meta) {
        long perSecond = ratePerSec > 0 ? ratePerSec : AdmissionDecider.MIN_CREDIT;
        // **재료에 실려 온 값을 먼저 본다** (P-1). 배포 없이 되돌릴 수 있어야
        // 롤백이 성립하고, 그 전파 경로가 스냅샷이다.
        long seconds = meta.tunables() == null
                ? MAX_IN_FLIGHT_SEC
                : meta.tunables().inFlightSeconds();
        // **곱이 넘치면 음수가 되고, 음수 상한은 전면 차단이다.** 예산은 밖에서
        // 오는 globalCredit 에서 나오므로 여기서 막는다.
        return perSecond > Long.MAX_VALUE / seconds
                ? Long.MAX_VALUE
                : perSecond * seconds;
    }

    private String pathVariable(ServerWebExchange exchange) {
        Map<String, String> vars = exchange.getAttribute(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE);
        return vars == null ? null : vars.get(COUPON_ID);
    }

    @Override
    public String toString() {
        return "Admission";
    }
}

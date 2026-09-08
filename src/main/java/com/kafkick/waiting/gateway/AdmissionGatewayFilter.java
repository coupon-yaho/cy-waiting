package com.kafkick.waiting.gateway;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.kafkick.waiting.control.FailureWindow;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.admission.AdmissionRequest;
import com.kafkick.waiting.domain.admission.Bulkhead;
import com.kafkick.waiting.domain.admission.CouponKeys;
import com.kafkick.waiting.domain.admission.EnqueueLatch;
import com.kafkick.waiting.control.PassRateSource;
import com.kafkick.waiting.domain.admission.PassRateMeter;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpHeaders;
import java.time.Clock;
import java.time.Instant;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import reactor.core.publisher.SignalType;

/**
 * 발급 요청을 통과·대기·거절로 가른다. <b>판정 재료는 로컬 스냅샷에서만 읽는다</b>
 * — 요청마다 레디스를 치면 제어 평면을 만든 이유가 사라진다.
 *
 * <p><b>스스로 안 걸린다.</b> 라우트가 이 인스턴스를 받아 붙인다 — 이름으로 적으면
 * 안 풀렸을 때 기동은 되고 판정만 사라진다.
 */
@Component
public final class AdmissionGatewayFilter implements GatewayFilter, PassRateSource {

    /** 응답을 쓰는 쪽이 읽는다. 다시 판정하면 두 번 세고 답이 갈릴 수 있다. */
    public static final String DECISION = "waiting.admission.decision";

    /**
     * 이 요청을 판정한 회차의 전역 폴링 배수.
     *
     * <p><b>속성으로 넘긴다.</b> 폴백은 서킷을 지나 나중에 도는 자리라 홀더를
     * 다시 읽으면 다른 회차의 값이 나간다 — 같은 장애에 두 값이 나가는 것이다.
     */
    public static final String POLL_SCALE = "waiting.admission.poll-scale";

    /**
     * 이 요청을 <b>재료를 갖고 판정했는가</b>. 게이트웨이의 품질 지표는 성공 응답
     * 비율이 아니라 판정 성공률이고, 그 지표가 이 값만 읽는다.
     *
     * <p>운영 카운터를 더해 만들지 않는다 — 한 요청이 여러 사유를 지나면
     * 실패율이 100% 를 넘고, 라벨을 리네임하면 그 항이 조용히 빠진다.
     */
    public static final String JUDGEMENT = "waiting.judgement";

    /** 재료 없이 판정한 요청. {@link #JUDGEMENT} 가 이 표시를 읽는다. */
    private static final String DEGRADED = "waiting.judgement.degraded";

    /** 낡은 재료에서만 나오는 판정. 사다리 4·7번의 결과다. */
    private static final Set<AdmissionDecision> STALE_DECISIONS = EnumSet.of(
            AdmissionDecision.PASS_FAIL_OPEN,
            AdmissionDecision.ENQUEUE_STALE,
            AdmissionDecision.REJECT_OVERLOAD);

    private static final Logger log = LoggerFactory.getLogger(AdmissionGatewayFilter.class);

    /** 경로 변수 이름. **관찰자도 이것을 읽는다** — 갈리면 담는 키와 읽는 키가 갈린다. */
    static final String COUPON_ID = "couponId";

    /** 판정 결과를 사유별로 센다. <b>요청마다 로그를 남기지 않는다</b> — 낡음
     * 구간에서 로그가 폭주하고, 그때 정작 봐야 할 것이 묻힌다. */
    private static final String METRIC = "waiting.admission";

    /**
     * 등록에서 순번 바닥값이 걸린 횟수. 순번 역행의 선행 신호다 — 이 값이 오르는
     * 구간은 줄 선 사람을 추월시키지 않는다는 약속이 방어 하나에 걸린 구간이다.
     *
     * <p>판정 카운터에 안 섞는다. 품질 지표의 분모가 그 이름들의 합이라 한 요청이
     * 두 번 세어지면 분모가 부풀고 실패율이 좋아 보인다.
     */
    private static final String CLOCK_BACK = "waiting.queue.clock.back";

    /** 받아도 되는 최대 대기 시간. 넘으면 줄을 세우는 것이 되레 나쁘다. */
    static final long MAX_ETA_SEC = 600;

    /** 재시도 안내의 흔들림 폭. 폴링 간격과 같은 정책을 쓴다. */
    private static final PollIntervalPolicy POLL = PollIntervalPolicy.of(PollIntervalPolicy.NORMAL_JITTER_RATIO);

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
     * 한 건이 뒷단에 걸려 있을 수 있는 시간(초). 상한은 초당 예산 × 이 값이다.
     *
     * <p>유입은 같은 예산이 이미 조이므로 걸려 있는 수는 <b>예산 × 지연</b>이고,
     * 이 값이 곧 격벽이 막기 시작하는 지연이다. 서킷의 느림 임계보다 커야
     * 느린 뒷단이 서킷에 집계된 뒤에 막힌다 — 나중에 튜너블로 뺀다.
     */
    private static final long MAX_IN_FLIGHT_SEC = 3;

    /**
     * 자리를 놓게 하는 상한의 여유 배수.
     *
     * <p>뒷단 응답 상한보다 뒤여야 한다. 여기가 먼저 끊으면 서킷에 가는 것이
     * 오류가 아니라 취소가 되고, 취소는 창에 안 쌓여 멎은 뒷단의 서킷이 영영
     * 안 열린다. 여기는 그 상한이 안 걸렸을 때의 마지막 그물이다.
     */
    private static final long IN_FLIGHT_GRACE = 5;

    /** 격벽이 막기 시작하는 지연. 시험이 5 를 손으로 나누지 않게 여기서 낸다. */
    static final Duration BLOCKING_DELAY = Duration.ofSeconds(MAX_IN_FLIGHT_SEC);

    /** 자리를 놓게 하는 시한. 시험이 손으로 베끼지 않게 여기서 한 번만 정한다. */
    static final Duration MAX_IN_FLIGHT =
            Duration.ofSeconds(MAX_IN_FLIGHT_SEC * IN_FLIGHT_GRACE);

    private final SnapshotHolder holder;
    private final AdmissionDecider decider;

    /** 뒷단 서킷의 상태. <b>판정의 입력이다</b> — 안 보면 half-open 회복을 방해한다. */
    private final CircuitStateReader circuit;
    private final Clock clock;
    private final MeterRegistry meters;

    /** 이 노드가 뒷단으로 보낸 초당 수. 회복 봉우리를 재려고 리더가 합산한다. */
    private final PassRateMeter passRate = PassRateMeter.of(PassRateMeter.DEFAULT_WINDOW_MS);
    private final DoubleSupplier random;
    private final QueuePort queue;
    private final QueueToken tokens;
    private final EntryToken entryTokens;
    private final SecondWindowLimiter limiter;
    private final EnqueueLatch latch;

    /** 뒷단의 멱등성이 작동할 근거. 같은 시도에 같은 값을 준다. */
    private final IdempotencyKey idempotency;

    /** 동시에 걸려 있는 건수를 센다. 리미터가 세는 초당 건수와 단위가 다르다. */
    private final Bulkhead bulkhead = Bulkhead.withMaxKeys(CouponKeys.MAX);
    private final ApiError error;

    /** 판정값을 응답 코드와 다시 올 시각으로 옮긴다. */
    private final Rejection rejection = Rejection.standard();
    private final QueueResponse waiting = QueueResponse.create();

    /** 설정 오류를 한 번만 알린다. 라우트가 틀렸으면 늘 틀리다. */
    private final AtomicBoolean misconfigured = new AtomicBoolean();

    /**
     * fail-open 구간의 진입과 해제를 쌍으로 남긴다.
     *
     * <p>이 전이가 로그에 없으면 사후에 <b>추월이 언제 열렸는지</b>를 못 짚는다.
     * 지표는 초 단위로 뭉개져 남고 보존 기간도 짧아, 사고 조사에서 필요한
     * "몇 시 몇 분에 열려 얼마나 갔는가" 를 답하지 못한다.
     */
    private final FailureWindow failOpenWindow;

    /**
     * 보호 장치가 끊는 구간. 진입과 해제를 쌍으로 남긴다.
     *
     * <p>카운터만 두면 사후에 "몇 시부터 몇 시까지, 몇 건을 끊었나" 를 못 답한다.
     */
    private final FailureWindow shedWindow;

    /** 재료가 아직 재고를 말하는 창에서 이것이 유일한 근거다. */
    private final SoldOutCache soldOutCache;

    /**
     * 캐시가 끊은 건수. {@code cause} 축에 안 싣는다 — 그 축은 실패 원인의 닫힌
     * 집합이라, 판정 출처를 넣으면 정상적인 매진 단락이 실패율에 잡힌다.
     */
    private final Counter soldOutHits;

    /** 레디스 시계가 뒤로 간 건수. 순번 역행의 선행 신호다. */
    private final Counter clockBack;

    private AdmissionGatewayFilter(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters, DoubleSupplier random,
            QueuePort queue, QueueToken tokens, SecondWindowLimiter limiter,
            EntryToken entryTokens, IdempotencyKey idempotency, LongSupplier ticker,
            SoldOutCache soldOutCache, CircuitStateReader circuit) {
        // **안 주면 안 보는 것으로 친다.** 모른다고 줄로 보내면 서킷을 안 붙인
        // 배치에서 전 요청이 큐로 간다 — 없는 장애를 만든다.
        this.circuit = circuit == null ? CircuitStateReader.of(null, "") : circuit;
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
        this.latch = EnqueueLatch.covering(CouponKeys.MAX, holder.dataStaleAfter());
        this.idempotency = Objects.requireNonNull(idempotency, "idempotency 는 필수다");
        // **만들어 두고 안 걸면 지표가 안 나온다.** 격벽이 차오르는 중인지는
        // 막힌 뒤에야 오르는 카운터로는 못 본다.
        BulkheadMetrics.bind(bulkhead, meters);
        this.error = ApiError.of(clock);
        this.soldOutCache = Objects.requireNonNull(soldOutCache, "soldOutCache 는 필수다");
        this.soldOutHits = meters.counter("waiting.soldout.cache.hit");
        // 여기서 만들어 0 을 내보낸다. 첫 증가 때 만들면 그 앞에 0 표본이 없어
        // 프로메테우스가 increase 를 못 내고, 드문 첫 사건이 통째로 사라진다.
        this.clockBack = meters.counter(CLOCK_BACK);
    }

    /**
     * 흔들림의 난수원은 스레드마다 따로 둔다 — 공유하면 그 자체가 경합점이다.
     *
     * <p>캐시는 주입받는다. 여기서 만들면 담는 쪽과 읽는 쪽이 갈려, 뒷단이 낸
     * 매진을 판정이 영영 못 본다.
     */
    @Autowired
    AdmissionGatewayFilter(SnapshotHolder holder, AdmissionDecider decider, Clock clock,
            MeterRegistry meters, QueuePort queue, QueueToken tokens,
            SecondWindowLimiter limiter, EntryToken entryTokens,
            IdempotencyKey idempotency, SoldOutCache soldOutCache,
            ObjectProvider<CircuitStateReader> circuit) {
        this(holder, decider, clock, meters,
                () -> ThreadLocalRandom.current().nextDouble(), queue, tokens, limiter,
                entryTokens, idempotency, System::nanoTime, soldOutCache,
                // **배분과 같은 것을 쓴다.** 각자 만들면 판정은 열렸다고 보는데
                // 배분은 아니라고 보는 구간이 생긴다.
                circuit.getIfAvailable());
    }

    /**
     * <b>이 필터만 쓰는 캐시</b>를 안에서 만든다. 아무도 안 담으므로 아무것도
     * 안 막는다 — 배선이 이쪽을 고르면 매진 보호가 신호 없이 사라진다.
     */
    public static AdmissionGatewayFilter withIsolatedSoldOutCache(SnapshotHolder holder,
            AdmissionDecider decider,
            Clock clock, MeterRegistry meters, QueuePort queue, QueueToken tokens,
            SecondWindowLimiter limiter, EntryToken entryTokens,
            IdempotencyKey idempotency) {
        return new AdmissionGatewayFilter(holder, decider, clock, meters,
                () -> ThreadLocalRandom.current().nextDouble(), queue, tokens, limiter,
                entryTokens, idempotency, System::nanoTime, SoldOutCache.standard(), null);
    }

    /** 매진 캐시를 함께 받는다. 담는 쪽과 읽는 쪽이 같은 것이라야 한다. */
    public static AdmissionGatewayFilter of(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters, QueuePort queue, QueueToken tokens,
            SecondWindowLimiter limiter, EntryToken entryTokens,
            IdempotencyKey idempotency, SoldOutCache soldOutCache) {
        return new AdmissionGatewayFilter(holder, decider, clock, meters,
                () -> ThreadLocalRandom.current().nextDouble(), queue, tokens, limiter,
                entryTokens, idempotency, System::nanoTime, soldOutCache, null);
    }

    /** 난수원을 받는다. 고정하지 못하면 흔들림이 실제로 붙었는지 못 잰다. */
    public static AdmissionGatewayFilter withIsolatedSoldOutCache(SnapshotHolder holder,
            AdmissionDecider decider,
            Clock clock, MeterRegistry meters, DoubleSupplier random,
            QueuePort queue, QueueToken tokens, SecondWindowLimiter limiter,
            EntryToken entryTokens, IdempotencyKey idempotency) {
        return new AdmissionGatewayFilter(holder, decider, clock, meters, random, queue,
                tokens, limiter, entryTokens, idempotency, System::nanoTime,
                SoldOutCache.standard(), null);
    }

    /**
     * 구간 시계를 받는다. <b>요청 시계와 따로다</b> — 구간 길이는 단조 시계로 재야
     * NTP 가 시각을 되돌릴 때 음수가 안 된다.
     *
     * <p>고정하지 못하면 fail-open 이 얼마나 이어졌는지를 재는 계산 자체가
     * 시험에서 늘 0 이 되어, 단위를 틀려도 통과한다.
     */
    public static AdmissionGatewayFilter withIsolatedSoldOutCache(SnapshotHolder holder,
            AdmissionDecider decider,
            Clock clock, MeterRegistry meters, DoubleSupplier random,
            QueuePort queue, QueueToken tokens, SecondWindowLimiter limiter,
            EntryToken entryTokens, IdempotencyKey idempotency, LongSupplier ticker) {
        return new AdmissionGatewayFilter(holder, decider, clock, meters, random, queue,
                tokens, limiter, entryTokens, idempotency, ticker, SoldOutCache.standard(),
                null);
    }

    /**
     * 지금 뒷단에 걸려 있는 건수.
     *
     * <p>종료할 때 이 값이 0 이 되기를 기다린다 — 안 되면 그만큼이 강제 종료로
     * 끊긴다. 격벽이 세는 값이라 뒷단으로 넘어간 것만 센다.
     */
    public int inFlight() {
        return bulkhead.inFlight();
    }

    /**
     * 이 노드가 최근에 뒷단으로 보낸 초당 수. <b>시계를 안 받는다</b> — 세는
     * 쪽과 읽는 쪽이 다른 시계를 쓰면 창이 매번 즉시 접힌다.
     */
    @Override
    public long passRatePerSec() {
        return passRate.perSecond(clock.millis());
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
     * 터지고, 프로메테우스를 붙이는 순간 터진다.
     */
    private void count(String outcome, String cause) {
        meters.counter(METRIC, "outcome", outcome, "cause", cause).increment();
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // **품질은 요청마다 한 번만 센다.** 운영 카운터를 여럿 더해 만들면 한
        // 요청이 여러 번 세어져 실패율이 100% 를 넘고, 라벨 하나를 리네임하면
        // 그 항이 조용히 빠진다.
        return judge(exchange, chain)
                .doFinally(signal -> meters.counter(JUDGEMENT, "quality",
                        exchange.getAttributeOrDefault(DEGRADED, false)
                                ? "degraded" : "fresh").increment());
    }

    private Mono<Void> judge(ServerWebExchange exchange, GatewayFilterChain chain) {
        String couponId = pathVariable(exchange);
        if (couponId == null) {
            // 라우트에서 변수 이름을 빼면 판정이 사라진 채로 기동만 성공한다.
            // 설정 오류라 늘 틀리므로 한 번만 알린다 — 요청마다 찍으면 로그 폭주다.
            if (misconfigured.compareAndSet(false, true)) {
                log.error("라우트에 {} 경로변수가 없다 — 판정할 대상을 못 정한다", COUPON_ID);
            }
            count("no-path-variable");
            degraded(exchange);
            return error.write(exchange, ApiError.Code.INVALID_REQUEST);
        }

        SnapshotHolder.View view = holder.view();
        // 분기 전에 심는다. 뒤에 두면 판정을 못 거치는 갈래(첫 틱 전·낡은 재료)가
        // 배수 없이 나가고, 폴백은 못 찾아 1.0 으로 답해 같은 장애에 두 값이 된다.
        exchange.getAttributes().put(POLL_SCALE, view.snapshot().meta().pollScale());
        CouponState state = view.snapshot().coupons().get(couponId);
        if (state == null) {
            return unknownCoupon(exchange, chain, view, couponId);
        }

        // **지금 시각이다.** 스냅샷 발행 시각을 넘기면 배분이 멎는 순간 윈도가
        // 영영 안 넘어가고, 상한만큼 쓴 뒤부터 전부 막힌다.
        long nowSec = clock.instant().getEpochSecond();
        releaseIfRestocked(couponId, state, view);
        if (soldOutCache.soldOut(couponId)) {
            AdmissionDecision cached = AdmissionDecision.REJECT_SOLD_OUT;
            exchange.getAttributes().put(DECISION, cached);
            count(cached.name());
            soldOutHits.increment();
            return route(exchange, chain, cached, couponId, state, view.snapshot().meta());
        }
        AdmissionDecision decision = decider.decide(new AdmissionRequest(
                couponId, state, view.snapshot().meta(),
                // 스냅샷은 한 틱 늦어 아직 한산하다고 말한다 — 래치가 없으면 다음
                // 창의 신규 유입이 방금 선 사람을 넘고, 입장 토큰을 안 보면 줄과
                // 무관하게 통과해 기다린 사람과 안 기다린 사람이 같아진다.
                holder.isDataStale(view), hasEntryToken(exchange, couponId), 
                latch.latched(couponId, nowSec),
                // **서킷을 여기서 읽는다.** 메모리 안의 값이라 왕복이 없다.
                // 안 실으면 판정이 서킷을 영영 안 보고, 열린 동안 계속 통과를
                // 내 전량이 fallback 으로 간다.
                nowSec, MAX_ETA_SEC, circuit.now()));
        exchange.getAttributes().put(DECISION, decision);
        count(decision.name());
        // **낡은 재료로 내린 판정도 재료 없이 판정한 것이다.** 사다리 4·7번이
        // 그 자리다 — 스냅샷에 있는 쿠폰은 deferred-* 를 안 지나므로, 여기서
        // 표시하지 않으면 스냅샷이 멎은 구간이 통째로 성공으로 잡힌다.
        if (STALE_DECISIONS.contains(decision)) {
            degraded(exchange);
        }
        return route(exchange, chain, decision, couponId, state, view.snapshot().meta());
    }

    /**
     * 관찰보다 나중에 발행된 재료가 재고를 말하면 푼다.
     *
     * <p>낡은 재료로는 안 푼다. 집행은 낡음을 견디는데 해제만 허용하면 비대칭이다.
     * 재고를 모르는 재료로도 안 푼다 — 해제의 근거는 재입고를 본 것인데, 못 읽은
     * 것은 본 것이 아니다. 재고 키를 잃는 것은 뒷단이 409 를 내는 것과 같이 오므로
     * 하필 그때 방패가 매 틱 열린다.
     */
    private void releaseIfRestocked(String couponId, CouponState state,
            SnapshotHolder.View view) {
        if (state.soldOut() || !state.stockKnown() || holder.isDataStale(view)) {
            return;
        }
        soldOutCache.restocked(couponId, view.snapshot().publishedAt())
                // **진입과 쌍으로 남긴다.** 쿠폰 ID 는 카디널리티가 높아 지표
                // 라벨로 못 쓰므로, 어느 쿠폰이 몇 초 동안 몇 건을 끊었는지는
                // 로그만이 답할 수 있는 자리다.
                .ifPresent(r -> log.info("매진 해제 — 쿠폰 {} 를 {}초 동안 끊었고 {}건이었다",
                        couponId, r.elapsed().toSeconds(), r.blocked()));
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
            degraded(exchange);
            markToken(exchange, couponId);
            // 재료가 없어 크레딧을 모른다. 폴백으로 최소 배수 속도를 가정한다.
            return forward(exchange, chain, couponId, 0, view.snapshot().meta());
        }
        // **모른다는 것이 무제한의 사유는 아니다.** 사다리 4번은 같은 무지에서
        // 노드 몫 안에서만 여는데, 여기만 열어 두면 아무 문자열 쿠폰이나 그
        // 상한 밖으로 나간다. 같은 예산에 태운다.
        if (holder.isDataStale(view)) {
            count("deferred-stale-material");
            degraded(exchange);
            markToken(exchange, couponId);
            return failOpen(exchange, chain, view.snapshot().meta(), couponId);
        }
        count("unknown-coupon");
        return error.write(exchange, ApiError.Code.UNKNOWN_COUPON);
    }

    private Mono<Void> route(ServerWebExchange exchange, GatewayFilterChain chain,
            AdmissionDecision decision, String couponId, CouponState state, SnapshotMeta meta) {
        if (decision.isPass()) {
            // **판정이 쓴 예산을 그대로 받는다.** 여기서 credit 을 다시 꺼내면
            // 한산 통과가 0 을 받고, 0 은 상한으로 쓰이는 순간 전면 차단이다.
            return forward(exchange, chain, couponId,
                    decider.admittedRatePerSec(decision, state, meta), meta);
        }
        if (decision.isEnqueue()) {
            return enqueue(exchange, chain, couponId, state, meta);
        }
        return error.write(exchange, rejection.code(decision),
                rejection.retryAfterSec(decision, random, meta.pollScale()));
    }

    /**
     * 줄에 세운다. <b>여기가 요청 경로에서 레디스를 치는 유일한 자리다</b> —
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
        // 판정에 쓴 상태를 그대로 쓴다. 다시 읽으면 그 사이 틱이 지나 어긋난다.
        // 상한 함수는 사다리 6번과 일부러 다르고 MAX_ETA_SEC 만 같다 — 인자까지
        // 갈라지면 6번이 건 상한과 실제 등록 상한의 근거가 어긋난다.
        long capacity = decider.queueCapacity(state, MAX_ETA_SEC);
        return queue.enqueue(couponId, memberId, capacity, clock.instant())
                // **여기까지만 열어 준다.** 뒤에 붙이면 줄에 선 사람이 응답을
                // 못 써서 뒷단까지 가고, 자리를 쥔 채로 재고까지 먹는다.
                .onErrorResume(e -> {
                    // 레디스 장애는 구간으로 오므로 요청마다 찍으면 초당 수천 줄이
                    // 쌓인다. 대신 예외 종류를 라벨로 센다 — 레디스가 죽은 것과
                    // 인자가 틀린 것은 한 숫자로 못 가린다.
                    count("enqueue-error", FailureCause.of(e));
                    return Mono.empty();
                })
                .switchIfEmpty(Mono.defer(() ->
                        failOpen(exchange, chain, meta, couponId).then(Mono.empty())))
                .flatMap(entry -> {
                    // 이 노드가 방금 이 쿠폰의 줄을 봤다. 다음 창의 신규 유입이
                    // 여기 선 사람을 넘지 않게 한 구간 붙잡는다.
                    //
                    // 거절이든 스냅샷이 줄을 보고 있든 무조건 찍는다. 스냅샷은
                    // 방금 넣은 이 사람을 아직 모르고, 안 찍으면 사다리 4번이 켜져
                    // fail-open 으로 뒤집혀 그 사람을 전원이 추월한다.
                    latch.mark(couponId, clock.instant().getEpochSecond());
                    if (entry.clockWentBack()) {
                        clockBack.increment();
                    }
                    // 등록이 다시 되면 fail-open 구간이 끝난 것이다. 쌍으로 안
                    // 남기면 로그에 진입만 있고 언제 닫혔는지가 없다.
                    failOpenWindow.exited().ifPresent(r -> log.info(
                            "fail-open 해제 — {}초 동안 {}건 통과시켰다",
                            NANOSECONDS.toSeconds(r.elapsedNanos()), r.swallowed()));
                    if (!entry.accepted()) {
                        // 2차 방어. 판정은 자리가 있다고 봤지만 실제로는 없었다.
                        // 판정 이름으로 안 센다 — 이 요청은 위에서 이미 한 번
                        // 세어졌고, 두 번 세면 SLI 의 분모가 부푼다.
                        count("queue-full-2nd");
                        return error.write(exchange, ApiError.Code.QUEUE_FULL,
                                rejection.retryAfterSec(AdmissionDecision.REJECT_QUEUE_FULL, random,
                                        meta.pollScale()));
                    }
                    double etaSec = EtaPolicy.etaSec(entry.rank(), state.credit());
                    return waiting.waiting(exchange,
                            tokens.issue(couponId, memberId, clock.instant()),
                            entry.rank(), EtaPolicy.reportSec(etaSec),
                            state.mode().name(),
                            // 전역 배수를 여기서도 지킨다. 등록 응답만 배수를
                            // 빼면 방금 줄에 선 사람이 예산 밖에서 두드린다.
                            POLL.intervalSec(etaSec, random, meta.pollScale()),
                            entry.rejoined());
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
        long cap = (long) (decider.globalCap(meta) * FAIL_OPEN_SHARE);
        if (limiter.tryAcquire(AdmissionDecider.GLOBAL_KEY, cap,
                clock.instant().getEpochSecond())) {
            // 매 요청 찍으면 정작 조사가 필요한 순간에 묻힌다. 구간의 시작만 찍는다.
            if (failOpenWindow.entered()) {
                log.warn("fail-open 진입 — 줄 등록이 안 돼 통과시킨다, 상한={}", cap);
            }
            count("enqueue-failed-open");
            degraded(exchange);
            // **연 예산이 곧 격벽의 밑변이다.** 여기서 0 을 넘기면 최소 배수
            // 속도로 떨어져, 상한을 두고 연 몫의 대부분이 격벽에서 다시 막힌다.
            return forward(exchange, chain, couponId, cap, meta);
        }
        count("enqueue-failed-shed");
        return error.write(exchange, ApiError.Code.TEMPORARILY_UNAVAILABLE,
                rejection.retryAfterSec(AdmissionDecision.REJECT_OVERLOAD, random,
                        meta.pollScale()));
    }

    /**
     * <b>404 를 미루고 통과시키는 갈래에서도 차례가 온 사람을 표시한다.</b> 보호
     * 차단과 폴백이 이 값으로 그를 가르는데, 비어 있으면 줄에 안 선 사람으로 읽혀
     * 멀리 밀린다. 토큰이 없으면 안 심는다 — 없는 자격을 지어내면 줄을 건너뛴다.
     */
    private void markToken(ServerWebExchange exchange, String couponId) {
        if (hasEntryToken(exchange, couponId)) {
            exchange.getAttributes().put(DECISION, AdmissionDecision.PASS_TOKEN);
        }
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
        // 자리를 잡기 전에 만든다. 잡은 뒤에 두면 여기서 던지는 순간 반납이 아직
        // 안 걸려 그 자리가 영영 안 돌아온다. 서명 한 번이 더 나가지만, 새는 자리를
        // 손으로 지키는 쪽은 다음에 한 줄이 끼어드는 순간 깨진다.
        String key = idempotency.of(couponId, memberId,
                headers.getFirst(IdempotencyKey.HEADER));
        // 초당 100건이어도 각각 10초 걸리면 동시 1,000건이라 초당 예산만으로는
        // 못 막는다. 노드 전체에도 씌우는 것은 쿠폰별 상한의 합이 안 묶여,
        // 캠페인이 여럿이면 그 합이 노드가 감당할 양을 넘기 때문이다.
        //
        // 라이브러리가 끼워 넣던 격벽이 그 자리를 하고 있었는데, 크기가 25 로
        // 박혀 있어 껐다. 그 몫을 여기서 제 예산으로 다시 세운다.
        if (!bulkhead.tryEnter(couponId, inFlightCap(ratePerSec, meta),
                inFlightCap(decider.globalCap(meta), meta))) {
            count("bulkhead-full");
            return shed(exchange, meta);
        }
        // 뒷단으로 넘어가는 건이 생겼으면 끊던 구간이 끝난 것이다. 쌍으로 안
        // 남기면 로그에 진입만 있고 언제 닫혔는지가 없다.
        shedWindow.exited().ifPresent(r -> log.warn(
                "보호 차단 해제 — {}초 동안 {}건 끊었다",
                NANOSECONDS.toSeconds(r.elapsedNanos()), r.swallowed()));
        // **여기부터 반납이 걸릴 때까지 던질 수 있는 것을 두지 않는다.**
        return chain.filter(exchange.mutate()
                        .request(r -> r.headers(h -> h.set(IdempotencyKey.HEADER, key)))
                        .build())
                // doFinally 는 끝나는 것만 돌려주지 안 끝나는 것을 끝내지 못한다.
                // 멈춘 뒷단 하나가 격벽을 영구히 닫는 것을 이 상한이 막는다.
                // 뒷단 응답 타임아웃과는 다르다 — 여기는 자리를 쥐는 시간이다.
                .timeout(MAX_IN_FLIGHT)
                // 헤더가 이미 나간 뒤라면 ApiError 가 조용히 비켜선다 — 그
                // 판단을 여기서 한 번 더 하면 두 곳이 갈릴 수 있다.
                .onErrorResume(TimeoutException.class, e -> {
                    count("bulkhead-timeout", "timeout");
                    return shed(exchange, meta);
                })
                // **어느 쪽으로 끝나도 돌려준다.** 안 돌려주면 격벽이 한 번 차고
                // 나서 영영 안 열리고, 그 쿠폰은 뒷단이 멀쩡해져도 계속 막힌다.
                .doFinally(signal -> {
                    bulkhead.exit(couponId);
                    // 여기서 센다. 판정 자리에서 세면 서킷이 열린 동안의
                    // 통과 판정까지 들어가는데 그것들은 뒷단에 안 닿는다. 취소는
                    // 뒷단 응답이 왔는지로 가른다 — 받는 중에 끊긴 것은 도착이다.
                    boolean reached = signal != SignalType.CANCEL
                            || exchange.getAttribute(
                                    ServerWebExchangeUtils.CLIENT_RESPONSE_ATTR) != null;
                    if (reached && !Boolean.TRUE.equals(
                            exchange.getAttribute(BackendFallback.NOT_CALLED))) {
                        passRate.passed(clock.millis());
                    }
                });
    }

    /**
     * 재료 없이 판정했다고 표시한다. 세는 것은 끝에서 한 번이다 — 한 요청이
     * 여러 사유를 지날 수 있어 여기서 세면 같은 요청이 여러 번 잡힌다.
     */
    private void degraded(ServerWebExchange exchange) {
        exchange.getAttributes().put(DEGRADED, true);
    }

    /**
     * 보호 장치가 끊는다. 판정도 같이 고쳐 적는다 — 사다리가 적어 둔 통과를
     * 그대로 두면 실제로 503 이 나가는데 뒤에 읽는 쪽에는 통과로 보인다.
     *
     * <p>판정에 쓴 재료를 그대로 받는다. 홀더를 다시 읽으면 시한 갈래는 수 초
     * 뒤에 도는 자리라 판정과 다른 회차의 배수가 나간다.
     */
    private Mono<Void> shed(ServerWebExchange exchange, SnapshotMeta meta) {
        // 매 요청 찍으면 정작 조사가 필요한 순간에 묻힌다. 구간의 시작만 찍는다.
        if (shedWindow.entered()) {
            log.warn("보호 차단 진입 — 뒷단이 못 받아 끊는다");
        }
        // 차례가 온 사람은 가까운 밴드로 부른다. 멀리 보내면 수명 있는 입장 토큰이
        // 죽어 줄 맨 뒤로 다시 선다. 폴백이 같은 장애에 쓰는 갈래와 같아야 한다
        // (BackendFallback).
        boolean hasToken = exchange.<AdmissionDecision>getAttribute(DECISION)
                == AdmissionDecision.PASS_TOKEN;
        exchange.getAttributes().put(DECISION, AdmissionDecision.REJECT_OVERLOAD);
        // **줄에 안 선 쪽만 배수를 지킨다.** 이 갈래가 도는 순간이 곧 예산이
        // 빠듯한 순간이라 거기만 빼면 과부하일수록 예산이 덜 걸린다. 토큰
        // 보유자는 반대다 — 그 순간이 곧 그가 가장 멀리 밀리는 순간이다.
        //
        // **응답 코드는 매핑에서 안 가져온다.** 바로 위에서 판정을 덮어썼으므로
        // 그것으로 코드를 뽑으면 차례가 온 사람도 과부하 거절로 나간다.
        return error.write(exchange, ApiError.Code.TEMPORARILY_UNAVAILABLE,
                rejection.retryAfterSec(hasToken
                                ? AdmissionDecision.RETRY_TOKEN
                                : AdmissionDecision.REJECT_OVERLOAD,
                        random, meta.pollScale()));
    }

    /**
     * 이 쿠폰이 동시에 걸어 둘 수 있는 건수.
     *
     * <p>이 통과를 낸 <b>초당 예산</b>에 한 건이 걸려 있을 수 있는 시간을 곱한다.
     * 예산이 줄면 격벽도 같이 조여진다.
     *
     * <p><b>예산이 0 인 구간에는 폴백을 쓴다.</b> 재료가 아직 없는 기동 직후가
     * 그렇고, 0 을 상한으로 쓰면 전면 차단이다 — 등록 경로와 같은 폴백이다.
     */
    private long inFlightCap(long ratePerSec, SnapshotMeta meta) {
        long perSecond = ratePerSec > 0 ? ratePerSec : AdmissionDecider.MIN_CREDIT;
        // **재료에 실려 온 값을 먼저 본다.** 배포 없이 되돌릴 수 있어야 롤백이
        // 성립하고, 그 전파 경로가 스냅샷이다.
        long seconds = meta.inFlightSecondsOr(MAX_IN_FLIGHT_SEC);
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

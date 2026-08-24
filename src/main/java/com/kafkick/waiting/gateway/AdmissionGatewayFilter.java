package com.kafkick.waiting.gateway;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.admission.AdmissionRequest;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 발급 요청을 통과·대기·거절로 가른다. <b>판정 재료는 로컬 스냅샷에서만 읽는다</b>
 * — 요청마다 레디스를 치면 제어 평면을 만든 이유가 사라진다.
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
    private static final long MAX_ETA_SEC = 600;

    /** 재시도 안내의 흔들림 폭. 폴링 간격과 같은 정책을 쓴다. */
    private static final PollIntervalPolicy POLL = PollIntervalPolicy.of(0.2);

    private static final String MEMBER_ID = "X-Member-Id";

    /**
     * 등록이 안 될 때 흘려보내는 몫. <b>노드 하나의 초당 값이다</b> — 노드가 N 대면
     * 뒷단이 받는 것은 그 N 배이고, 오토스케일이 붙으면 조용히 커진다.
     */
    private static final long FAIL_OPEN_CAP = 200;

    private static final String FAIL_OPEN_KEY = "enqueue";

    private final SnapshotHolder holder;
    private final AdmissionDecider decider;
    private final Clock clock;
    private final MeterRegistry meters;
    private final DoubleSupplier random;
    private final QueuePort queue;
    private final QueueToken tokens;
    private final SecondWindowLimiter failOpen;
    private final ApiError error;
    private final QueueResponse waiting = QueueResponse.create();

    /** 설정 오류를 한 번만 알린다. 라우트가 틀렸으면 늘 틀리다. */
    private final AtomicBoolean misconfigured = new AtomicBoolean();

    private AdmissionGatewayFilter(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters, DoubleSupplier random,
            QueuePort queue, QueueToken tokens) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
        this.decider = Objects.requireNonNull(decider, "decider 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.random = Objects.requireNonNull(random, "random 은 필수다");
        this.queue = Objects.requireNonNull(queue, "queue 는 필수다");
        this.tokens = Objects.requireNonNull(tokens, "tokens 는 필수다");
        this.failOpen = SecondWindowLimiter.withMaxKeys(1);
        this.error = ApiError.of(clock);
    }

    /** 흔들림의 난수원은 스레드마다 따로 둔다 — 공유하면 그 자체가 경합점이다. */
    @Autowired
    AdmissionGatewayFilter(SnapshotHolder holder, AdmissionDecider decider, Clock clock,
            MeterRegistry meters, QueuePort queue, QueueToken tokens) {
        this(holder, decider, clock, meters,
                () -> ThreadLocalRandom.current().nextDouble(), queue, tokens);
    }

    public static AdmissionGatewayFilter of(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters, QueuePort queue, QueueToken tokens) {
        return new AdmissionGatewayFilter(holder, decider, clock, meters, queue, tokens);
    }

    /** 난수원을 받는다. 고정하지 못하면 흔들림이 실제로 붙었는지 못 잰다 (TS-4). */
    public static AdmissionGatewayFilter of(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters, DoubleSupplier random,
            QueuePort queue, QueueToken tokens) {
        return new AdmissionGatewayFilter(holder, decider, clock, meters, random, queue, tokens);
    }

    /**
     * 사유별로 센다. <b>쿠폰 ID 를 라벨에 안 넣는다</b> — 인증이 없어 아무 문자열이나
     * 들어오고, 그러면 지표 하나가 메모리를 밀어낸다.
     */
    private void count(String outcome) {
        meters.counter(METRIC, "outcome", outcome).increment();
    }

    /** 예외 종류는 우리 코드가 정하는 값이라 라벨이 안 폭발한다. */
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

        AdmissionDecision decision = decider.decide(new AdmissionRequest(
                couponId, state, view.snapshot().meta(),
                holder.isDataStale(view), false, false,
                // **지금 시각이다.** 스냅샷 발행 시각을 넘기면 배분이 멎는 순간
                // 윈도가 영영 안 넘어가고, 상한만큼 쓴 뒤부터 전부 막힌다 —
                // 열어 줘야 할 구간에서 정반대로 조인다.
                clock.instant().getEpochSecond(), MAX_ETA_SEC));
        exchange.getAttributes().put(DECISION, decision);
        count(decision.name());
        return route(exchange, chain, decision, couponId, state);
    }

    /**
     * 스냅샷에 없는 쿠폰. <b>여기서 끝내는 것이 레디스 키 무한 생성을 막는다</b> —
     * 그대로 흘리면 아무 문자열이나 큐를 하나씩 만든다.
     */
    private Mono<Void> unknownCoupon(ServerWebExchange exchange, GatewayFilterChain chain,
            SnapshotHolder.View view, String couponId) {
        // 기동 직후 재료가 없다고 전면 404 를 내면 뜨자마자 모든 쿠폰이 없는 것이
        // 된다. 재료를 못 믿을 때도 마찬가지다 — 없는 것과 모르는 것은 다르다.
        if (view.isBeforeFirstTick() || holder.isDataStale(view)) {
            count("deferred-no-material");
            return chain.filter(exchange);
        }
        count("unknown-coupon");
        return error.write(exchange, ApiError.Code.UNKNOWN_COUPON);
    }

    private Mono<Void> route(ServerWebExchange exchange, GatewayFilterChain chain,
            AdmissionDecision decision, String couponId, CouponState state) {
        if (decision.isPass()) {
            return chain.filter(exchange);
        }
        if (decision.isEnqueue()) {
            return enqueue(exchange, chain, couponId, state);
        }
        return error.write(exchange, codeOf(decision), retryAfterSec(decision, random));
    }

    /**
     * 줄에 세운다. <b>여기가 요청 경로에서 레디스를 치는 유일한 자리다</b> (RD-4) —
     * 통과한 사람은 여기 안 온다.
     */
    private Mono<Void> enqueue(ServerWebExchange exchange, GatewayFilterChain chain,
            String couponId, CouponState state) {
        String memberId = exchange.getRequest().getHeaders().getFirst(MEMBER_ID);
        if (memberId == null) {
            // 형식 검증이 앞에서 걸렀어야 한다. 여기 오면 배선이 틀린 것이다.
            count("no-member");
            return error.write(exchange, ApiError.Code.INVALID_REQUEST);
        }
        // **판정에 쓴 상태를 그대로 쓴다.** 여기서 다시 읽으면 그 사이 틱이
        // 지나 판정과 답이 어긋난다.
        long capacity = state.queueCapacity(MAX_ETA_SEC);
        return queue.enqueue(couponId, memberId, capacity, clock.instant())
                // **여기까지만 열어 준다.** 뒤에 붙이면 줄에 선 사람이 응답을
                // 못 써서 뒷단까지 가고, 자리를 쥔 채로 재고까지 먹는다.
                .onErrorResume(e -> {
                    // **요청마다 안 찍는다.** 레디스 장애는 순간이 아니라 구간으로
                    // 오므로, 여기서 찍으면 초당 수천 줄이 스택트레이스째 쌓인다.
                    //
                    // 대신 예외 종류를 라벨로 센다. 레디스가 죽은 것과 인자가
                    // 틀린 것은 다르게 다뤄야 하는데, 한 숫자로는 못 가린다.
                    count("enqueue-error", e.getClass().getSimpleName());
                    return Mono.empty();
                })
                .switchIfEmpty(Mono.defer(() -> failOpen(exchange, chain).then(Mono.empty())))
                .flatMap(entry -> {
                    if (!entry.accepted()) {
                        // 2차 방어에 걸렸다. 판정은 자리가 있다고 봤지만 실제로는 없다.
                        count(AdmissionDecision.REJECT_QUEUE_FULL.name());
                        return error.write(exchange, ApiError.Code.QUEUE_FULL,
                                retryAfterSec(AdmissionDecision.REJECT_QUEUE_FULL, random));
                    }
                    double etaSec = EtaPolicy.etaSec(entry.rank(), state.credit());
                    return waiting.waiting(exchange,
                            tokens.issue(couponId, memberId, clock.instant()),
                            entry.rank(), (long) Math.max(0, etaSec),
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
    private Mono<Void> failOpen(ServerWebExchange exchange, GatewayFilterChain chain) {
        if (failOpen.tryAcquire(FAIL_OPEN_KEY, FAIL_OPEN_CAP, clock.instant().getEpochSecond())) {
            count("enqueue-failed-open");
            return chain.filter(exchange);
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

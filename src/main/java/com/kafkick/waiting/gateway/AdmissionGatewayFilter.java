package com.kafkick.waiting.gateway;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.admission.AdmissionRequest;
import com.kafkick.waiting.domain.coupon.CouponState;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 발급 요청을 통과·대기·거절로 가른다. <b>판정 재료는 로컬 스냅샷에서만 읽는다</b>
 * — 요청마다 레디스를 치면 제어 평면을 만든 이유가 사라진다.
 */
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

    private final SnapshotHolder holder;
    private final AdmissionDecider decider;
    private final Clock clock;
    private final MeterRegistry meters;
    private final ApiError error = ApiError.create();

    /** 설정 오류를 한 번만 알린다. 라우트가 틀렸으면 늘 틀리다. */
    private final AtomicBoolean misconfigured = new AtomicBoolean();

    private AdmissionGatewayFilter(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
        this.decider = Objects.requireNonNull(decider, "decider 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
    }

    public static AdmissionGatewayFilter of(SnapshotHolder holder, AdmissionDecider decider,
            Clock clock, MeterRegistry meters) {
        return new AdmissionGatewayFilter(holder, decider, clock, meters);
    }

    /**
     * 사유별로 센다. <b>쿠폰 ID 를 라벨에 안 넣는다</b> — 인증이 없어 아무 문자열이나
     * 들어오고, 그러면 지표 하나가 메모리를 밀어낸다.
     */
    private void count(String outcome) {
        meters.counter(METRIC, "outcome", outcome).increment();
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
            return reject(exchange, HttpStatus.BAD_REQUEST, ApiError.INVALID_REQUEST,
                    "요청을 처리할 수 없습니다.");
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
        return route(exchange, chain, decision);
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
        return reject(exchange, HttpStatus.NOT_FOUND, ApiError.NOT_FOUND,
                "쿠폰을 찾을 수 없습니다.");
    }

    /**
     * 판정값이 받는 상태 코드. <b>거절을 하나로 뭉치지 않는다</b> — 매진은 끝난
     * 것이고, 큐 만원은 잠시 뒤 다시 오면 되고, 과부하는 노드를 늘려야 한다.
     */
    public static HttpStatus statusOf(AdmissionDecision decision) {
        return switch (decision) {
            case PASS_TOKEN, PASS_BYPASS, PASS_FAIL_OPEN, PASS_UNDER_CAP -> HttpStatus.OK;
            case ENQUEUE_STALE, ENQUEUE_ALWAYS, ENQUEUE_BACKLOG,
                 ENQUEUE_RATE_COUPON, ENQUEUE_RATE_GLOBAL, ENQUEUE_KEY_SATURATED ->
                    HttpStatus.ACCEPTED;
            case REJECT_SOLD_OUT -> HttpStatus.CONFLICT;
            case REJECT_QUEUE_FULL -> HttpStatus.TOO_MANY_REQUESTS;
            case REJECT_OVERLOAD -> HttpStatus.SERVICE_UNAVAILABLE;
            // 차례가 온 사람을 큐 뒤로 안 돌린다. 되돌리면 허가가 "아마도" 가 된다.
            case RETRY_TOKEN -> HttpStatus.TOO_MANY_REQUESTS;
        };
    }

    private Mono<Void> route(ServerWebExchange exchange, GatewayFilterChain chain,
            AdmissionDecision decision) {
        if (decision.isPass()) {
            return chain.filter(exchange);
        }
        // 큐 등록은 CY-402 다. 그전까지는 붙잡지 않고 뒷단으로 보낸다 —
        // 여기서 막으면 아직 못 만든 응답을 기다리는 사람이 생긴다.
        if (decision.isEnqueue()) {
            return chain.filter(exchange);
        }
        return reject(exchange, statusOf(decision), codeOf(decision), messageOf(decision));
    }

    private String codeOf(AdmissionDecision decision) {
        return switch (decision) {
            case REJECT_QUEUE_FULL -> ApiError.QUEUE_FULL;
            case REJECT_OVERLOAD -> ApiError.TEMPORARILY_UNAVAILABLE;
            case RETRY_TOKEN -> ApiError.RETRY_TOKEN;
            default -> ApiError.STOCK_EXHAUSTED;
        };
    }

    private String messageOf(AdmissionDecision decision) {
        return switch (decision) {
            case REJECT_QUEUE_FULL -> "대기열이 가득 찼습니다.";
            case REJECT_OVERLOAD -> "지금은 처리할 수 없습니다.";
            case RETRY_TOKEN -> "잠시 후 다시 시도해 주세요.";
            default -> "재고가 모두 소진되었습니다.";
        };
    }

    private String pathVariable(ServerWebExchange exchange) {
        Map<String, String> vars = exchange.getAttribute(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE);
        return vars == null ? null : vars.get(COUPON_ID);
    }

    private Mono<Void> reject(ServerWebExchange exchange, HttpStatus status,
            String code, String message) {
        return error.write(exchange.getResponse(), status,
                error.body(status, code, message));
    }

    @Override
    public String toString() {
        return "Admission";
    }
}

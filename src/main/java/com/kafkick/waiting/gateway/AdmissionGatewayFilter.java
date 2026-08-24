package com.kafkick.waiting.gateway;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.admission.AdmissionRequest;
import com.kafkick.waiting.domain.coupon.CouponState;
import java.util.Map;
import java.util.Objects;
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

    /** 받아도 되는 최대 대기 시간. 넘으면 줄을 세우는 것이 되레 나쁘다. */
    private static final long MAX_ETA_SEC = 600;

    private final SnapshotHolder holder;
    private final AdmissionDecider decider;
    private final ApiError error = ApiError.create();

    private AdmissionGatewayFilter(SnapshotHolder holder, AdmissionDecider decider) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
        this.decider = Objects.requireNonNull(decider, "decider 는 필수다");
    }

    public static AdmissionGatewayFilter of(SnapshotHolder holder, AdmissionDecider decider) {
        return new AdmissionGatewayFilter(holder, decider);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String couponId = pathVariable(exchange);
        if (couponId == null) {
            // 라우트에서 변수 이름을 빼면 판정할 쿠폰이 없다. 그대로 흘리면
            // 판정이 사라진 채로 기동만 성공한다.
            log.error("라우트에 {} 경로변수가 없다 — 판정할 대상을 못 정한다", COUPON_ID);
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
                view.snapshot().publishedAt().getEpochSecond(), MAX_ETA_SEC));
        exchange.getAttributes().put(DECISION, decision);
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
            log.debug("재료를 못 믿어 판정을 미룬다 — 쿠폰 {}", couponId);
            return chain.filter(exchange);
        }
        return reject(exchange, HttpStatus.NOT_FOUND, ApiError.NOT_FOUND,
                "쿠폰을 찾을 수 없습니다.");
    }

    private Mono<Void> route(ServerWebExchange exchange, GatewayFilterChain chain,
            AdmissionDecision decision) {
        if (decision.isPass()) {
            return chain.filter(exchange);
        }
        if (decision.isReject()) {
            return reject(exchange, HttpStatus.CONFLICT, ApiError.STOCK_EXHAUSTED,
                    "재고가 모두 소진되었습니다.");
        }
        // 큐 등록은 CY-402 다. 그전까지는 붙잡지 않고 뒷단으로 보낸다 —
        // 여기서 막으면 아직 못 만든 응답을 기다리는 사람이 생긴다.
        return chain.filter(exchange);
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

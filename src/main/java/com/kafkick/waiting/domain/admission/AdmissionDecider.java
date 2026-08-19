package com.kafkick.waiting.domain.admission;

import com.kafkick.waiting.domain.admission.SecondWindowLimiter.AcquireResult;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;

/**
 * 입장 판정. <b>순서가 곧 정책이다.</b>
 *
 * <p>각 줄에는 앞줄보다 먼저 와야 하는 이유가 있다. 이전 구현은 {@code dataStale}
 * 을 맨 앞에 두는 바람에 이미 줄 선 사람이 있는 쿠폰에도 신규 유입을 통과시켰다.
 */
public class AdmissionDecider {

    /** 노드 전역 예산의 키. 쿠폰과 겹치지 않게 접두사를 둔다. */
    private static final String GLOBAL_KEY = "__node__";

    private final SecondWindowLimiter limiter;
    private final double idleCreditRatio;

    public AdmissionDecider(SecondWindowLimiter limiter, double idleCreditRatio) {
        this.limiter = limiter;
        this.idleCreditRatio = idleCreditRatio;
    }

    /** 판정 사다리 10줄. 위에서부터 처음 걸리는 줄이 답이다. */
    public AdmissionDecision decide(AdmissionRequest req) {
        CouponState s = req.state();

        // 1 — 재고가 없으면 나머지를 볼 필요가 없다. 맨 앞이어야 매진 쿠폰이
        //     fail-open 상한을 갉아먹지 않는다.
        if (s.remainingStock() <= 0) {
            return AdmissionDecision.REJECT_SOLD_OUT;
        }

        // 2 — 차례가 온 사람. 다시 세우지 않는다. 쿠폰별 상한은 걸지 않고
        //     노드 상한만 본다 — 이미 배분 시점에 크레딧을 썼다 (B-14).
        if (req.validToken()) {
            return limiter.tryAcquire(GLOBAL_KEY, globalCap(req), req.epochSecond())
                    ? AdmissionDecision.PASS_TOKEN
                    : AdmissionDecision.RETRY_TOKEN;
        }

        // 3 — 운영자가 껐다. 붐비든 말든 줄을 안 세운다.
        if (s.mode() == QueueMode.OFF) {
            return AdmissionDecision.PASS_BYPASS;
        }

        boolean hasQueue = s.waiting() > 0 || req.justEnqueued();

        // 4 — 낡았지만 줄이 비었다. 밀어낼 사람이 없으니 상한 안에서 통과.
        if (req.dataStale() && !hasQueue) {
            return limiter.tryAcquire(GLOBAL_KEY, globalCap(req), req.epochSecond())
                    ? AdmissionDecision.PASS_FAIL_OPEN
                    : AdmissionDecision.REJECT_OVERLOAD;
        }

        // 5 — 줄 자체가 꽉 찼다. 큐로 보내는 모든 줄보다 앞에 있어야 한다.
        //     **줄이 있을 때만 의미가 있다.** 한산한 쿠폰은 credit 이 0 이라
        //     용량도 0 이고, 조건을 안 걸면 waiting(0) >= 0 이 참이 되어
        //     R1 경로가 통째로 막힌다.
        if (s.waiting() > 0 && s.waiting() >= s.queueCapacity(req.maxEtaSec())) {
            return AdmissionDecision.REJECT_QUEUE_FULL;
        }

        // 6 — 낡았는데 줄에 사람이 있다. 모른다는 것이 추월의 사유가 아니다 (F1).
        if (req.dataStale()) {
            return AdmissionDecision.ENQUEUE_STALE;
        }

        // 7 — 운영자가 무조건 세우기로 했다.
        if (s.mode() == QueueMode.ALWAYS) {
            return AdmissionDecision.ENQUEUE_ALWAYS;
        }

        // 8 — 이미 붐빈다. 래치는 스냅샷이 따라잡기 전의 한 틱을 메운다.
        if (s.runtime() != RuntimeState.IDLE || req.justEnqueued()) {
            return AdmissionDecision.ENQUEUE_BACKLOG;
        }

        // ── 여기까지 왔다면 안 몰리는 쿠폰이다 ──

        // 9 — 안 몰려도 무제한은 아니다. 두 예산을 함께 차감한다.
        AcquireResult acquired = limiter.tryAcquireAll(
                req.couponKey(), s.idleCap(req.meta(), idleCreditRatio),
                GLOBAL_KEY, globalCap(req), req.epochSecond());

        return switch (acquired) {
            // 10 — 줄도 토큰도 없이 뒷단으로. 이 경로가 R1 이다.
            case ACQUIRED -> AdmissionDecision.PASS_UNDER_CAP;
            case COUPON_EXHAUSTED -> AdmissionDecision.ENQUEUE_RATE_COUPON;
            case GLOBAL_EXHAUSTED -> AdmissionDecision.ENQUEUE_RATE_GLOBAL;
        };
    }

    /** 이 노드가 초당 감당할 양. 쿠폰과 무관한 노드 전체의 상한이다. */
    private long globalCap(AdmissionRequest req) {
        return req.meta().globalCredit() / req.meta().effectiveGatewayCount();
    }
}

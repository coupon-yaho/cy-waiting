package com.kafkick.waiting.domain.admission;

import com.kafkick.waiting.domain.admission.SecondWindowLimiter.AcquireResult;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.coupon.RuntimeState;

/**
 * 입장 판정. <b>순서가 곧 정책이다.</b>
 *
 * <p>각 줄에는 앞줄보다 먼저 와야 하는 이유가 있다. 이전 구현은 {@code dataStale}
 * 을 맨 앞에 두는 바람에 이미 줄 선 사람이 있는 쿠폰에도 신규 유입을 통과시켰다.
 */
public class AdmissionDecider {

    /**
     * 노드 전역 예산의 키.
     *
     * <p>쿠폰 키에는 {@link #couponBudgetKey} 가 다른 접두사를 붙인다. 접두사가
     * 없으면 쿠폰 ID 하나가 이 값과 같아지는 순간 두 예산이 한 카운터로 합쳐진다.
     */
    /**
     * 노드 전체 예산의 키.
     *
     * <p><b>드러내 둔다.</b> 장애 개방처럼 판정 밖에서 여는 경로도 같은 키를 써야
     * 한다 — 따로 들면 한 초에 두 예산이 겹쳐 나간다 (F4).
     */
    public static final String GLOBAL_KEY = "node:";

    private static final String COUPON_KEY_PREFIX = "coupon:";

    private final SecondWindowLimiter limiter;
    private final double idleCreditRatio;

    private AdmissionDecider(SecondWindowLimiter limiter, double idleCreditRatio) {
        this.limiter = limiter;
        this.idleCreditRatio = idleCreditRatio;
    }

    /**
     * 설정을 검증하고 만든다.
     *
     * <p>비율은 10번 줄에서만 쓰이므로, 여기서 안 막으면 <b>잘못된 설정으로도
     * 토큰·bypass·fail-open 판정이 정상으로 돌아간다.</b> 그러다 한산한 쿠폰
     * 요청 하나가 들어오는 순간 터진다 — 원인에서 먼 곳에서.
     */
    public static AdmissionDecider of(SecondWindowLimiter limiter, double idleCreditRatio) {
        if (limiter == null) {
            throw new IllegalArgumentException("limiter 는 필수다");
        }
        if (!Double.isFinite(idleCreditRatio) || idleCreditRatio < 0) {
            throw new IllegalArgumentException(
                    "idleCreditRatio 는 0 이상 유한값이어야 한다: %s".formatted(idleCreditRatio));
        }
        return new AdmissionDecider(limiter, idleCreditRatio);
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

        // **줄의 존재는 정책보다 먼저 본다.** mode 는 사람이 고른 값이고
        // waiting 은 기계가 관측한 값이라 서로 독립이다 — 어긋난 조합이
        // 실제로 생긴다.
        boolean hasQueue = s.waiting() > 0 || req.justEnqueued();

        // 3 — 낡았지만 줄이 비었다. 밀어낼 사람이 없으니 상한 안에서 통과.
        //
        // **꺼진 쿠폰보다 앞이다.** 낡은 구간은 상태를 모르는 구간이고 그래서
        // 상한이 있다. 뒤에 두면 꺼진 쿠폰만 무제한으로 뒷단에 꽂혀 그 상한이
        // 있으나 마나가 된다.
        if (req.dataStale() && !hasQueue) {
            return limiter.tryAcquire(GLOBAL_KEY, globalCap(req), req.epochSecond())
                    ? AdmissionDecision.PASS_FAIL_OPEN
                    : AdmissionDecision.REJECT_OVERLOAD;
        }

        // 4 — 운영자가 껐다. **다만 줄이 비었을 때만이다.**
        //
        // 줄이 남아 있는데 우회시키면 신규 유입이 그 줄을 통째로 추월하고
        // 재고까지 먼저 먹는다 — 6번이 낡은 스냅샷에서 막은 것을 여기서
        // 그대로 뚫는 셈이다 (불변식 4).
        //
        // `justEnqueued` 를 포함한다. waiting 이 아직 0 인데 이 요청이 방금
        // 줄에 들어갔으면, 우회시킬 때 자기가 방금 선 줄을 자기가 추월한다.
        if (s.mode() == QueueMode.OFF && !hasQueue) {
            return AdmissionDecision.PASS_BYPASS;
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
                couponBudgetKey(req.couponKey()), s.idleCap(req.meta(), idleCreditRatio),
                GLOBAL_KEY, globalCap(req), req.epochSecond());

        return switch (acquired) {
            // 10 — 줄도 토큰도 없이 뒷단으로. 이 경로가 R1 이다.
            case ACQUIRED -> AdmissionDecision.PASS_UNDER_CAP;
            case COUPON_EXHAUSTED -> AdmissionDecision.ENQUEUE_RATE_COUPON;
            case GLOBAL_EXHAUSTED -> AdmissionDecision.ENQUEUE_RATE_GLOBAL;
            case KEY_SATURATED -> AdmissionDecision.ENQUEUE_KEY_SATURATED;
        };
    }

    /**
     * 줄 길이 상한.
     *
     * <p><b>배수 속도를 모르는 것과 자리가 없는 것은 다르다.</b> 한산하던 쿠폰에
     * 사람이 몰리기 시작하면 그 순간 credit 은 0 이다 — 배분은 줄이 있어야 나가고
     * 줄은 게이트웨이가 만든다. 여기서 0 을 상한으로 쓰면 줄이 한 번도 안 생기고
     * 쿠폰이 그 상태에 갇힌다.
     *
     * <p>그렇다고 상한을 아예 없애면 안 된다. 스냅샷이 낡은 동안에는 대기 인원이
     * 영영 0 으로 보여서 이 자리가 계속 불린다. 장애가 지속되는 내내 줄이 무한히
     * 자란다 (R5).
     *
     * <p>그래서 <b>노드 몫으로 잰다.</b> 이 노드가 초당 감당하는 양에 최대 대기
     * 시간을 곱한 값이다 — 9번의 한산 통과 상한과 같은 논법이고, 새 숫자를
     * 발명하지 않는다.
     *
     * <p><b>{@code maxEtaSec} 은 사다리 5번과 같은 값이어야 한다.</b> 갈라지면
     * 대기 인원이 0 보다 큰데 상한이 0 인 조합이 생기고, 5번이 상한을 걸었다고
     * 믿는 쿠폰에서 무제한 등록이 난다.
     */
    public static long queueCapacity(CouponState state, SnapshotMeta meta, long maxEtaSec) {
        long byCredit = state.queueCapacity(maxEtaSec);
        if (byCredit > 0) {
            return byCredit;
        }
        try {
            return Math.max(1, Math.multiplyExact(globalCap(meta), maxEtaSec));
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /** 이 노드가 초당 감당할 양. 쿠폰과 무관한 노드 전체의 상한이다. */
    public static long globalCap(SnapshotMeta meta) {
        return meta.globalCredit() / meta.effectiveGatewayCount();
    }

    private long globalCap(AdmissionRequest req) {
        return globalCap(req.meta());
    }

    /** 접두사를 붙여 전역 키와 절대 겹치지 않게 한다. */
    private String couponBudgetKey(String couponKey) {
        return COUPON_KEY_PREFIX + couponKey;
    }
}

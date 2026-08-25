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
        //
        //     **여기서는 폴백을 안 쓴다.** 줄이 이미 섰으면 배분이 이 쿠폰을
        //     보고 있고, 뺄 수 없다고 아는 줄에 더 세우느니 거절이 낫다.
        //     credit 이 0 인 채로 굳는 구간은 있다 — 활성 쿠폰 수가 전역
        //     크레딧보다 많으면 몫이 0 으로 떨어진다. 그때도 거절이 맞다.
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
     * <b>등록 경로가 쓰는</b> 줄 길이 상한. 배수 속도를 알면 그것으로, 모르면
     * 가장 낮은 배수 속도를 가정해 잰다.
     *
     * <p>사다리 5번은 이 함수를 안 쓴다 — 폴백은 줄이 아직 없는 구간만을 위한
     * 것이다. 그래서 등록 경로와 5번이 서로 다른 상한을 본다 (AIJ-0073).
     */
    public static long queueCapacity(CouponState state, SnapshotMeta meta, long maxEtaSec) {
        // **원 함수의 가드를 뒤집지 않는다.** 받아 줄 시간이 없으면 자리도 없다.
        // 이걸 안 걸면 아래 폴백이 그 0 을 "모른다" 로 읽고 하한 1 을 돌려준다.
        if (maxEtaSec <= 0) {
            return 0;
        }
        // **배수 속도를 모르는 것과 자리가 없는 것은 다르다.** 0 을 상한으로 쓰면
        // 줄이 한 번도 안 생기고, 아예 없애면 낡은 구간 내내 줄이 자란다 (R5).
        long byCredit = state.queueCapacity(maxEtaSec);
        if (byCredit > 0) {
            return byCredit;
        }
        // **모르면 가장 낮은 배수 속도를 가정한다.** 판의 크기로 재면 안 된다 —
        // 그 수는 전 노드가 공유하는 줄 길이와 비교되고, 무엇보다 이 구간에는
        // 그만큼 뺄 수 있다는 근거가 없다. 배분이 한 번 돌면 주 경로가 실제
        // 크레딧으로 넘겨받는다. 전문은 AIJ-0073.
        return MIN_CREDIT * maxEtaSec;
    }

    /**
     * 배수 속도를 모를 때 가정하는 초당 배수 인원.
     *
     * <p>배분이 줄 수 있는 가장 작은 몫이다. 모르는 구간의 천장은 판의 크기가
     * 아니라 <b>아는 것이 없다는 사실</b>에서 나와야 한다.
     */
    public static final long MIN_CREDIT = 1;

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

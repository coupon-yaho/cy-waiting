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
     * <p>비율은 9번 줄에서만 쓰이므로, 여기서 안 막으면 <b>잘못된 설정으로도
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

    /**
     * 사다리 6번이 보는 참. 폴백을 안 탄다 (AIJ-0073).
     *
     * <p><b>줄이 있을 때만 의미가 있다.</b> 한산한 쿠폰은 credit 이 0 이라 용량도
     * 0 이고, 조건을 안 걸면 {@code 0 >= 0} 이 참이 되어 R1 경로가 통째로 막힌다.
     */
    private boolean queueFull(CouponState s, AdmissionRequest req) {
        return s.waiting() > 0 && s.waiting() >= s.queueCapacity(req.maxEtaSec());
    }

    /**
     * 사다리 3번이 보는 참. <b>등록 경로와 같은 상한으로 잰다.</b>
     *
     * <p>6번의 참을 그대로 쓰면 안 된다. 거기가 폴백을 안 타는 근거는 "줄이 이미
     * 섰으니 배분이 이 쿠폰을 보고 있다" 인데, 3번에는 그 전제가 없다. 배분이 아직
     * 안 돈 구간에서 `credit` 이 0 이면 용량이 0 이 되어 <b>대기자 한 명에 전원이
     * 거절된다</b> — 운영자가 이 값을 거는 오픈 직후가 정확히 그 구간이다 (C-8).
     */
    private boolean alwaysQueueFull(CouponState s, AdmissionRequest req) {
        return s.waiting() > 0 && s.waiting() >= queueCapacity(s, req.maxEtaSec());
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

        // 3 — 운영자가 무조건 세우기로 했다. **낡음보다 앞이다.**
        //
        // 뒤에 두면 낡은 구간에서 이 쿠폰이 통째로 우회하고, 아무도 큐에 안
        // 들어가니 hasQueue 가 영영 거짓이라 그 상태가 스스로 유지된다. 운영자가
        // 이 값을 거는 순간이 바로 리더가 흔들리는 오픈 직후다.
        //
        // 가용성을 안 버린다. 큐가 안 닿으면 등록이 실패하고, 그때는 부르는
        // 쪽이 상한 있는 fail-open 으로 받는다.
        // **줄이 이미 찼으면 여기서 안 세운다.** 안 걸면 거절 대상 하나하나가
        // 레디스 왕복을 시도하고, 레디스가 느린 구간에서는 그 왕복이 전부
        // 타임아웃해 fail-open 으로 흘러 뒷단 트래픽 생성기가 된다.
        if (s.mode() == QueueMode.ALWAYS && !alwaysQueueFull(s, req)) {
            return AdmissionDecision.ENQUEUE_ALWAYS;
        }

        // 4 — 낡았지만 줄이 비었다. 밀어낼 사람이 없으니 상한 안에서 통과.
        //
        // **꺼진 쿠폰보다 앞이다.** 낡은 구간은 상태를 모르는 구간이고 그래서
        // 상한이 있다. 뒤에 두면 꺼진 쿠폰만 무제한으로 뒷단에 꽂혀 그 상한이
        // 있으나 마나가 된다.
        if (req.dataStale() && !hasQueue) {
            return limiter.tryAcquire(GLOBAL_KEY, globalCap(req), req.epochSecond())
                    ? AdmissionDecision.PASS_FAIL_OPEN
                    : AdmissionDecision.REJECT_OVERLOAD;
        }

        // 5 — 운영자가 껐다. **다만 줄이 비었을 때만이다.**
        //
        // 줄이 남아 있는데 우회시키면 신규 유입이 그 줄을 통째로 추월하고
        // 재고까지 먼저 먹는다 — 7번이 낡은 스냅샷에서 막은 것을 여기서
        // 그대로 뚫는 셈이다 (불변식 4).
        //
        // `justEnqueued` 를 포함한다. waiting 이 아직 0 인데 이 요청이 방금
        // 줄에 들어갔으면, 우회시킬 때 자기가 방금 선 줄을 자기가 추월한다.
        if (s.mode() == QueueMode.OFF && !hasQueue) {
            return AdmissionDecision.PASS_BYPASS;
        }

        // 6 — 줄 자체가 꽉 찼다. 큐로 보내는 모든 줄보다 앞에 있어야 한다.
        //     **줄이 있을 때만 의미가 있다.** 한산한 쿠폰은 credit 이 0 이라
        //     용량도 0 이고, 조건을 안 걸면 waiting(0) >= 0 이 참이 되어
        //     R1 경로가 통째로 막힌다.
        //
        //     **여기서는 폴백을 안 쓴다.** 줄이 이미 섰으면 배분이 이 쿠폰을
        //     보고 있고, 뺄 수 없다고 아는 줄에 더 세우느니 거절이 낫다.
        //     credit 이 0 인 채로 굳는 구간은 있다 — 활성 쿠폰 수가 전역
        //     크레딧보다 많으면 몫이 0 으로 떨어진다. 그때도 거절이 맞다.
        if (queueFull(s, req)) {
            return AdmissionDecision.REJECT_QUEUE_FULL;
        }

        // 7 — 낡았는데 줄에 사람이 있다. 모른다는 것이 추월의 사유가 아니다 (F1).
        if (req.dataStale()) {
            return AdmissionDecision.ENQUEUE_STALE;
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
     * <b>등록 경로가 쓰는</b> 줄 길이 상한. 사다리 6번은 이 함수를 안 쓴다 —
     * 폴백은 줄이 아직 없는 구간만의 것이라 둘이 다른 값을 본다 (AIJ-0073).
     */
    public static long queueCapacity(CouponState state, long maxEtaSec) {
        // **원 함수의 가드를 뒤집지 않는다.** 받아 줄 시간이 없으면 자리도 없다.
        // 이걸 안 걸면 음수가 그대로 폴백을 타고 나가고, 스크립트가 오류를 내고,
        // 그 오류는 fail-open 으로 흘러 닫히는 게 아니라 열린다.
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
     * 배수 속도를 모를 때 가정하는 초당 배수 인원. 배분이 줄 수 있는
     * <b>0 이 아닌</b> 가장 작은 몫이다. 올리려면 오버플로 방어를 되살린다 —
     * 곱이 안 넘치는 것은 이 값이 1 이기 때문이다.
     */
    public static final long MIN_CREDIT = 1;

    /** 이 노드가 초당 감당할 양. 쿠폰과 무관한 노드 전체의 상한이다. */
    public static long globalCap(SnapshotMeta meta) {
        return meta.globalCredit() / meta.effectiveGatewayCount();
    }

    /**
     * 이 통과가 <b>이 노드에서</b> 차감한 초당 예산. 격벽 상한이 여기서 나온다.
     *
     * @throws IllegalArgumentException 통과 판정이 아닐 때 — 부르는 쪽이 틀린 것이다
     */
    public long admittedRatePerSec(AdmissionDecision decision, CouponState state,
            SnapshotMeta meta) {
        // **쿠폰 credit 을 그대로 쓰지 않는다.** 사다리 4·5·9번이 통과시키는 것은
        // 전부 IDLE 쿠폰이고 IDLE 이면 credit 이 0 이다 (I1). 그 값으로 재면
        // 한산한 쿠폰일수록 조여진다 — 이전 구현의 핵심 버그가 층만 바꿔 재발한다.
        // 각 줄이 실제로 차감한 예산을 그대로 돌려준다.
        return switch (decision) {
            // 2번 — 쿠폰별 상한 없이 노드 예산만 봤다. credit 은 전 노드가 나눠
            // 쓰는 값이라 그대로 쓰면 노드 수만큼 부풀려진다.
            case PASS_TOKEN -> state.contendedCap(meta.effectiveGatewayCount());
            // 9번 — 한산 몫이 이 경로를 막는 값이다.
            case PASS_UNDER_CAP -> state.idleCap(meta, idleCreditRatio);
            // 4·5번 — 쿠폰별 예산을 안 거친다. 노드 예산이 정직한 상한이다.
            case PASS_BYPASS, PASS_FAIL_OPEN -> globalCap(meta);
            // **전부 열거한다.** default 로 두면 새 통과값이 조용히 0 을 받고,
            // 0 은 상한으로 쓰이는 순간 전면 차단이다.
            case RETRY_TOKEN, REJECT_SOLD_OUT, REJECT_QUEUE_FULL, REJECT_OVERLOAD,
                 ENQUEUE_STALE, ENQUEUE_ALWAYS, ENQUEUE_BACKLOG,
                 ENQUEUE_RATE_COUPON, ENQUEUE_RATE_GLOBAL, ENQUEUE_KEY_SATURATED ->
                    throw new IllegalArgumentException("통과가 아니다: " + decision);
        };
    }

    private long globalCap(AdmissionRequest req) {
        return globalCap(req.meta());
    }

    /** 접두사를 붙여 전역 키와 절대 겹치지 않게 한다. */
    private String couponBudgetKey(String couponKey) {
        return COUPON_KEY_PREFIX + couponKey;
    }
}

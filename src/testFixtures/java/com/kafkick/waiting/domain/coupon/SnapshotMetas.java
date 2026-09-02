package com.kafkick.waiting.domain.coupon;

/**
 * 재료의 전역 값 픽스처.
 *
 * <p><b>자유형 생성을 감싼다</b> (TS-3). 정규 생성자는 노드 수와 배수를 아무렇게나
 * 조합하게 두는데, 그 둘은 운영에서 독립이 아니다.
 */
public final class SnapshotMetas {

    private SnapshotMetas() {
    }

    /** 예산이 넉넉한 회차. 배수가 안 걸린 정상 상태다. */
    public static SnapshotMeta withinBudget(long globalCredit, int gatewayCount) {
        return SnapshotMeta.withoutPollScale(globalCredit, gatewayCount, null);
    }

    /**
     * 예산을 넘긴 스냅샷.
     *
     * <p>배수는 {@code 예상 폴링 / (노드당 예산 × 노드 수)} 에서 나온다. 대기자
     * 상한이 20,000 이므로 노드가 많을수록 나올 수 있는 배수의 천장이 낮아진다 —
     * 노드 100 대에 배수 50 은 초당 100만 폴링을 뜻해 설계 규모 밖이다.
     */
    public static SnapshotMeta overBudget(long globalCredit, int gatewayCount, double pollScale) {
        double 천장 = MAX_WAITING / (BUDGET_RPS_PER_NODE * Math.max(1, gatewayCount));
        if (pollScale > 천장) {
            throw new IllegalArgumentException(
                    "노드 %d 대에서 나올 수 있는 배수는 %.1f 까지다: %s"
                            .formatted(gatewayCount, 천장, pollScale));
        }
        return new SnapshotMeta(globalCredit, gatewayCount, null, pollScale);
    }

    /** R4 의 동시 대기 상한. 이보다 많은 사람이 줄에 있을 수 없다. */
    private static final double MAX_WAITING = 20_000;

    /** {@code PollBudgetPlanner.BUDGET_RPS_PER_NODE} 와 같은 값이다. */
    private static final double BUDGET_RPS_PER_NODE = 200;
}

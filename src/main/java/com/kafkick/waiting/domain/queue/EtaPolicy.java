package com.kafkick.waiting.domain.queue;

/**
 * 예상 대기 시간과 그 표시 구간.
 *
 * <p><b>순간 배수율로 나누지 않는다.</b> 평활화한 값을 쓴다 — GC 스파이크 한 번에
 * 표시 시간이 두 배가 되면 사용자는 서비스가 망가진 신호로 읽는다.
 */
public final class EtaPolicy {

    /** 배수율을 아직 모른다. 무한을 그대로 내보내면 표시 계층이 터진다. */
    public static final double UNKNOWN = -1;

    private static final double[] BUCKET_EDGES = {30, 90, 450};

    private static final EtaDisplay[] BUCKETS = {
        EtaDisplay.ALMOST_THERE,
        EtaDisplay.ABOUT_A_MINUTE,
        EtaDisplay.ABOUT_FIVE_MINUTES,
        EtaDisplay.OVER_TEN_MINUTES
    };

    /**
     * 앞선 {@code rank} 명이 빠지는 데 걸리는 시간(초).
     *
     * @param smoothedCredit 평활화한 초당 배수율. 순간값이 아니다
     */
    public static double etaSec(long rank, double smoothedCredit) {
        if (rank < 0) {
            throw new IllegalArgumentException("rank 는 0 이상이어야 한다: " + rank);
        }
        if (rank == 0) {
            return 0;
        }
        if (!(smoothedCredit > 0)) {
            return UNKNOWN;
        }
        return rank / smoothedCredit;
    }

    /** 표시할 구간. */
    public static EtaDisplay bucket(double etaSec) {
        if (etaSec < 0) {
            return EtaDisplay.CALCULATING;
        }
        for (int i = 0; i < BUCKET_EDGES.length; i++) {
            if (etaSec < BUCKET_EDGES[i]) {
                return BUCKETS[i];
            }
        }
        return BUCKETS[BUCKETS.length - 1];
    }

    private EtaPolicy() {
    }
}

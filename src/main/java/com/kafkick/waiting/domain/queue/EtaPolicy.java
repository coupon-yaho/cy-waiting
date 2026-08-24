package com.kafkick.waiting.domain.queue;

/**
 * 예상 대기 시간과 그 표시 구간.
 *
 * <p><b>순간 배수율로 나누지 않는다.</b> 평활화한 값을 쓴다 — GC 스파이크 한 번에
 * 표시 시간이 두 배가 되면 사용자는 서비스가 망가진 신호로 읽는다.
 */
public final class EtaPolicy {

    /**
     * 배수율을 아직 모른다.
     *
     * <p><b>NaN 으로 표현한다.</b> 음수를 쓰면 어떤 구간보다도 작아서, 읽는 쪽이
     * 모름을 "아주 가까움" 으로 뒤집어 읽는다 — 하필 배수가 멈춘 순간에 폴링이
     * 가장 짧아진다.
     */
    public static final double UNKNOWN = Double.NaN;

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

    /**
     * 표시할 구간. <b>모를 때도 값을 준다</b> — "계산 중" 은 떠날지 기다릴지
     * 판단할 근거를 안 주고, 그것만 떠 있으면 서비스가 멈춘 것으로 읽힌다.
     *
     * <p>모르면 가장 넓은 구간이다. 짧게 말했다가 오래 기다리게 하는 쪽이 훨씬
     * 나쁘다 — 넉넉히 말했다가 일찍 들어가는 것은 반대다.
     */
    public static EtaDisplay bucket(double etaSec) {
        // 모름을 따로 안 본다. NaN 은 어떤 비교에서도 거짓이라 아래 반복을 그대로
        // 지나 마지막 구간으로 떨어진다 — 분기를 두면 아무도 안 도는 가지가 된다.
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

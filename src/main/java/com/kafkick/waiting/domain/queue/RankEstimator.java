package com.kafkick.waiting.domain.queue;

/**
 * 샤드 안 순위를 전역 순위로 환산한다.
 *
 * <p>{@code localRank} 는 <b>도메인이 계산하지 않고 주입받는 값</b>이다 — 어댑터의
 * {@code ZCOUNT} 결과이고, 그 값이 단조라는 보장은 Phase 3 이 진다 (G3.11).
 */
public final class RankEstimator {

    /**
     * 표시할 전역 순위.
     *
     * <p>내 앞의 사람들이 각 샤드에 고르게 흩어져 있다고 보고 곱한다. 오차는
     * 커지지만 <b>상대 오차는 앞으로 갈수록 작아지고</b>, 사용자가 체감하는 것은
     * 자기 앞의 절대값이라 정확해야 할 자리에서 정확하다.
     */
    public static long globalRank(long localRank, int shards) {
        try {
            return Math.multiplyExact(localRank, Math.max(1, shards));
        } catch (ArithmeticException overflow) {
            // 넘치면 음수가 되어 순위가 뒤로 간다. 포화가 거짓말이긴 해도
            // "역행 0" 을 깨는 것보다는 낫다 — 그 크기의 줄은 이미 비정상이다.
            return Long.MAX_VALUE;
        }
    }

    private RankEstimator() {
    }
}

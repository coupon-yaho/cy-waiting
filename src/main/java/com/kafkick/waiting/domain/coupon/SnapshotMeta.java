package com.kafkick.waiting.domain.coupon;

/**
 * 쿠폰별 상태와 함께 오는 전역 값.
 *
 * <p>{@code gatewayCount} 는 분모로 쓰인다. 0 이 들어오면 판정 전체가 터지므로
 * 읽는 쪽이 아니라 여기서 한 번만 방어한다.
 *
 * @param globalCredit  전 쿠폰 합산 초당 통과 몫
 * @param gatewayCount  신선한 게이트웨이 수. 스케줄러가 하트비트로 센다
 * @param pollScale     폴링 간격 배수. <b>판 전체를 보고 나온 값 하나다</b> —
 *                      쿠폰별 필드에 담으면 그 쿠폰이 스냅샷에서 빠지는 순간
 *                      배수도 같이 사라져, 그 줄 전체가 예산 밖으로 나간다
 */
public record SnapshotMeta(long globalCredit, int gatewayCount, Tunables tunables,
        double pollScale) {

    public SnapshotMeta {
        if (globalCredit < 0) {
            throw new IllegalArgumentException("globalCredit 은 음수가 될 수 없다: %d".formatted(globalCredit));
        }
        // NaN 은 비교가 전부 false 라 Math.max 를 그냥 통과한다. 그대로 두면
        // 폴링 간격 계산이 조용히 NaN 이 되어 대기자가 폴링을 멈춘다.
        // 무한도 같이 막는다 — 계산된 값이 흘러드는 자리가 됐다.
        if (!Double.isFinite(pollScale)) {
            throw new IllegalArgumentException("pollScale 이 유한하지 않다: " + pollScale);
        }
        // 거부가 아니라 정규화다. 1 미만은 폴링을 더 자주 하라는 뜻이 되는데
        // 그건 예산을 늘리는 방향이라 의미가 없다.
        pollScale = Math.max(1.0, pollScale);
    }

    /**
     * 배수를 갈아 끼운 사본.
     *
     * <p>배수를 내려면 노드 수가 먼저 있어야 하고, 그 방어는 이 레코드가 쥐고
     * 있다. 부르는 쪽에서 다시 방어하면 사본이 생긴다.
     */
    public SnapshotMeta withPollScale(double scale) {
        return new SnapshotMeta(globalCredit, gatewayCount, tunables, scale);
    }

    /** 배수를 안 실은 재료. 예산이 넉넉하면 이것이 정상이다. */
    public static SnapshotMeta withoutPollScale(long globalCredit, int gatewayCount,
            Tunables tunables) {
        return new SnapshotMeta(globalCredit, gatewayCount, tunables, 1.0);
    }

    /**
     * 튜너블을 안 실은 재료.
     *
     * <p><b>{@code null} 은 "안 실려 왔다" 는 뜻입니다.</b> 기본값으로 채워 버리면
     * 그 기본값이 기동 설정을 덮어써서, 운영자가 아무것도 안 바꿨는데 값이
     * 바뀝니다 — 읽는 쪽이 기동값을 쓰도록 그대로 둡니다.
     */
    public SnapshotMeta(long globalCredit, int gatewayCount) {
        this(globalCredit, gatewayCount, null, 1.0);
    }

    /**
     * 분모로 쓸 수 있는 노드 수.
     *
     * <p>0 이나 음수는 관측 실패지 "노드가 없다"가 아니다. 노드가 정말 없으면
     * 이 코드가 돌지 않는다 — 자기 자신이 노드이기 때문이다.
     */
    public int effectiveGatewayCount() {
        return Math.max(1, gatewayCount);
    }

    /**
     * 실려 온 한산 몫, 없으면 기동값.
     *
     * <p><b>규칙을 한 곳에 둡니다.</b> 부르는 쪽마다 널 검사를 다시 쓰면 같은
     * 규칙을 새로 구현하게 되고, 그중 하나가 조용히 달라집니다.
     */
    // 기동값은 튜너블 문턱에 안 가둔다. 배포로 정하는 값은 사람이 판을 보고
    // 넣지만 운영 값은 장애 중에 눌린 채로 넣는다 — 그래서 그쪽만 좁게 막는다.
    public double idleCreditRatioOr(double startup) {
        return tunables == null ? startup : tunables.idleCreditRatio();
    }

    /** 실려 온 걸림 시간, 없으면 기동값. */
    public long inFlightSecondsOr(long startup) {
        return tunables == null ? startup : tunables.inFlightSeconds();
    }
}

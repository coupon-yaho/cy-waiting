package com.kafkick.waiting.domain.coupon;

/**
 * 쿠폰별 상태와 함께 오는 전역 값.
 *
 * <p>{@code gatewayCount} 는 분모로 쓰인다. 0 이 들어오면 판정 전체가 터지므로
 * 읽는 쪽이 아니라 여기서 한 번만 방어한다.
 *
 * @param globalCredit  전 쿠폰 합산 초당 통과 몫
 * @param gatewayCount  신선한 게이트웨이 수. 스케줄러가 하트비트로 센다
 */
public record SnapshotMeta(long globalCredit, int gatewayCount, Tunables tunables) {

    public SnapshotMeta {
        if (globalCredit < 0) {
            throw new IllegalArgumentException("globalCredit 은 음수가 될 수 없다: %d".formatted(globalCredit));
        }
    }

    /**
     * 튜너블을 안 실은 재료.
     *
     * <p><b>{@code null} 은 "안 실려 왔다" 는 뜻입니다.</b> 기본값으로 채워 버리면
     * 그 기본값이 기동 설정을 덮어써서, 운영자가 아무것도 안 바꿨는데 값이
     * 바뀝니다 — 읽는 쪽이 기동값을 쓰도록 그대로 둡니다.
     */
    public SnapshotMeta(long globalCredit, int gatewayCount) {
        this(globalCredit, gatewayCount, null);
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
}

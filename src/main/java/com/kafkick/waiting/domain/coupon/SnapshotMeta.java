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
public record SnapshotMeta(long globalCredit, int gatewayCount) {

    public SnapshotMeta {
        if (globalCredit < 0) {
            throw new IllegalArgumentException("globalCredit 은 음수가 될 수 없다: %d".formatted(globalCredit));
        }
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

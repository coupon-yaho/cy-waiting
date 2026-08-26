package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Instant;
import java.util.Map;

/**
 * 한 번에 통째로 갈리는 판정 재료.
 *
 * <p><b>키별 캐시가 아니라 통째 교체다.</b> miss 가 나면 그 순간 레디스로 요청이
 * 몰리고, 쿠폰마다 낡음 시점이 다르면 "이 판정이 얼마나 낡았나" 를 말할 수 없다.
 *
 * @param coupons     쿠폰별 상태. 밖에서 못 바꾼다
 * @param meta        전 쿠폰 공통 값
 * @param publishedAt <b>스케줄러가 발행한 시각.</b> 로컬 수신 시각이 아니다 —
 *                    그것만 쓰면 스케줄러가 죽어도 "방금 갱신했다" 고 속는다
 */
public record GatewaySnapshot(Map<String, CouponState> coupons, SnapshotMeta meta,
        Instant publishedAt) {

    /** 첫 갱신 전. {@link Instant#EPOCH} 이라 어떤 임계로도 낡음이다. */
    public static final GatewaySnapshot EMPTY =
            new GatewaySnapshot(Map.of(), new SnapshotMeta(0, 1), Instant.EPOCH);

    public GatewaySnapshot {
        coupons = Map.copyOf(coupons);
    }

    /**
     * 한 번이라도 발행된 것인가.
     *
     * <p>이 판정이 여러 곳에 흩어지면, 한쪽을 느슨하게 할 때 다른 쪽이 조용히
     * 거짓말한다. 갱신 루프의 수용 판정과 헬스가 같은 것을 봐야 한다.
     */
    public boolean isPublished() {
        return !publishedAt.equals(Instant.EPOCH);
    }
}

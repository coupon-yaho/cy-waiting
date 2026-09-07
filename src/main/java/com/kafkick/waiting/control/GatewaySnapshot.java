package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Instant;
import com.kafkick.waiting.domain.routing.InstanceRouting;
import java.util.List;
import java.util.Map;

/**
 * 한 번에 통째로 갈리는 판정 재료 — 키별 캐시면 miss 때 레디스로 몰리고 낡음 시점이 쿠폰마다 갈린다.
 * 보고는 리더만 읽고 요청 경로에서는 레디스를 치지 않으므로 라우팅 목록까지 여기 싣는다.
 *
 * @param coupons     쿠폰별 상태. 밖에서 못 바꾼다
 * @param meta        전 쿠폰 공통 값
 * @param publishedAt <b>스케줄러가 발행한 시각.</b> 로컬 수신 시각만 쓰면 스케줄러가 죽어도 속는다
 * @param instances   라우팅에 쓸 뒷단 목록. <b>비어 있을 수 있다</b> — 옛 리더거나 주소 없는 인스턴스뿐이다
 */
public record GatewaySnapshot(Map<String, CouponState> coupons, SnapshotMeta meta,
        Instant publishedAt, List<InstanceRouting> instances) {

    /** 첫 갱신 전. {@link Instant#EPOCH} 이라 어떤 임계로도 낡음이다. */
    public static final GatewaySnapshot EMPTY =
            new GatewaySnapshot(Map.of(), new SnapshotMeta(0, 1), Instant.EPOCH);

    /** 라우팅 목록이 없는 스냅샷. 옛 리더와 라우팅을 안 쓰는 시험이 이 자리다. */
    public GatewaySnapshot(Map<String, CouponState> coupons, SnapshotMeta meta,
            Instant publishedAt) {
        this(coupons, meta, publishedAt, List.of());
    }

    public GatewaySnapshot {
        coupons = Map.copyOf(coupons);
        instances = List.copyOf(instances);
    }

    /**
     * 한 번이라도 발행된 것인가. 이 판정이 여러 곳에 흩어지면 한쪽을 느슨하게 할 때
     * 다른 쪽이 조용히 거짓말한다 — 갱신 루프의 수용 판정과 헬스가 같은 것을 봐야 한다.
     */
    public boolean isPublished() {
        return !publishedAt.equals(Instant.EPOCH);
    }
}

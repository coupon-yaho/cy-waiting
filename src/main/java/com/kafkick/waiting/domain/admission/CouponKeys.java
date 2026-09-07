package com.kafkick.waiting.domain.admission;

/**
 * 쿠폰별 표를 들고 있는 자리들이 담을 수 있는 쿠폰 수. <b>식별자는 밖에서 오는
 * 값이라 가짓수에 상한이 없어</b> 안 막으면 맵 하나가 노드를 죽인다. 한 곳에서만
 * 정하는 것은 사본이 갈라지면 리미터와 격벽이 다른 상한을 쓰기 때문이다.
 */
public final class CouponKeys {

    /**
     * 운영에서 상정한 것은 활성 쿠폰 <b>2,000개</b>이고, 상한은 그 다섯 배다. 여유를
     * 크게 둔 것은 이 값이 성능이 아니라 <b>폭주를 막는 자리</b>라서다. 넘었을 때
     * 무엇을 하는지는 자리마다 다르다 — 래치는 비우고, 격벽과 리미터는 안 받는다.
     */
    public static final int MAX = 10_000;

    private CouponKeys() {
    }
}

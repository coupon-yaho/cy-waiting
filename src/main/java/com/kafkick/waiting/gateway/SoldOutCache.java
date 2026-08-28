package com.kafkick.waiting.gateway;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 매진을 관찰한 사실을 노드가 기억한다 (7.2 · B-10).
 *
 * <p><b>스냅샷보다 오래 살면 안 된다</b> (B-11). 재입고가 상시 발생하므로
 * 영구 캐시하면 재입고된 쿠폰이 영영 막힌다.
 */
public final class SoldOutCache {

    private final Duration ttl;
    private final int maxKeys;
    private final Map<String, Instant> observed = new ConcurrentHashMap<>();

    private SoldOutCache(Duration ttl, int maxKeys) {
        // **값으로 끄지 못하게 한다.** 0 을 받으면 그 사실이 설정 어디에도 안
        // 드러나고, 캐시가 아무것도 안 하는 것이 정상으로 읽힌다.
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL 은 양수여야 한다: " + ttl);
        }
        if (maxKeys <= 0) {
            throw new IllegalArgumentException("키 상한은 양수여야 한다: " + maxKeys);
        }
        this.ttl = ttl;
        this.maxKeys = maxKeys;
    }

    public static SoldOutCache of(Duration ttl, int maxKeys) {
        return new SoldOutCache(ttl, maxKeys);
    }

    /**
     * 뒷단이 매진이라고 답한 것을 기록한다.
     *
     * <p><b>자리가 없으면 안 받는다.</b> 밀어내면 클라이언트가 고른 키로
     * 이미 들어온 관찰을 지울 수 있다.
     */
    public void observed(String couponId, Instant now) {
        if (observed.size() >= maxKeys) {
            // 죽은 항목이 자리를 잡고 있으면 상한이 "아무것도 못 받는다" 가
            // 된다. 자리가 모자랄 때만 훑는다 — 평소 경로에 비용을 안 얹는다.
            observed.values().removeIf(at -> expired(at, now));
            if (observed.size() >= maxKeys) {
                return;
            }
        }
        observed.put(couponId, now);
    }

    /** 지금 이 쿠폰을 매진으로 봐야 하는가. */
    public boolean soldOut(String couponId, Instant now) {
        Instant at = observed.get(couponId);
        if (at == null) {
            return false;
        }
        if (expired(at, now)) {
            observed.remove(couponId, at);
            return false;
        }
        return true;
    }

    /** 재고가 돌아온 것을 봤다. TTL 을 안 기다리고 푼다 (7.2.4). */
    public void restocked(String couponId) {
        observed.remove(couponId);
    }

    /** 담고 있는 항목 수. 계측이 이 값을 싣는다. */
    public int size() {
        return observed.size();
    }

    private boolean expired(Instant at, Instant now) {
        // 경계는 살아 있다. `isAfter` 를 쓰면 TTL 이 한 눈금 짧아진다.
        return now.isAfter(at.plus(ttl));
    }
}

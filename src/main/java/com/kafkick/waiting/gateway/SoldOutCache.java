package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.CouponKeys;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
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
     * 운영 기본값.
     *
     * <p>TTL 은 스냅샷 낡음 한계(3초)의 몇 배로 잡는다 — 재입고 해제 신호를
     * 놓쳐도 그만큼만 막힌다. 키 상한은 쿠폰 키 상한과 같은 값을 쓴다.
     */
    public static SoldOutCache standard() {
        return new SoldOutCache(Duration.ofSeconds(30), CouponKeys.MAX);
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

    /**
     * 관찰보다 <b>나중에 발행된</b> 재료가 재고를 말한다. TTL 을 안 기다리고 푼다.
     *
     * <p>발행 시각을 안 보면 관찰과 같은 재료가 곧바로 관찰을 지운다 — 캐시가
     * 존재하는 창이 바로 그 창이라, 그러면 아무것도 안 막는다.
     */
    public void restocked(String couponId, Instant publishedAt) {
        observed.computeIfPresent(couponId,
                (key, at) -> publishedAt.isAfter(at) ? null : at);
    }

    /** 담고 있는 항목 수. 계측이 이 값을 싣는다. */
    public int size() {
        return observed.size();
    }

    /**
     * 담긴 수와 상한을 게이지로 낸다 (7.2.7).
     *
     * <p>상한에 닿으면 새 관찰을 못 받고, 그때부터 뒷단이 다시 다 맞는다.
     * 막힌 뒤에 오르는 카운터로는 그 순간을 못 본다.
     */
    public void bindMetrics(MeterRegistry meters) {
        Gauge.builder("waiting.soldout.cache.size", this, SoldOutCache::size)
                .description("매진 관찰을 담고 있는 쿠폰 수").register(meters);
        Gauge.builder("waiting.soldout.cache.capacity", this, c -> c.maxKeys)
                .description("담을 수 있는 상한").register(meters);
    }

    private boolean expired(Instant at, Instant now) {
        // 경계는 살아 있다. `isAfter` 를 쓰면 TTL 이 한 눈금 짧아진다.
        return now.isAfter(at.plus(ttl));
    }
}

package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.CouponKeys;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * 매진을 관찰한 사실을 노드가 기억한다 (7.2 · B-10).
 *
 * <p><b>스냅샷보다 오래 살면 안 된다</b> (B-11). 재입고가 상시 발생하므로
 * 영구 캐시하면 재입고된 쿠폰이 영영 막힌다.
 */
public final class SoldOutCache {

    /**
     * 한 쿠폰의 무장 기록.
     *
     * @param publishedAt 무장 당시 손에 들고 있던 재료의 발행 시각. <b>레디스 시계</b>다
     * @param armedNanos  무장 시각. <b>단조 시계</b>다 — 수명은 벽시계로 안 잰다
     * @param blocked     그동안 끊은 건수. 해제 로그가 이 값을 싣는다
     */
    // **`equals` 가 동일성으로 떨어진다** — `LongAdder` 가 재정의를 안 하기
    // 때문이다. 아래 CAS 제거(`remove(key, armed)`)가 "이 무장을 지운다" 로
    // 도는 것이 그 덕이다. 값 비교가 되는 형으로 바꾸면 뜻이 "같은 무장을
    // 지운다" 로 바뀌어 새 무장을 지울 수 있다.
    private record Armed(Instant publishedAt, long armedNanos, LongAdder blocked) {
    }

    private final Duration ttl;
    private final int maxKeys;
    private final LongSupplier ticker;
    private final Map<String, Armed> observed = new ConcurrentHashMap<>();

    private SoldOutCache(Duration ttl, int maxKeys, LongSupplier ticker) {
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
        this.ticker = Objects.requireNonNull(ticker, "ticker 는 필수다");
    }

    /** 단조 시계를 받는다. 벽시계로 수명을 재면 NTP 보정 한 번에 전부 같이 풀린다. */
    public static SoldOutCache of(Duration ttl, int maxKeys, LongSupplier ticker) {
        return new SoldOutCache(ttl, maxKeys, ticker);
    }

    public static SoldOutCache of(Duration ttl, int maxKeys) {
        return new SoldOutCache(ttl, maxKeys, System::nanoTime);
    }

    /**
     * 배포 값과 같은 모양으로 만든다. <b>정본은 `application.yml` 이다</b> —
     * 여기 값이 갈리면 시험이 배포되는 숫자를 안 재게 된다.
     */
    public static SoldOutCache standard() {
        return of(Duration.ofSeconds(30), CouponKeys.MAX);
    }

    /**
     * 뒷단이 매진이라고 답한 것을 기록한다.
     *
     * @param publishedAt 지금 손에 든 재료의 발행 시각. <b>해제는 이것보다 나중에
     *                    발행된 재료만 한다</b> — 같은 재료로 풀면 관찰을 만든 그
     *                    재료가 곧바로 관찰을 지운다
     * @return 이번에 새로 무장했으면 참. 이미 무장 중이면 거짓 — 로그를 쿠폰당
     *         한 번만 찍게 하려는 것이다 (LG-3)
     */
    public boolean observed(String couponId, Instant publishedAt) {
        Armed existing = observed.get(couponId);
        if (existing != null && !expired(existing)) {
            return false;
        }
        if (existing == null && observed.size() >= maxKeys) {
            // 죽은 항목이 자리를 잡고 있으면 상한이 "아무것도 못 받는다" 가
            // 된다. 자리가 모자랄 때만 훑는다 — 평소 경로에 비용을 안 얹는다.
            observed.values().removeIf(this::expired);
            if (observed.size() >= maxKeys) {
                // **밀어내지 않는다.** 밀어내면 클라이언트가 고른 키로 이미
                // 들어온 관찰을 지울 수 있다.
                return false;
            }
        }
        observed.put(couponId, new Armed(publishedAt, ticker.getAsLong(), new LongAdder()));
        return true;
    }

    /** 지금 이 쿠폰을 매진으로 봐야 하는가. 참이면 끊은 건수를 하나 올린다. */
    public boolean soldOut(String couponId) {
        Armed armed = observed.get(couponId);
        if (armed == null) {
            return false;
        }
        if (expired(armed)) {
            observed.remove(couponId, armed);
            return false;
        }
        // **`LongAdder` 다.** 한 쿠폰에 100K 가 몰리는 것이 전제라, 셀 하나에
        // CAS 를 걸면 그 자체가 경합점이 된다 (RX-11). 값은 해제할 때 한 번만
        // 읽으므로 정합한 읽기 비용을 낼 이유가 없다.
        armed.blocked().increment();
        return true;
    }

    /**
     * 관찰보다 <b>나중에 발행된</b> 재료가 재고를 말한다. TTL 을 안 기다리고 푼다.
     *
     * <p>발행 시각을 안 보면 관찰과 같은 재료가 곧바로 관찰을 지운다 — 캐시가
     * 존재하는 창이 바로 그 창이라, 그러면 아무것도 안 막는다.
     *
     * @return 이번에 푼 기록. 없으면 빈 값 — 부르는 쪽이 로그를 찍는다
     */
    public Optional<Released> restocked(String couponId, Instant publishedAt) {
        // **먼저 락 없이 읽는다** (RX-11). 매진이 듣는 동안 그 쿠폰의 항목이
        // 있으므로, 바로 computeIfPresent 를 부르면 끊는 요청 전부가 같은 버킷
        // 모니터로 수렴한다 — 한 쿠폰에 100K 가 몰리는 것이 이 제품의 전제다.
        Armed armed = observed.get(couponId);
        if (armed == null || !publishedAt.isAfter(armed.publishedAt())) {
            return Optional.empty();
        }
        return observed.remove(couponId, armed)
                ? Optional.of(new Released(
                        Duration.ofNanos(ticker.getAsLong() - armed.armedNanos()),
                        armed.blocked().sum()))
                : Optional.empty();
    }

    /** 해제된 기록. 얼마나 오래 끊었고 몇 건을 끊었는가 (LG-2). */
    public record Released(Duration elapsed, long blocked) {
    }

    /** 담고 있는 항목 수. 계측이 이 값을 싣는다. */
    public int size() {
        return observed.size();
    }

    /**
     * 담긴 수와 상한을 게이지로 낸다 (7.2.7).
     *
     * <p>상한에 닿으면 새 관찰을 못 받고, 그때부터 뒷단이 다시 다 맞는다.
     */
    public void bindMetrics(MeterRegistry meters) {
        Gauge.builder("waiting.soldout.cache.size", this, SoldOutCache::size)
                .description("매진 관찰을 담고 있는 쿠폰 수").register(meters);
        Gauge.builder("waiting.soldout.cache.capacity", this, c -> c.maxKeys)
                .description("담을 수 있는 상한").register(meters);
    }

    // 벽시계로 재면 NTP 계단 한 번에 전 노드의 방패가 같은 순간 풀린다.
    // 경계는 살아 있는 쪽이다 — 아니면 TTL 이 한 눈금 짧아진다.
    private boolean expired(Armed armed) {
        return ticker.getAsLong() - armed.armedNanos() > ttl.toNanos();
    }
}

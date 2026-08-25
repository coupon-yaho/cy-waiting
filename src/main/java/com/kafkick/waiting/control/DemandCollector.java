package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.coupon.QueueMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/**
 * 이번 틱의 수요를 모은다.
 *
 * <p><b>{@code waiting} 을 한 번만 읽는다.</b> 크레딧을 산출한 뒤 다시 읽으면 그
 * 사이에 사람이 빠져 도메인이 막는 조합이 발행되고, 코덱이 그 쿠폰만 떨군다.
 * 떨어진 쿠폰은 판정에서 없는 쿠폰, 즉 매진으로 보인다.
 */
public final class DemandCollector {

    private final Supplier<Mono<List<String>>> activeCoupons;
    private final Function<List<String>, Mono<Map<String, Long>>> queueSizes;
    private final Function<List<String>, Mono<Map<String, Long>>> stocks;
    private final Function<List<String>, Mono<Map<String, QueueMode>>> queueModes;

    private DemandCollector(Supplier<Mono<List<String>>> activeCoupons,
            Function<List<String>, Mono<Map<String, Long>>> queueSizes,
            Function<List<String>, Mono<Map<String, Long>>> stocks,
            Function<List<String>, Mono<Map<String, QueueMode>>> queueModes) {
        this.activeCoupons = Objects.requireNonNull(activeCoupons, "activeCoupons 는 필수다");
        this.queueSizes = Objects.requireNonNull(queueSizes, "queueSizes 는 필수다");
        this.stocks = Objects.requireNonNull(stocks, "stocks 는 필수다");
        this.queueModes = Objects.requireNonNull(queueModes, "queueModes 는 필수다");
    }

    /** 샤드는 여기서 안 본다 — 합산은 명령을 내는 쪽이 한다. */
    public static DemandCollector of(Supplier<Mono<List<String>>> activeCoupons,
            Function<List<String>, Mono<Map<String, Long>>> queueSizes,
            Function<List<String>, Mono<Map<String, Long>>> stocks,
            Function<List<String>, Mono<Map<String, QueueMode>>> queueModes) {
        return new DemandCollector(activeCoupons, queueSizes, stocks, queueModes);
    }

    public Mono<List<CouponDemand>> collect() {
        return activeCoupons.get().flatMap(coupons -> {
            // 빈 인자로 명령을 보내면 레디스가 오류를 낸다. 그 오류가 판을 죽이면
            // 대상이 생겨도 배분이 안 돈다.
            if (coupons.isEmpty()) {
                return Mono.just(List.<CouponDemand>of());
            }
            return Mono.zip(queueSizes.apply(coupons), stocks.apply(coupons), queueModes.apply(coupons))
                    .map(all -> assemble(coupons, all.getT1(), all.getT2(), all.getT3()));
        });
    }

    /**
     * <b>안 온 쿠폰이 있으면 판을 버린다.</b> 빠진 자리를 0 으로 채우면 대기가
     * 0 인 쿠폰이 되어 크레딧이 안 나간다 — 줄 선 사람이 통째로 멈추는데 아무
     * 신호도 없다. 재고는 없을 수 있지만 대기 수는 언제나 온다.
     */
    private List<CouponDemand> assemble(List<String> coupons, Map<String, Long> sizes,
            Map<String, Long> stockValues, Map<String, QueueMode> modes) {
        if (!sizes.keySet().containsAll(coupons)) {
            throw new IllegalStateException("대기 응답에 빠진 쿠폰이 있다: 기대=%d 실제=%d"
                    .formatted(coupons.size(), sizes.size()));
        }
        List<CouponDemand> demands = new ArrayList<>(coupons.size());
        // **정책이 없는 쿠폰은 적응형이다.** 여기만은 빠진 자리를 채워도 된다 —
        // 안 걸었다는 것이 곧 기본값이지, 못 읽은 것이 아니다.
        coupons.forEach(couponId -> demands.add(new CouponDemand(couponId,
                orZero(sizes.get(couponId)), orZero(stockValues.get(couponId)),
                modes.getOrDefault(couponId, QueueMode.ADAPTIVE))));
        return demands;
    }

    private long orZero(Long value) {
        return value == null ? 0 : value;
    }
}

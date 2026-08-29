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

    private final Supplier<Mono<TimedCoupons>> activeCoupons;
    private final Function<List<String>, Mono<Map<String, Long>>> queueSizes;
    private final Function<List<String>, Mono<Map<String, Long>>> stocks;
    private final Function<List<String>, Mono<Map<String, QueueMode>>> queueModes;

    private DemandCollector(Supplier<Mono<TimedCoupons>> activeCoupons,
            Function<List<String>, Mono<Map<String, Long>>> queueSizes,
            Function<List<String>, Mono<Map<String, Long>>> stocks,
            Function<List<String>, Mono<Map<String, QueueMode>>> queueModes) {
        this.activeCoupons = Objects.requireNonNull(activeCoupons, "activeCoupons 는 필수다");
        this.queueSizes = Objects.requireNonNull(queueSizes, "queueSizes 는 필수다");
        this.stocks = Objects.requireNonNull(stocks, "stocks 는 필수다");
        this.queueModes = Objects.requireNonNull(queueModes, "queueModes 는 필수다");
    }

    /** 샤드는 여기서 안 본다 — 합산은 명령을 내는 쪽이 한다. */
    public static DemandCollector of(Supplier<Mono<TimedCoupons>> activeCoupons,
            Function<List<String>, Mono<Map<String, Long>>> queueSizes,
            Function<List<String>, Mono<Map<String, Long>>> stocks,
            Function<List<String>, Mono<Map<String, QueueMode>>> queueModes) {
        return new DemandCollector(activeCoupons, queueSizes, stocks, queueModes);
    }

    /**
     * 이번 틱의 수요와 <b>그것을 읽은 시각</b>.
     *
     * <p>발행 시각이 여기서 나온다 — 재료를 읽은 순간이 곧 그 재료의 나이가
     * 시작되는 지점이다.
     */
    public Mono<TimedDemands> collect() {
        return activeCoupons.get().flatMap(read -> {
            List<String> coupons = read.coupons();
            // 빈 인자로 명령을 보내면 레디스가 오류를 낸다. 그 오류가 판을 죽이면
            // 대상이 생겨도 배분이 안 돈다.
            if (coupons.isEmpty()) {
                return Mono.just(new TimedDemands(List.of(), read.now()));
            }
            return Mono.zip(queueSizes.apply(coupons), stocks.apply(coupons), queueModes.apply(coupons))
                    .map(all -> new TimedDemands(
                            assemble(coupons, all.getT1(), all.getT2(), all.getT3()), read.now()));
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
        coupons.forEach(couponId -> demands.add(demandOf(couponId,
                orZero(sizes.get(couponId)), stockValues.get(couponId),
                modes.getOrDefault(couponId, QueueMode.ADAPTIVE))));
        return demands;
    }

    /**
     * <b>못 읽은 재고를 0 으로 안 접는다.</b> 접으면 재고 키를 잃은 쿠폰이
     * 매진으로 보이고, 다음 판도 이것을 안 되돌린다.
     */
    // **읽은 음수는 미상이 아니다.** 재고 값은 발급 계층이 소유하고, 차감이 0 을
    // 지나치면 실제로 음수가 된다. 미상 표시와 값이 겹친다고 그것을 미상으로
    // 읽으면 다 팔린 줄이 영영 안 닫힌다 — 이 자리가 막으려던 것의 반대다.
    private CouponDemand demandOf(String couponId, long waiting, Long stock, QueueMode mode) {
        return stock == null
                ? CouponDemand.stockUnknown(couponId, waiting, mode)
                : new CouponDemand(couponId, waiting, Math.max(0, stock), mode);
    }

    private long orZero(Long value) {
        return value == null ? 0 : value;
    }
}

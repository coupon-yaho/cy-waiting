package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import java.util.ArrayList;
import java.util.List;
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

    private final int shards;
    private final Supplier<Mono<List<String>>> activeCoupons;
    private final Function<List<String>, Mono<List<Long>>> queueSizes;
    private final Function<List<String>, Mono<List<Long>>> stocks;

    private DemandCollector(int shards, Supplier<Mono<List<String>>> activeCoupons,
            Function<List<String>, Mono<List<Long>>> queueSizes,
            Function<List<String>, Mono<List<Long>>> stocks) {
        if (shards < 1) {
            throw new IllegalArgumentException("shards 는 1 이상이어야 한다: %d".formatted(shards));
        }
        this.shards = shards;
        this.activeCoupons = Objects.requireNonNull(activeCoupons, "activeCoupons 는 필수다");
        this.queueSizes = Objects.requireNonNull(queueSizes, "queueSizes 는 필수다");
        this.stocks = Objects.requireNonNull(stocks, "stocks 는 필수다");
    }

    public static DemandCollector of(int shards, Supplier<Mono<List<String>>> activeCoupons,
            Function<List<String>, Mono<List<Long>>> queueSizes,
            Function<List<String>, Mono<List<Long>>> stocks) {
        return new DemandCollector(shards, activeCoupons, queueSizes, stocks);
    }

    public Mono<List<CouponDemand>> collect() {
        return activeCoupons.get().flatMap(coupons -> {
            // 빈 인자로 명령을 보내면 레디스가 오류를 낸다. 그 오류가 판을 죽이면
            // 대상이 생겨도 배분이 안 돈다.
            if (coupons.isEmpty()) {
                return Mono.just(List.<CouponDemand>of());
            }
            return Mono.zip(queueSizes.apply(coupons), stocks.apply(coupons))
                    .map(both -> assemble(coupons, both.getT1(), both.getT2()));
        });
    }

    /**
     * <b>길이가 안 맞으면 판을 버린다.</b> 짧은 응답을 0 으로 채우면 대기가 0 인
     * 쿠폰이 되어 크레딧이 안 나간다 — 줄 선 사람이 통째로 멈추는데 아무 신호도 없다.
     */
    private List<CouponDemand> assemble(List<String> coupons, List<Long> sizes,
            List<Long> stockValues) {
        if (sizes.size() != coupons.size() * shards) {
            throw new IllegalStateException("대기 응답 길이가 안 맞는다: 기대=%d 실제=%d"
                    .formatted(coupons.size() * shards, sizes.size()));
        }
        if (stockValues.size() != coupons.size()) {
            throw new IllegalStateException("재고 응답 길이가 안 맞는다: 기대=%d 실제=%d"
                    .formatted(coupons.size(), stockValues.size()));
        }
        List<CouponDemand> demands = new ArrayList<>(coupons.size());
        for (int i = 0; i < coupons.size(); i++) {
            long waiting = 0;
            for (int shard = 0; shard < shards; shard++) {
                waiting += orZero(sizes.get(i * shards + shard));
            }
            demands.add(new CouponDemand(coupons.get(i), waiting, orZero(stockValues.get(i))));
        }
        return demands;
    }

    /**
     * 없는 값은 0 이다.
     *
     * <p>재고를 모를 때 크게 잡으면 재고보다 많이 통과시킨다. 초과 발급은 되돌릴
     * 수 없으므로 모르는 쪽은 작게 본다.
     */
    private long orZero(Long value) {
        return value == null ? 0 : value;
    }
}

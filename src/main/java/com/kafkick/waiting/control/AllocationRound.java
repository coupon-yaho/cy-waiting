package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.FairShareAllocator;
import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 한 판. 수요를 모아 크레딧을 나누고 적용한 뒤 발행한다.
 *
 * <p><b>대기 수를 한 번만 읽는다.</b> 크레딧을 산출한 뒤 다시 읽으면 그 사이에
 * 사람이 빠져 도메인이 막는 조합이 발행되고, 코덱이 그 쿠폰만 떨군다. 떨어진
 * 쿠폰은 판정에서 없는 쿠폰 — 즉 매진으로 보인다.
 */
public final class AllocationRound {

    private static final Logger log = LoggerFactory.getLogger(AllocationRound.class);

    private final Supplier<Mono<List<CouponDemand>>> demands;
    private final LongSupplier globalCredit;
    private final IntSupplier gatewayCount;
    private final Function<Grant, Mono<Long>> apply;
    private final Function<Map<String, String>, Mono<Void>> publish;
    private final Supplier<Instant> clock;
    private final CreditSmoother smoother;
    private final SnapshotCodec codec;
    private final FairShareAllocator allocator = FairShareAllocator.create();

    private AllocationRound(Supplier<Mono<List<CouponDemand>>> demands, LongSupplier globalCredit,
            IntSupplier gatewayCount, Function<Grant, Mono<Long>> apply,
            Function<Map<String, String>, Mono<Void>> publish, Supplier<Instant> clock,
            CreditSmoother smoother, SnapshotCodec codec) {
        this.demands = Objects.requireNonNull(demands, "demands 는 필수다");
        this.globalCredit = Objects.requireNonNull(globalCredit, "globalCredit 은 필수다");
        this.gatewayCount = Objects.requireNonNull(gatewayCount, "gatewayCount 는 필수다");
        this.apply = Objects.requireNonNull(apply, "apply 는 필수다");
        this.publish = Objects.requireNonNull(publish, "publish 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.smoother = Objects.requireNonNull(smoother, "smoother 는 필수다");
        this.codec = Objects.requireNonNull(codec, "codec 은 필수다");
    }

    public static AllocationRound of(Supplier<Mono<List<CouponDemand>>> demands,
            LongSupplier globalCredit, IntSupplier gatewayCount, Function<Grant, Mono<Long>> apply,
            Function<Map<String, String>, Mono<Void>> publish, Supplier<Instant> clock,
            CreditSmoother smoother, SnapshotCodec codec) {
        return new AllocationRound(demands, globalCredit, gatewayCount, apply, publish, clock,
                smoother, codec);
    }

    public Mono<Void> run() {
        return demands.get().flatMap(this::allocate);
    }

    private Mono<Void> allocate(List<CouponDemand> collected) {
        long credit = Math.round(smoother.observe(Math.max(0, globalCredit.getAsLong())));
        Map<String, Long> granted = new LinkedHashMap<>();
        allocator.allocate(credit, collected).forEach(g -> granted.put(g.couponId(), g.credit()));

        return Flux.fromIterable(collected)
                .concatMap(demand -> applyOne(demand, granted.getOrDefault(demand.couponId(), 0L)))
                .reduce(0L, Long::sum)
                // **실제로 들어온 수는 나눠 준 수와 다르다.** 큐가 몫보다 짧으면
                // 남고, 적용이 실패하면 0 이다. 안 남기면 크레딧이 어디서 새는지
                // 사후에 못 가린다.
                .doOnNext(admitted -> log.info("배분 한 판 — 크레딧 {}, 들인 인원 {}, 쿠폰 {}개",
                        credit, admitted, collected.size()))
                .then(Mono.defer(() -> publish.apply(
                        codec.encode(snapshot(collected, granted, credit), smoother.snapshot()))));
    }

    /**
     * <b>적용이 실패해도 그 쿠폰을 빼지 않는다.</b> 빠지면 판정에서 없는 쿠폰이
     * 되어 매진으로 보이는데, 적용이 안 된 것과 매진은 전혀 다른 상태다.
     */
    private Mono<Long> applyOne(CouponDemand demand, long credit) {
        if (credit <= 0) {
            return Mono.just(0L);
        }
        return apply.apply(new Grant(demand.couponId(), credit))
                .doOnError(e -> log.warn("배분 적용 실패 — 임계는 그대로다, couponId={}",
                        demand.couponId(), e))
                .onErrorReturn(0L);
    }

    /**
     * 런타임을 <b>발행하는 그 쌍에서</b> 유도한다.
     *
     * <p>못 박으면 다 뺄 수 있는 줄까지 줄 서는 중이 되어 도메인이 막고, 그
     * 쿠폰만 떨어져 매진으로 보인다. 경계는 도메인과 같은 자리여야 한다.
     */
    private GatewaySnapshot snapshot(List<CouponDemand> collected, Map<String, Long> granted,
            long credit) {
        Map<String, CouponState> coupons = new LinkedHashMap<>();
        collected.forEach(demand -> {
            long mine = granted.getOrDefault(demand.couponId(), 0L);
            coupons.put(demand.couponId(), demand.waiting() > 0
                    ? CouponState.offWithQueue(mine, demand.stock(), demand.waiting())
                    : CouponState.idle(demand.stock()));
        });
        return new GatewaySnapshot(coupons,
                new SnapshotMeta(credit, gatewayCount.getAsInt()), clock.get());
    }
}

package com.kafkick.waiting.domain.queue;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import java.util.List;
import java.util.function.ToDoubleFunction;

/**
 * 폴링 부하를 <b>큐를 훑지 않고</b> 구한다.
 *
 * <p>2만 명을 세면 그 계산 자체가 부하다. 배수율을 알면 각 밴드에 몇 명이 있는지는
 * 곱셈 몇 번으로 나온다 — 앞에서 {@code drainRate × 밴드폭} 명씩 채워 나간다.
 */
public final class PollBudgetPlanner {

    /** 밴드 상한(초). 마지막 밴드는 상한이 없다. */
    private static final double[] BAND_EDGES = {5, 30, 120};

    /** 밴드별 폴링 간격(초). {@link PollIntervalPolicy} 와 같은 표를 본다. */
    private static final double[] BAND_INTERVALS = {1, 3, 10, 30};

    /**
     * 이 큐가 만드는 초당 폴링 수.
     *
     * <p>{@code drainRate} 가 0 이면 전원의 ETA 가 무한이라 가장 먼 밴드다.
     * 방어가 없으면 0 으로 나눠 터진다.
     */
    public static double expectedPollRps(long waiting, double drainRate) {
        if (waiting <= 0) {
            return 0;
        }
        if (!(drainRate > 0)) {
            return waiting / BAND_INTERVALS[BAND_INTERVALS.length - 1];
        }

        double rps = 0;
        long placed = 0;
        for (int i = 0; i < BAND_EDGES.length && placed < waiting; i++) {
            // 누적 상한을 올림으로 잡는다. 반올림하면 배수가 아주 느릴 때
            // 맨 앞사람(ETA 0)이 첫 밴드에서 빠져 예산을 과소 추정하고,
            // pollScale 이 안 올라 실제 부하가 예산을 넘는다.
            long cumulative = Math.min(waiting, (long) Math.ceil(BAND_EDGES[i] * drainRate));
            long inBand = Math.max(0, cumulative - placed);
            rps += inBand / BAND_INTERVALS[i];
            placed += inBand;
        }
        rps += (waiting - placed) / BAND_INTERVALS[BAND_INTERVALS.length - 1];
        return rps;
    }

    /**
     * 살아 있는 쿠폰만 합산한다.
     *
     * <p><b>매진 큐를 빼지 않으면</b> 죽은 큐 10만 명이 예산의 대부분을 먹고,
     * 배분에서 막아 둔 기아가 폴링 경로로 되살아난다 (Phase 7 3.3절).
     */
    public static double expectedPollRps(
            List<CouponDemand> demands, ToDoubleFunction<String> drainRateOf) {
        return demands.stream()
                .filter(CouponDemand::isActive)
                .mapToDouble(d -> expectedPollRps(
                        d.waiting(), drainRateOf.applyAsDouble(d.couponId())))
                .sum();
    }

    /**
     * 예산을 넘은 비율. 이 배수만큼 모두의 간격을 함께 늘린다.
     *
     * <p><b>1 미만으로 내려가지 않는다.</b> 한산하다고 더 자주 두드리게 하면
     * 한산할 때 없던 부하를 만든다.
     */
    public static double pollScale(double expectedRps, double budgetRps) {
        if (!(budgetRps > 0)) {
            return 1.0;
        }
        return Math.max(1.0, expectedRps / budgetRps);
    }

    private PollBudgetPlanner() {
    }
}

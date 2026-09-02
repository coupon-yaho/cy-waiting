package com.kafkick.waiting.domain.queue;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
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
     * 노드 한 대가 감당할 폴링(초당).
     *
     * <p>폴링은 레디스를 안 치고 그 노드의 메모리에서 끝난다 — 그래서 예산의
     * 단위가 노드다. 20 대면 4,000 으로 계획서 3.3 절의 예산과 같다.
     */
    // **아직 가정이다.** 계획서의 4,000 을 20 으로 나눠 역산한 값이지 실측이
    // 아니다. 반증하는 것은 한 노드에서 폴링만 걸었을 때의 CPU·지연 실측이고,
    // 그것이 이 값보다 낮으면 배수는 예산을 지키는 시늉만 하게 된다.
    // Phase 10 의 부하 게이트에서 채운다.
    private static final double BUDGET_RPS_PER_NODE = 200;

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
     * 이 규모에서 감당할 폴링(초당).
     *
     * <p><b>상수 하나로 고정하면 증설이 폴링 간격을 못 줄인다.</b> 노드를
     * 늘리는 것이 곧 예산을 늘리는 것이다.
     */
    // 0 이하 방어를 여기서 다시 하지 않는다 — 사본이 생기면 양쪽 다 뮤테이션이
    // 살아남는다. 한쪽을 지워도 다른 쪽이 막아 주기 때문이다. 방어는
    // SnapshotMeta.effectiveGatewayCount() 하나가 쥔다.
    public static double budgetRps(int nodes) {
        return BUDGET_RPS_PER_NODE * nodes;
    }

    /**
     * 이 재료로 걸리는 전역 폴링 배수.
     *
     * <p><b>조립을 한 자리에 둔다.</b> 어느 분모로 예산을 잡는지가 여기 있고,
     * 부르는 쪽이 저마다 조립하면 분모를 바꾸는 날 한쪽만 따라간다 — 시나리오가
     * 낡은 값을 초록으로 단언하게 된다.
     */
    public static Scale scaleFor(SnapshotMeta meta, List<CouponDemand> demands,
            ToDoubleFunction<String> drainRateOf) {
        int nodes = meta.effectiveGatewayCount();
        double expected = expectedPollRps(demands, drainRateOf);
        double budget = budgetRps(nodes);
        return new Scale(expected, budget, pollScale(expected, budget), nodes);
    }

    /** 한 틱의 폴링 예산과 그 결과. */
    public record Scale(double expected, double budget, double scale, int nodes) {
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

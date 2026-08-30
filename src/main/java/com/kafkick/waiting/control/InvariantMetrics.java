package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.ClockSkewTracker;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.function.DoubleSupplier;
import java.util.function.ToDoubleFunction;

/**
 * 불변식이 깨지기 <b>전에</b> 오르는 값들 (6.9.1).
 *
 * <p>초과 발급 자체는 재고를 가진 발급 계층만 압니다. 게이트웨이는 스스로 계산한
 * 값으로 대신 봅니다 — 여기가 오르면 원인이 이쪽에 있습니다.
 */
public final class InvariantMetrics {

    private final AllocationRound round;

    private final ClockSkewTracker skew;

    /**
     * 발행이 버린 미상 표시를 읽는 함수.
     *
     * <p><b>여기 붙들어 둔다.</b> 함수형 계측기는 상태 객체를 약한 참조로 잡으므로,
     * 부르는 자리에서 만든 람다를 그대로 넘기면 GC 뒤에 그 계수가 0 으로 굳는다.
     */
    private final DoubleSupplier markersDropped;

    private InvariantMetrics(AllocationRound round, ClockSkewTracker skew,
            DoubleSupplier markersDropped) {
        this.round = Objects.requireNonNull(round, "round 는 필수다");
        this.skew = Objects.requireNonNull(skew, "skew 는 필수다");
        this.markersDropped = Objects.requireNonNull(markersDropped, "markersDropped 는 필수다");
    }

    /**
     * 지표에 겁니다.
     *
     * <p><b>게이지가 아니라 누적입니다.</b> 마지막 틱의 값을 내면 리더십을 잃는
     * 순간 그 값이 굳고, 15초 스크레이프가 1초짜리 사건을 열넷 중 열넷 놓칩니다.
     */
    public static InvariantMetrics bind(AllocationRound round, ClockSkewTracker skew,
            MeterRegistry meters) {
        return bind(round, skew, meters, () -> 0);
    }

    /**
     * 발행이 버린 미상 표시까지 건다. <b>그 수가 거짓 매진의 직접 증거다.</b>
     */
    public static InvariantMetrics bind(AllocationRound round, ClockSkewTracker skew,
            MeterRegistry meters, DoubleSupplier markersDropped) {
        Objects.requireNonNull(meters, "meters 는 필수다");
        InvariantMetrics metrics = new InvariantMetrics(round, skew, markersDropped);
        // **형제들과 같은 상태 객체를 쓴다.** 여기만 딴 객체를 넘기면 그것만
        // 약한 참조로 남아, GC 뒤에 이 계수가 조용히 0 으로 굳는다.
        metrics.count(meters, "waiting.snapshot.stock.unknown.dropped",
                InvariantMetrics::markersDropped,
                "상한을 넘겨 버린 재고 미상 표시 수. 0 이 아니면 거짓 매진이 나갔다");
        metrics.count(meters, "waiting.allocation.budget.overshoot",
                InvariantMetrics::budgetOvershoot,
                "뒷단이 받는다는 것보다 더 나눠 준 누적량. 초과 발급의 선행 지표다");
        metrics.count(meters, "waiting.allocation.entered.overshoot",
                InvariantMetrics::enteredOvershoot,
                "예산보다 더 들여보낸 누적 인원. 초과 발급의 직접 증거다");
        metrics.count(meters, "waiting.poll.budget.overshoot.ticks",
                InvariantMetrics::pollBudgetOvershootTicks,
                "폴링 예산을 넘겨 전원의 간격을 늘린 누적 틱 수");
        metrics.count(meters, "waiting.snapshot.clock.floor.applied",
                InvariantMetrics::floorApplied,
                "재료를 읽을 때 시각이 뒤로 가 바닥값이 걸린 횟수");
        metrics.count(meters, "waiting.allocation.admitted",
                InvariantMetrics::admitted,
                "차례를 준 누적 인원. 크레딧 낭비의 분모다 (G7.5)");
        metrics.count(meters, "waiting.allocation.stock.unknown.ticks",
                InvariantMetrics::stockUnknownTicks,
                "재고를 못 읽은 채 발행한 누적 쿠폰·틱. 0 이 아니면 재고 키를 잃었다");
        return metrics;
    }

    /** <b>태그를 안 붙입니다.</b> 쿠폰 식별자는 가짓수에 상한이 없습니다 (LG-4). */
    private void count(MeterRegistry meters, String name,
            ToDoubleFunction<InvariantMetrics> read, String why) {
        FunctionCounter.builder(name, this, read)
                .description(why)
                .register(meters);
    }

    /** 평활 지연과 하한이 만드는 초과. 배분기 자체는 준 예산을 안 넘긴다. */
    private double budgetOvershoot() {
        return round.budgetOvershoot();
    }

    /** 발행이 버린 미상 표시 수. 거짓 매진이 나간 직접 증거다. */
    private double markersDropped() {
        return markersDropped.getAsDouble();
    }

    /** 재고를 못 읽은 채 발행한 누적 쿠폰·틱. 매진 오판의 선행 지표다. */
    private double stockUnknownTicks() {
        return round.stockUnknownTicks();
    }

    /** 차례를 준 누적 인원. 실제로 받아 간 수와의 차이가 곧 낭비다. */
    private double admitted() {
        return round.admitted();
    }

    /** 죽은 큐가 예산을 먹거나 노드가 모자라면 여기가 올라간다. */
    private double pollBudgetOvershootTicks() {
        return round.pollBudgetOvershootTicks();
    }

    /** 동점 score 로 임계 하나에 여럿이 걸리면 준 몫보다 많이 들어간다. */
    private double enteredOvershoot() {
        return round.enteredOvershoot();
    }

    /** 큐의 바닥값이 아니라 <b>재료를 읽을 때의 시각</b>이다. 이름이 그렇게 말한다. */
    private double floorApplied() {
        return skew.appliedCount();
    }
}

package com.kafkick.waiting.domain.allocation;

/**
 * 대기열을 켜고 끄는 임계를 비대칭으로 둔다.
 *
 * <p>같은 임계를 쓰면 유입이 임계선 근처에서 흔들릴 때 사용자에게
 * <b>"대기 없음 → 500명 → 대기 없음"</b> 이 반복해서 보인다.
 */
public class QueueingHysteresis {

    private final double enterRatio;
    private final double exitRatio;
    private final int minHoldTicks;

    private boolean queueing;
    private int belowExitTicks;

    private QueueingHysteresis(double enterRatio, double exitRatio, int minHoldTicks) {
        this.enterRatio = enterRatio;
        this.exitRatio = exitRatio;
        this.minHoldTicks = minHoldTicks;
    }

    /** 해제 임계가 진입 임계보다 크면 히스테리시스가 아니라 진동 증폭기가 된다. */
    public static QueueingHysteresis of(double enterRatio, double exitRatio, int minHoldTicks) {
        if (!Double.isFinite(enterRatio) || !Double.isFinite(exitRatio)) {
            throw new IllegalArgumentException("임계는 유한값이어야 한다");
        }
        if (exitRatio > enterRatio) {
            throw new IllegalArgumentException(
                    "해제 임계가 진입 임계보다 클 수 없다: %s > %s".formatted(exitRatio, enterRatio));
        }
        return new QueueingHysteresis(enterRatio, exitRatio, Math.max(0, minHoldTicks));
    }

    /**
     * 이번 틱에 줄을 세울 것인가.
     *
     * <p>켤 때는 {@code enterRatio} 를 넘어야 하고, 끌 때는 {@code exitRatio}
     * 아래로 <b>연속해서</b> {@code minHoldTicks} 만큼 머물러야 한다.
     */
    public boolean shouldQueue(long demand, long capacity) {
        double load = load(demand, capacity);

        if (!queueing) {
            queueing = load >= enterRatio;
            belowExitTicks = 0;
            return queueing;
        }

        if (load >= exitRatio) {
            belowExitTicks = 0;
            return true;
        }

        belowExitTicks++;
        if (belowExitTicks >= minHoldTicks) {
            queueing = false;
            belowExitTicks = 0;
        }
        return queueing;
    }

    /** 배수할 수 없는데(용량 0) 수요가 있으면 줄이 맞다. 없으면 부하도 0 이다. */
    private static double load(long demand, long capacity) {
        if (capacity <= 0) {
            return demand > 0 ? Double.POSITIVE_INFINITY : 0;
        }
        return (double) demand / capacity;
    }
}

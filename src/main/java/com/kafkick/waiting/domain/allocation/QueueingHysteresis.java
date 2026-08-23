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

    /**
     * 이월받은 상태로 시작한다.
     *
     * <p>리더가 바뀔 때마다 꺼진 채로 시작하면, 붙잡고 있던 대기열이 한 틱
     * 꺼졌다 다시 켜진다 — 사람에게는 "대기 없음 → 500명" 이 반복해 보인다.
     * 히스테리시스가 막으려던 진동이 리더 교체 때마다 나는 셈이다.
     */
    public static QueueingHysteresis restore(double enterRatio, double exitRatio,
            int minHoldTicks, Snapshot snapshot) {
        QueueingHysteresis restored = of(enterRatio, exitRatio, minHoldTicks);
        restored.queueing = snapshot.queueing();
        // **유지 틱을 자른다.** 설정이 줄어든 뒤 옛 값이 실려 오면 이미 최소
        // 유지를 넘어, 이월받자마자 첫 틱에 놓아 버린다 — 이월이 스스로를
        // 무력화하고 표시가 한 번 더 튄다.
        restored.belowExitTicks = Math.min(snapshot.belowExitTicks(), minHoldTicks - 1);
        return restored;
    }

    /** 다음 리더에게 넘길 상태. */
    public Snapshot snapshot() {
        return new Snapshot(queueing, belowExitTicks);
    }

    /**
     * 이월 가능한 히스테리시스 상태.
     *
     * @param queueing        지금 줄을 세우고 있는가
     * @param belowExitTicks  이탈 비율 아래로 연속 몇 틱인가
     */
    public record Snapshot(boolean queueing, int belowExitTicks) {

        public Snapshot {
            if (belowExitTicks < 0) {
                throw new IllegalArgumentException(
                        "belowExitTicks 는 0 이상이어야 한다: " + belowExitTicks);
            }
            // 안 붙잡고 있는데 유지 틱이 쌓여 있으면 말이 안 된다. 그대로 이월하면
            // 다음 리더가 켜지자마자 곧바로 끄는 판단을 한다.
            if (!queueing && belowExitTicks != 0) {
                throw new IllegalArgumentException(
                        "안 붙잡는 상태의 belowExitTicks 는 0 이어야 한다: " + belowExitTicks);
            }
        }

        public static Snapshot empty() {
            return new Snapshot(false, 0);
        }
    }

    /** 해제 임계가 진입 임계보다 크면 히스테리시스가 아니라 진동 증폭기가 된다. */
    public static QueueingHysteresis of(double enterRatio, double exitRatio, int minHoldTicks) {
        if (!Double.isFinite(enterRatio) || enterRatio < 0
                || !Double.isFinite(exitRatio) || exitRatio < 0) {
            // 음수를 허용하면 수요가 0 이어도 load(0) >= enterRatio 가 참이라
            // 아무도 안 왔는데 대기열이 켜진다.
            throw new IllegalArgumentException(
                    "임계는 0 이상 유한값이어야 한다: %s / %s".formatted(enterRatio, exitRatio));
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
    private double load(long demand, long capacity) {
        if (capacity <= 0) {
            return demand > 0 ? Double.POSITIVE_INFINITY : 0;
        }
        return (double) demand / capacity;
    }
}

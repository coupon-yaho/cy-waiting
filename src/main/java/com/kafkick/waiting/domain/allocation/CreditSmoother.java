package com.kafkick.waiting.domain.allocation;

/**
 * 여유 값을 지수 이동평균으로 다듬는다.
 *
 * <p>순간값을 그대로 쓰면 GC 스파이크 한 번이 표시 ETA 를 두 배로 만든다.
 * ETA 오차의 지배항이 배수율의 흔들림이다 (Phase 4 F9).
 */
public class CreditSmoother {

    private final double alpha;

    private double value;
    private boolean seeded;

    private CreditSmoother(double alpha, Snapshot snapshot) {
        this.alpha = alpha;
        this.value = snapshot.value();
        this.seeded = snapshot.seeded();
    }

    /** {@code alpha} 가 클수록 최근 값을 빨리 따라간다. */
    public static CreditSmoother of(double alpha) {
        return restore(alpha, Snapshot.empty());
    }

    /**
     * 이월받은 상태로 시작한다.
     *
     * <p>리더가 바뀔 때마다 0 에서 다시 시작하면 그 순간 ETA 가 튄다 (F9).
     */
    public static CreditSmoother restore(double alpha, Snapshot snapshot) {
        if (!Double.isFinite(alpha) || alpha <= 0 || alpha > 1) {
            throw new IllegalArgumentException("alpha 는 (0, 1] 이어야 한다: " + alpha);
        }
        return new CreditSmoother(alpha, snapshot);
    }

    /**
     * 관측치를 넣고 다듬어진 값을 돌려준다.
     *
     * <p>첫 관측치는 그대로 초기값이 된다. 0 에서 시작하면 첫 몇 틱 동안
     * 실제보다 한참 낮은 값이 나가고 그 사이 표시 ETA 가 몇 배로 뛴다.
     */
    public double observe(double credit) {
        if (!Double.isFinite(credit) || credit < 0) {
            throw new IllegalArgumentException("credit 은 0 이상 유한값이어야 한다: " + credit);
        }
        value = seeded ? alpha * credit + (1 - alpha) * value : credit;
        seeded = true;
        return value;
    }

    /** Phase 4 가 스냅샷 메타에 실어 다음 리더에게 넘긴다. */
    public Snapshot snapshot() {
        return new Snapshot(value, seeded);
    }

    /**
     * 이월 가능한 평활화 상태.
     *
     * @param value  지금까지 다듬어진 값
     * @param seeded 관측을 한 번이라도 했는가. 안 했으면 다음 값이 초기값이 된다
     */
    public record Snapshot(double value, boolean seeded) {

        public static Snapshot empty() {
            return new Snapshot(0, false);
        }
    }
}

package com.kafkick.waiting.domain.allocation;

/**
 * 조여 둔 배분을 푸는 속도를 제한한다.
 *
 * <p>서킷이 닫히는 한 틱에 값이 원래 몫으로 그대로 돌아간다. 평활이 조여진 값을
 * 한 번도 안 보기 때문이다 — 그 계단을 회차당 배수로 나눈다 (AIJ-0245).
 */
public final class ReleaseRamp {

    /**
     * 한 회차에 몫이 오를 수 있는 배수.
     *
     * <p><b>제약은 RC4 가 아니라 RC3 이다</b> — 하한 40 에서 여덟 틱이라 회복이
     * 30초 안에 끝난다. 근거는 AIJ-0245.
     */
    public static final double DEFAULT_STEP = 2.0;

    private final double limit;

    /**
     * 지난 회차에 실제로 발행한 몫. 램프가 안 걸린 회차에는 없다.
     *
     * <p>회차 사이에 원자 참조를 안 거친다. {@code CapacityCollector} 의 첫 회차
     * 표시와 같은 선례로, 회차가 겹쳐 돌지 않는 것에 기댄다.
     */
    private long floor;

    /** 램프가 걸려 있는가. 원래 몫에 닿으면 비켜선다. */
    private boolean ramping;

    private ReleaseRamp(double limit) {
        this.limit = limit;
    }

    /** {@code limit} 은 한 회차에 몫이 오를 수 있는 배수다. */
    public static ReleaseRamp of(double limit) {
        if (!Double.isFinite(limit) || limit <= 1) {
            throw new IllegalArgumentException("limit 은 1 보다 커야 한다: " + limit);
        }
        return new ReleaseRamp(limit);
    }

    /**
     * 이번 회차에 낼 몫을 정한다.
     *
     * <p><b>조이는 동안은 손대지 않는다.</b> 조임은 서킷이 하는 일이다. 대신 그
     * 값을 기억해 조임이 풀린 뒤의 출발점으로 쓴다.
     *
     * <p><b>정책 하한 아래로는 안 내려간다</b> — 깎으면 한산 통과 상한이 0 이
     * 된다 (R1). 다만 목표가 그보다 낮으면 목표를 따른다.
     *
     * @param target  조임까지 마친 이번 회차의 몫
     * @param minimum 이 회차의 정책 하한. 조인 회차에는 안 쓴다
     * @param gated   이번 회차가 서킷에 조여졌는가
     */
    public long next(long target, long minimum, boolean gated) {
        if (target < 0) {
            throw new IllegalArgumentException("target 은 0 이상이어야 한다: " + target);
        }
        if (minimum < 0) {
            throw new IllegalArgumentException("minimum 은 0 이상이어야 한다: " + minimum);
        }
        if (gated) {
            // 회복 도중에 다시 조이면 기준도 그 값으로 내려간다. 안 그러면 두
            // 번째 회복이 첫 회복의 중간 값에서 시작한다.
            floor = target;
            ramping = true;
            return target;
        }
        if (!ramping) {
            return target;
        }
        // **1 에서도 올라가야 한다.** 서킷이 반쯤 열린 동안의 몫이 정확히 1 이고,
        // 열려 있던 동안은 0 이다. 배수만으로는 둘 다 제자리다 — 그 회차의 램프는
        // 안 푸는 것과 같다.
        long allowed = Math.max(minimum, Math.max(floor + 1, (long) Math.floor(floor * limit)));
        if (target <= allowed) {
            // **따라잡은 것과 목표가 내려간 것은 다르다.** 목표가 기준 아래로
            // 내려간 회차를 따라잡음으로 읽으면 램프가 그 자리에서 꺼지고, 다음
            // 회차의 계단이 무제한이 된다 — 뒷단이 "여유 0" 을 한 번 보고하는
            // 것만으로 이 장치가 사라진다.
            boolean caught = target >= floor;
            floor = target;
            ramping = !caught;
            return target;
        }
        floor = allowed;
        return allowed;
    }

    /** 지금 램프가 걸려 있는가. 진입과 해제를 쌍으로 남기려면 밖에서 알아야 한다. */
    public boolean ramping() {
        return ramping;
    }

    /** 회차가 접혔을 때 되돌리려고 뜬다. {@code CreditSmoother.Snapshot} 과 같은 모양이다. */
    public record State(long floor, boolean ramping) {
    }

    public State snapshot() {
        return new State(floor, ramping);
    }

    /**
     * 회차가 접혔으면 되돌린다.
     *
     * <p><b>안 되돌리면 기준만 전진한다.</b> 발행 안 된 회차가 기준을 올리면
     * 다음 발행이 실제로 나간 값의 배수에서 시작한다 — 램프가 막으려던 계단이다.
     */
    public void restore(State state) {
        this.floor = state.floor();
        this.ramping = state.ramping();
    }
}

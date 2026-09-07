package com.kafkick.waiting.domain.allocation;

/**
 * 배분이 한 회차에 오를 수 있는 배수를 제한한다. 평활이 조여진 값을 한 번도 안 봐서,
 * 서킷이 닫히는 한 틱에 값이 원래 몫으로 그대로 돌아간다. <b>회복 구간에만
 * 걸지 않는다</b> — 그러면 그 구간이 끝나는 순간이 새 계단이 된다.
 */
public final class ReleaseRamp {

    /**
     * 한 회차에 몫이 오를 수 있는 배수. <b>이 값을 묶는 것은 회복 봉우리가 아니라
     * 회복에 걸리는 시간이다</b> — 다 오르는 데 {@code log(상한/하한)} 틱이 드니
     * 낮출수록 복귀가 늦는다. 근거로 잰 두 회차는 조건이 서로 달라, 기본 조건에서
     * 되풀이해 확인한 값이 아니다.
     */
    public static final double DEFAULT_STEP = 4.0;

    private final double limit;

    /**
     * 지난 회차에 실제로 발행한 몫. <b>음수면 아직 아무것도 안 냈다.</b>
     *
     * <p>회차가 겹쳐 돌지 않는 것에 기대 원자 참조를 안 거친다 ({@code CapacityCollector} 선례).
     */
    private long floor = -1;

    /** 이 회차가 잘렸는가. 목표를 그대로 낸 회차는 안 센다. */
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
     * 이번 회차에 낼 몫을 정한다. <b>조이는 동안은 손대지 않고</b> 그 값을 기억해 풀린
     * 뒤의 출발점으로 쓴다. 정책 하한 아래로 깎으면 한산 통과 상한이 0 이 된다.
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
        // **첫 회차는 견줄 것이 없다.** 제한하면 기동이 하한에서 시작해 예열이
        // 대여섯 틱 늦는다. 한산 통과는 이 면제가 없어도 minimum 이 지킨다 —
        // 그 하한이 곧 한산 통과 상한이 0 이 되는 경계다.
        if (floor < 0) {
            floor = target;
            return target;
        }
        // **1 에서도 올라가야 한다.** 서킷이 반쯤 열린 동안의 몫이 정확히 1 이고,
        // 열려 있던 동안은 0 이다. 배수만으로는 둘 다 제자리다 — 그 회차의 램프는
        // 안 푸는 것과 같다.
        long allowed = Math.max(minimum, Math.max(floor + 1, (long) Math.floor(floor * limit)));
        long value = Math.min(target, allowed);
        // 잘린 회차만 램프로 센다. 목표를 그대로 낸 회차까지 세면 진입 로그가
        // 평상시에도 나간다.
        ramping = value < target;
        floor = value;
        return value;
    }

    /** 마지막 회차가 잘렸는가. 진입과 해제를 쌍으로 남기려면 밖에서 알아야 한다. */
    public boolean ramping() {
        return ramping;
    }

    /**
     * <b>이어받은 값에서 올린다.</b> 승계 노드는 조인 적이 없어 램프가 안 걸리고 첫 회차가
     * 목표까지 한 번에 뛴다. 낡은 큰 값이 브레이크를 풀지 못하게 올리는 쪽으로는 안 받는다.
     *
     * @param published 이 노드가 마지막으로 본 발행 몫
     */
    public void resumeFrom(long published) {
        if (published < 0) {
            throw new IllegalArgumentException("published 는 0 이상이어야 한다: " + published);
        }
        // **발행한 적이 있으면 낮추는 쪽으로만 받는다.** 잘린 회차인지로 가르면
        // 목표가 한 번 내려간 뒤의 승계에서 브레이크가 통째로 풀린다.
        floor = floor < 0 ? published : Math.min(published, floor);
        ramping = true;
    }

    /**
     * 회차가 접혔을 때 되돌리려고 뜬다. <b>못 만드는 조합을 막는다</b> — 픽스처가
     * {@code floor} 음수에 램프를 켜 넣으면 다음 회차가 무제한이 된다.
     */
    public record State(long floor, boolean ramping) {

        public State {
            if (floor < -1) {
                throw new IllegalArgumentException("floor 는 -1 이상이어야 한다: " + floor);
            }
            if (floor < 0 && ramping) {
                throw new IllegalArgumentException("아무것도 안 낸 램프는 걸릴 수 없다");
            }
        }
    }

    public State snapshot() {
        return new State(floor, ramping);
    }

    /**
     * 회차가 접혔으면 되돌린다. <b>안 되돌리면 기준만 전진한다</b> — 발행 안 된
     * 회차가 기준을 올리면 다음 발행이 램프가 막으려던 계단을 밟는다.
     */
    public void restore(State state) {
        this.floor = state.floor();
        this.ramping = state.ramping();
    }
}

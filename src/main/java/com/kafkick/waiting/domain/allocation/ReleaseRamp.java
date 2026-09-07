package com.kafkick.waiting.domain.allocation;

/**
 * 배분이 한 회차에 오를 수 있는 배수를 제한한다.
 *
 * <p>서킷이 닫히는 한 틱에 값이 원래 몫으로 그대로 돌아간다. 평활이 조여진 값을
 * 한 번도 안 보기 때문이다 (AIJ-0245).
 *
 * <p><b>회복 구간에만 걸지 않는다.</b> 그러면 그 구간이 끝나는 순간이 새 계단이
 * 된다 — 실측에서 33 에서 300 으로 뛰었다 (AIJ-0249).
 */
public final class ReleaseRamp {

    /**
     * 한 회차에 몫이 오를 수 있는 배수.
     *
     * <p><b>제약은 RC4 가 아니라 RC3 이다</b> — 드는 틱이 {@code log(상한/하한)}
     * 이라 상한이 크면 그만큼 는다. 2.0 은 상한 1200 에서 회복이 34.9초로 게이트를
     * 넘겼고, 4.0 에서 22.7초다 (AIJ-0248). <b>그 두 회차는 조건이 서로
     * 달랐다</b> — 기본 조건(상한 300)에서 되풀이해 잰 값이 아니다.
     */
    public static final double DEFAULT_STEP = 4.0;

    private final double limit;

    /**
     * 지난 회차에 실제로 발행한 몫. <b>음수면 아직 아무것도 안 냈다</b> — 첫
     * 회차는 견줄 것이 없어 제한하지 않는다.
     *
     * <p>회차 사이에 원자 참조를 안 거친다. {@code CapacityCollector} 의 첫 회차
     * 표시와 같은 선례로, 회차가 겹쳐 돌지 않는 것에 기댄다.
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
        // **첫 회차는 견줄 것이 없다.** 제한하면 기동이 하한에서 시작해 예열이
        // 대여섯 틱 늦는다. R1 은 이 면제가 없어도 minimum 이 지킨다 — 그 하한이
        // 곧 한산 통과 상한이 0 이 되는 경계다.
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
     * <b>이어받은 값에서 올린다.</b> 승계로 리더가 된 노드는 조인 적이 없어
     * 램프가 안 걸리고, 그러면 첫 회차가 발행된 값에서 목표까지 한 번에 뛴다.
     *
     * <p><b>올리는 쪽으로는 안 받는다.</b> 이 값은 승계를 일으킨 바로 그 경로에서
     * 오므로, 낡은 큰 값이면 브레이크가 그 자리에서 풀린다. 내가 직접 본 값이
     * 있으면 그쪽이 더 믿을 만하다.
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
     * 회차가 접혔을 때 되돌리려고 뜬다. {@code CreditSmoother.Snapshot} 과 같은 모양이다.
     *
     * <p><b>못 만드는 조합을 막는다.</b> {@code floor} 가 음수인데 램프가 걸린
     * 상태는 어느 경로도 안 만드는데, 픽스처가 그것을 넣으면 램프가 켜진 채로
     * 다음 회차가 무제한이 된다 (DS-2).
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

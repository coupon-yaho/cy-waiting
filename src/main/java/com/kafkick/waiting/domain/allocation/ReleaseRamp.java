package com.kafkick.waiting.domain.allocation;

/**
 * 조여 둔 배분을 푸는 속도를 제한한다 (RC4).
 *
 * <p>서킷이 배분을 조이는 동안 평활은 억눌린 값을 한 번도 안 본다. 그래서 서킷이
 * 닫히는 한 틱에 값이 원래 몫으로 그대로 돌아간다 — 방금 실패를 끝낸 뒷단을 향한
 * 계단이다. 이 램프가 그 계단을 회차당 배수로 나눈다.
 */
public class ReleaseRamp {

    /** 회복 버스트의 허용 배수. 이보다 크면 회복이 곧 2차 장애다 (RC4). */
    public static final double DEFAULT_LIMIT = 1.2;

    private final double limit;

    /** 지난 회차에 실제로 발행한 몫. 램프가 안 걸린 회차에는 없다. */
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
     * <p><b>조이는 동안은 손대지 않는다.</b> 조임은 서킷이 하는 일이고, 여기서
     * 한 번 더 누르면 같은 값을 두 장치가 두 번 누른다. 대신 그 값을 기억해,
     * 조임이 풀린 뒤의 출발점으로 쓴다.
     *
     * @param target 조임까지 마친 이번 회차의 몫
     * @param gated  이번 회차가 서킷에 조여졌는가
     */
    public long next(long target, boolean gated) {
        if (target < 0) {
            throw new IllegalArgumentException("target 은 0 이상이어야 한다: " + target);
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
        long allowed = Math.max(floor + 1, (long) Math.floor(floor * limit));
        if (target <= allowed) {
            // 닿았다. 다음 회차부터는 비켜선다 — 안 그러면 정상 구간이 계속
            // 램프에 묶여, 캠페인이 열릴 때의 정상적인 증가까지 늦어진다.
            ramping = false;
            return target;
        }
        floor = allowed;
        return allowed;
    }

    /**
     * 기준을 버린다. <b>리더가 바뀔 때 부른다</b> — 남이 리더였던 동안 움직인
     * 값을 못 보고 제 옛 값을 이어 쓰면, 승계 직후 한 틱이 옛 기준의 배수로 나간다.
     */
    public void reset() {
        floor = 0;
        ramping = false;
    }
}

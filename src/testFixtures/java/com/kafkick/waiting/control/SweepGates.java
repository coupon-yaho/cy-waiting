package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.time.Duration;
import java.util.Map;

/**
 * 정상 상태의 스위퍼 판단을 만든다.
 *
 * <p>갓 만들어진 게이트는 승계 직후로 보고 유예만큼 안 쓴다 (CY-822). 정상
 * 상태를 재려는 시험은 그 유예를 먼저 흘려야 한다.
 */
// **프로덕션에는 이런 팩토리를 안 둔다.** 두면 누군가 승계 직후에 그것을 쓰고,
// 그 순간 이 방어가 사라진다. 유예를 지난 상태는 시험이 틱을 흘려서 만든다 —
// 실제로 도달하는 방법과 같다.
public final class SweepGates {

    private SweepGates() {
    }

    /** 승계 유예를 이미 지난 게이트. 재는 것이 정상 상태일 때 쓴다. */
    public static SweepGate warmed(Duration tick, Duration aliveTtl) {
        SweepGate gate = SweepGate.of(tick, aliveTtl);
        Map<String, CouponState> 아무_쿠폰 = Map.of("__warmup", CouponStates.idle(1));
        long 유예 = Math.ceilDiv(
                aliveTtl.plus(PollIntervalPolicy.maxInterval()).toMillis(), tick.toMillis());
        for (long i = 0; i <= 유예; i++) {
            gate.sweepable(아무_쿠폰, false);
        }
        return gate;
    }
}

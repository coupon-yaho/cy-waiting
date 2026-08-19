package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 판정값은 전부 도달 가능해야 한다.
 *
 * <p>사다리는 위에서부터 처음 걸리는 줄이 답이라, 줄 하나를 잘못 끼우면 그 아래
 * 판정이 <b>영영 안 나오는데 아무 테스트도 안 깨진다.</b> 판정값을 늘리고
 * 사다리를 안 고쳤을 때도 마찬가지다. 여기서 붙잡는다.
 */
class DecisionReachabilityTest {

    private static final SnapshotMeta META = new SnapshotMeta(1000, 10);

    @Test
    @DisplayName("사다리는_모든_판정값을_실제로_만들어낸다")
    void 사다리는_모든_판정값을_실제로_만들어낸다() {
        Set<AdmissionDecision> seen = EnumSet.noneOf(AdmissionDecision.class);

        seen.add(decide(CouponStates.closed(100), r -> r));
        seen.add(decide(CouponStates.queueing(100, 500, 3000), r -> r.withValidToken(true)));
        seen.add(decide(CouponStates.off(500), r -> r));
        seen.add(decide(CouponStates.idle(500), r -> r.withDataStale(true)));
        seen.add(decide(CouponStates.queueing(100, 500, 5000), r -> r.withDataStale(true)));
        seen.add(decide(CouponStates.queueing(100, 500, 20_000), r -> r));
        seen.add(decide(always(), r -> r));
        seen.add(decide(CouponStates.queueing(100, 500, 3000), r -> r));
        seen.add(decide(CouponStates.idle(500), r -> r));

        // 상한을 말려야 나오는 넷은 같은 리미터를 반복해서 두드린다
        seen.add(drain(CouponStates.queueing(100, 500, 3000), r -> r.withValidToken(true), 0.7));
        seen.add(drain(CouponStates.idle(500), r -> r.withDataStale(true), 0.7));
        seen.add(drain(CouponStates.idle(500), r -> r, 0.7));
        seen.add(drain(CouponStates.idle(500), r -> r, 5.0));

        // 자리가 하나뿐이면 쿠폰·전역 두 키를 함께 못 넣는다
        AdmissionDecider tight = AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(1), 0.7);
        seen.add(tight.decide(request(CouponStates.idle(500))));

        assertThat(seen)
                .withFailMessage(
                        "도달 못 하는 판정: %s",
                        EnumSet.complementOf(EnumSet.copyOf(seen)))
                .containsExactlyInAnyOrder(AdmissionDecision.values());
    }

    private CouponState always() {
        return new CouponState(QueueMode.ALWAYS, RuntimeState.IDLE, 0, 500, 0, 1.0);
    }

    private AdmissionDecision decide(
            CouponState state, java.util.function.UnaryOperator<AdmissionRequest> tweak) {
        AdmissionDecider decider = AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(1000), 0.7);
        return decider.decide(tweak.apply(request(state)));
    }

    /** 상한이 마를 때까지 두드리고 마지막 판정을 돌려준다. */
    private AdmissionDecision drain(
            CouponState state,
            java.util.function.UnaryOperator<AdmissionRequest> tweak,
            double idleRatio) {
        AdmissionDecider decider =
                AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(1000), idleRatio);
        AdmissionRequest req = tweak.apply(request(state));
        AdmissionDecision last = null;
        for (int i = 0; i < 200; i++) {
            last = decider.decide(req);
        }
        return last;
    }

    private AdmissionRequest request(CouponState state) {
        return new AdmissionRequest("c1", state, META, false, false, false, 0, 100);
    }
}

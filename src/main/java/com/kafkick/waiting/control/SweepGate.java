package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import java.util.ArrayList;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 스위퍼가 <b>돌면 안 되는 구간</b>을 정한다 (7.4.8·7.4.9 · 5.4절).
 *
 * <p>잘못 쓸면 되돌릴 수 없다 — 성실히 줄 선 사람을 이탈자로 판정하면
 * 재입장이 새 score 이고, 그건 순번 역행이다.
 */
public final class SweepGate {

    /**
     * 멈춘 쿠폰과, 다시 쓸 수 있게 되는 틱.
     *
     * <p><b>한 틱이 아니다.</b> 생존 신호는 마지막 폴링에서 TTL 만큼 살고,
     * 그 폴링은 최대 간격만큼 늦게 온다. 그 합보다 짧게 재개하면 아직 신호를
     * 못 채운 사람을 이탈자로 판정한다.
     */
    private final Map<String, Integer> resumeAt = new HashMap<>();

    private int tick;

    private final int resumeDelayTicks;

    private SweepGate(int resumeDelayTicks) {
        if (resumeDelayTicks < 1) {
            throw new IllegalArgumentException("재개 유예는 양수여야 한다: " + resumeDelayTicks);
        }
        this.resumeDelayTicks = resumeDelayTicks;
    }

    /**
     * 재개 유예를 <b>생존 신호 수명과 폴링 간격에서 끌어온다.</b>
     *
     * <p>따로 적으면 한쪽만 고쳤을 때 갈리고, 그때 나는 일이 순번 역행이다.
     */
    public static SweepGate of(Duration tick, Duration aliveTtl) {
        long delay = aliveTtl.plus(PollIntervalPolicy.maxInterval()).toSeconds();
        return new SweepGate((int) Math.ceil((double) delay / tick.toSeconds()));
    }

    /**
     * 이번 틱에 쓸어도 되는 쿠폰들.
     *
     * @param dataStale 재료가 낡았는가. <b>노드 전체에 걸리는 조건</b>이라
     *                  쿠폰별이 아니다
     */
    public List<String> sweepable(Map<String, CouponState> coupons, boolean dataStale) {
        tick++;
        List<String> sweepable = new ArrayList<>();
        coupons.forEach((couponId, state) -> {
            // **매진 중에는 멈춘다.** 7.1 이 매진 조회를 게이트웨이에서
            // 종결하면서 그 쿠폰의 폴링은 생존 신호를 안 갱신한다. 갱신처가
            // 거기 하나뿐이라, 매진으로 보이는 동안 줄 선 전원의 신호가
            // 일제히 멎는다 — 장애 문단과 글자 그대로 같은 사슬이다.
            if (dataStale || state.soldOut()) {
                resumeAt.put(couponId, tick + resumeDelayTicks);
                return;
            }
            // **풀린 뒤 유예만큼 건너뛴다** (7.4.9). 그 구간은 밀렸던 폴링이
            // 아직 안 왔다. 한 틱만 쉬면 신호를 못 채운 사람을 걷는다.
            Integer at = resumeAt.get(couponId);
            if (at != null && tick < at) {
                return;
            }
            resumeAt.remove(couponId);
            // 줄이 없으면 쓸 것도 없다. 왕복을 아낀다.
            if (state.waiting() > 0) {
                sweepable.add(couponId);
            }
        });
        resumeAt.keySet().retainAll(coupons.keySet());
        return List.copyOf(sweepable);
    }
}

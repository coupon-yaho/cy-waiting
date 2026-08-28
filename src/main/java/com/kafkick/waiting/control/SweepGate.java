package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 스위퍼가 <b>돌면 안 되는 구간</b>을 정한다 (7.4.8·7.4.9 · 5.4절).
 *
 * <p>잘못 쓸면 되돌릴 수 없다 — 성실히 줄 선 사람을 이탈자로 판정하면
 * 재입장이 새 score 이고, 그건 순번 역행이다.
 */
public final class SweepGate {

    /** 지난 틱에 멈춰 있던 쿠폰. 풀린 첫 틱은 건너뛴다 (7.4.9). */
    private final Set<String> paused = new HashSet<>();

    public static SweepGate create() {
        return new SweepGate();
    }

    /**
     * 이번 틱에 쓸어도 되는 쿠폰들.
     *
     * @param dataStale 재료가 낡았는가. <b>노드 전체에 걸리는 조건</b>이라
     *                  쿠폰별이 아니다
     */
    public List<String> sweepable(Map<String, CouponState> coupons, boolean dataStale) {
        List<String> sweepable = new ArrayList<>();
        coupons.forEach((couponId, state) -> {
            // **매진 중에는 멈춘다.** 7.1 이 매진 조회를 게이트웨이에서
            // 종결하면서 그 쿠폰의 폴링은 생존 신호를 안 갱신한다. 갱신처가
            // 거기 하나뿐이라, 매진으로 보이는 동안 줄 선 전원의 신호가
            // 일제히 멎는다 — 장애 문단과 글자 그대로 같은 사슬이다.
            if (dataStale || state.soldOut()) {
                paused.add(couponId);
                return;
            }
            // **풀린 첫 틱은 건너뛴다** (7.4.9). 그 순간은 밀렸던 폴링이 아직
            // 안 왔다. 바로 쓸면 안 쓴 것이 한 틱에 한꺼번에 나간다.
            if (paused.remove(couponId)) {
                return;
            }
            // 줄이 없으면 쓸 것도 없다. 왕복을 아낀다.
            if (state.waiting() > 0) {
                sweepable.add(couponId);
            }
        });
        paused.retainAll(coupons.keySet());
        return List.copyOf(sweepable);
    }
}

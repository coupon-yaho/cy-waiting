package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 매진된 쿠폰의 큐를 <b>언제</b> 지워도 되는지 답한다 (7.3).
 *
 * <p>지우는 일은 어댑터가 한다. 여기는 판단만 하므로 레디스 없이 잴 수 있다.
 */
public final class SoldOutCleanup {

    /** 쿠폰별로 매진을 연달아 본 틱 수. 재고가 돌아오면 지운다. */
    private final Map<String, Integer> seen = new HashMap<>();

    /** 이미 지운 쿠폰. 매 틱 같은 명령을 다시 내면 틱당 명령 수가 쿠폰 수만큼 는다. */
    private final Map<String, Boolean> deleted = new HashMap<>();

    private final int graceTicks;

    private SoldOutCleanup(int graceTicks) {
        // **0 이면 유예가 아니다.** 값으로 끄면 그 사실이 설정 어디에도 안
        // 드러나고, 마지막 폴링이 줄을 잃는 것이 정상으로 읽힌다.
        if (graceTicks <= 0) {
            throw new IllegalArgumentException("유예 틱은 양수여야 한다: " + graceTicks);
        }
        this.graceTicks = graceTicks;
    }

    public static SoldOutCleanup of(int graceTicks) {
        return new SoldOutCleanup(graceTicks);
    }

    /**
     * 이번 틱에 큐를 지워도 되는 쿠폰들.
     *
     * <p><b>재료에 없는 쿠폰의 셈은 버린다.</b> 안 버리면 활성 목록을 드나드는
     * 쿠폰이 옛 셈을 이어받아 유예를 다 안 채우고 지워진다.
     */
    public List<String> due(Map<String, CouponState> coupons) {
        seen.keySet().retainAll(coupons.keySet());
        deleted.keySet().retainAll(coupons.keySet());
        List<String> due = new ArrayList<>();
        coupons.forEach((couponId, state) -> {
            if (!state.soldOut()) {
                // **재고가 돌아왔다.** 셈을 버려 삭제를 취소한다 (7.3.2b) —
                // 지워 버리면 줄 선 사람이 순번을 잃는다.
                seen.remove(couponId);
                deleted.remove(couponId);
                return;
            }
            if (deleted.containsKey(couponId)) {
                return;
            }
            int ticks = seen.merge(couponId, 1, Integer::sum);
            if (ticks > graceTicks) {
                deleted.put(couponId, true);
                due.add(couponId);
            }
        });
        return List.copyOf(due);
    }
}

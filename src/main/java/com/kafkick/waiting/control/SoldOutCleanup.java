package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 매진된 쿠폰의 큐를 <b>언제</b> 지워도 되는지 답한다 (7.3).
 *
 * <p>지우는 일은 어댑터가 한다. 여기는 판단만 하므로 레디스 없이 잴 수 있다.
 */
public final class SoldOutCleanup {

    /** 쿠폰별로 매진을 연달아 본 틱 수. 재고가 돌아오면 지운다. */
    private final Map<String, Integer> seen = new HashMap<>();

    /** 이미 지운 쿠폰. <b>지운 것이 확인된 뒤에</b> 들어온다. */
    private final Set<String> deleted = new HashSet<>();

    /** 이번 판에 세기 시작한 쿠폰. 줄 옆에 울타리 표를 세울 대상이다. */
    private final List<String> claimed = new ArrayList<>();

    private final int graceTicks;
    private final Counter dropped;
    private final Counter cancelled;
    private final Counter failed;

    private SoldOutCleanup(int graceTicks, MeterRegistry meters) {
        // **0 이면 유예가 아니다.** 값으로 끄면 그 사실이 설정 어디에도 안
        // 드러나고, 마지막 폴링이 줄을 잃는 것이 정상으로 읽힌다.
        if (graceTicks <= 0) {
            throw new IllegalArgumentException("유예 틱은 양수여야 한다: " + graceTicks);
        }
        Objects.requireNonNull(meters, "meters 는 필수다");
        this.graceTicks = graceTicks;
        this.dropped = meters.counter("waiting.soldout.cleanup", "outcome", "dropped");
        // **취소가 0 이면 안전 장치가 죽어 있다는 뜻이다** (7.3.2b). 그것을
        // 알 방법이 이 계수뿐이다 — 쿠폰 ID 는 라벨로 못 쓴다 (LG-4).
        this.cancelled = meters.counter("waiting.soldout.cleanup", "outcome", "cancelled");
        this.failed = meters.counter("waiting.soldout.cleanup", "outcome", "failed");
    }

    public static SoldOutCleanup of(int graceTicks, MeterRegistry meters) {
        return new SoldOutCleanup(graceTicks, meters);
    }

    /**
     * 이번 틱에 큐를 지워도 되는 쿠폰들.
     *
     * <p><b>재료에 없는 쿠폰의 셈은 버린다.</b> 안 버리면 활성 목록을 드나드는
     * 쿠폰이 옛 셈을 이어받아 유예를 다 안 채우고 지워진다.
     */
    public List<String> due(Map<String, CouponState> coupons) {
        seen.keySet().retainAll(coupons.keySet());
        deleted.retainAll(coupons.keySet());
        claimed.clear();
        List<String> due = new ArrayList<>();
        coupons.forEach((couponId, state) -> {
            if (!state.soldOut()) {
                cancelIfCounting(couponId);
                return;
            }
            // **대기자가 남았을 때가 지울 때다.** "줄이 빈 뒤에 지운다" 로
            // 쓰면 영영 안 돈다 — 매진 쿠폰은 크레딧이 0 이라 아무도 입장으로
            // 안 빠지고, 폴링은 게이트웨이가 종결하므로 큐에서 빼는 스크립트가
            // 안 돌고, 스위퍼는 매진 중 멈춘다. `waiting` 을 줄이는 주체가
            // 하나도 없다. 줄을 지우는 것이 곧 `waiting` 을 0 으로 만드는 일이다.
            if (deleted.contains(couponId)) {
                return;
            }
            int ticks = seen.merge(couponId, 1, Integer::sum);
            if (ticks == 1) {
                // **세기 시작한 것을 줄 옆에 알린다** (CY-766). 울타리 표는
                // 지웠을 때만 생기므로, 아직 한 번도 안 지운 줄에는 표가 없다 —
                // 얼었다 깨어난 옛 리더가 그 줄을 지우는 것을 못 막는다. 후보로
                // 올리는 순간 표를 세워야 그 뒤의 옛 판이 걸린다.
                claimed.add(couponId);
            }
            if (ticks > graceTicks) {
                due.add(couponId);
            }
        });
        return List.copyOf(due);
    }

    /**
     * 이번 판에 <b>세기 시작한</b> 쿠폰들. {@link #due} 뒤에 읽는다.
     *
     * <p>울타리 표는 지웠을 때만 생기므로, 한 번도 안 지운 줄에는 표가 없다.
     * 후보로 올리는 순간 표를 세워야 그 뒤에 오는 옛 판이 걸린다.
     */
    public List<String> claimed() {
        return List.copyOf(claimed);
    }

    /**
     * 지운 것이 확인됐다.
     *
     * <p><b>시도 전에 표시하면 재시도가 영영 없다.</b> 부분 실패가 "다음 틱에
     * 다시 온다" 가 아니라 영구 누수가 된다.
     */
    public void dropped(List<String> couponIds) {
        deleted.addAll(couponIds);
        couponIds.forEach(id -> dropped.increment());
    }

    /** 못 지웠다. 셈을 남겨 두어 다음 틱에 다시 시도한다. */
    public void failed(List<String> couponIds) {
        couponIds.forEach(id -> failed.increment());
    }

    /**
     * 리더가 됐다. <b>셈을 처음부터 준다.</b>
     *
     * <p>비리더 구간에 얼어 있던 셈을 이어 쓰면 유예가 설정값이 아니라 "내가
     * 리더였던 틱 수" 가 된다 — 그 둘은 장애 중에 완전히 갈린다.
     */
    public void leadershipAcquired() {
        seen.clear();
        deleted.clear();
    }

    private void cancelIfCounting(String couponId) {
        // 재고가 돌아왔다. 셈과 표시를 둘 다 버려 삭제를 취소한다 (7.3.2b).
        if (seen.remove(couponId) != null || deleted.remove(couponId)) {
            cancelled.increment();
        }
    }
}

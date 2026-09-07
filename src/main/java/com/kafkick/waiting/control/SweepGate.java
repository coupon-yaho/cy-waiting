package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import java.util.ArrayList;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 잘못 쓸면 되돌릴 수 없다 — 이탈자로 잘못 판정하면 재입장이 새 score 다. */
public final class SweepGate {

    /** 멈춘 쿠폰과 다시 쓸 수 있게 되는 틱. <b>한 틱이 아니다</b> — 아래 팩토리 참조. */
    private final Map<String, Long> resumeAt = new HashMap<>();

    // **`long` 이다.** 1ms 틱이면 `int` 는 25일 만에 넘치고, 그 경계에서
    // 더한 값이 음수가 되어 유예가 통째로 풀린다.
    private long tick;

    private final long resumeDelayTicks;

    /** 승계 유예의 길이. 픽스처가 산식을 두 벌 갖지 않게 열어 둔다. */
    long resumeDelayTicks() {
        return resumeDelayTicks;
    }

    private SweepGate(long resumeDelayTicks) {
        if (resumeDelayTicks < 1) {
            throw new IllegalArgumentException("재개 유예는 양수여야 한다: " + resumeDelayTicks);
        }
        this.resumeDelayTicks = resumeDelayTicks;
    }

    /** 재개 유예를 신호 수명과 폴링 간격에서 끌어온다 — 따로 적으면 갈린다. */
    public static SweepGate of(Duration tick, Duration aliveTtl) {
        // **밀리초로 잰다.** 초로 나누면 1초 미만 틱이 0 이 되고, 나눗셈이
        // 무한이 되어 유예가 사실상 영원이 된다 — 청소가 조용히 멎는다.
        long delayMillis = aliveTtl.plus(PollIntervalPolicy.maxInterval()).toMillis();
        long tickMillis = tick.toMillis();
        if (tickMillis <= 0) {
            throw new IllegalArgumentException("틱은 1ms 이상이어야 한다: " + tick);
        }
        return new SweepGate(Math.ceilDiv(delayMillis, tickMillis));
    }

    /**
     * 리더가 되면 <b>유예를 처음부터 준다.</b> 새 리더는 생존 신호가 얼마나 멎었는지
     * 모르는데, 모른다는 것이 걷을 이유가 되면 안 된다 — 걷힌 사람은 새 score 로 다시 서서
     * 순번이 뒤로 간다. 틱을 안 되돌리면 되찾은 회차가 이미 유예를 넘어 있다.
     */
    public void leadershipAcquired() {
        tick = 0;
        resumeAt.clear();
    }

    /**
     * 승계 유예 중인가. <b>앞줄 제거만 접는다</b> — 대상까지 비우면 만료 신호와 유예 기록이
     * 한 방향으로만 자라 커서가 전진을 못 하고, 승계가 유예보다 잦으면 청소가 영영 안 돈다.
     */
    public boolean removalHeld() {
        return tick <= resumeDelayTicks;
    }

    /**
     * 이번 틱에 정리할 쿠폰들. <b>유예와 무관하다</b> — 만료 신호와 유예 기록을
     * 걷는 일은 줄에서 사람을 빼지 않으므로 승계에 안전하다.
     */
    public List<String> cleanable(Map<String, CouponState> coupons) {
        return coupons.keySet().stream().sorted().toList();
    }

    /**
     * 이번 틱에 쓸어도 되는 쿠폰들.
     *
     * @param dataStale 재료가 낡았는가. <b>노드 전체에 걸리는 조건</b>이라 쿠폰별이 아니다
     */
    public List<String> sweepable(Map<String, CouponState> coupons, boolean dataStale) {
        tick++;
        if (removalHeld()) {
            return List.of();
        }
        List<String> sweepable = new ArrayList<>();
        coupons.forEach((couponId, state) -> {
            // **매진이거나 재고를 모르는 동안은 멈춘다.** 그 쿠폰의 폴링은
            // 게이트웨이가 종결해 생존 신호의 유일한 갱신처가 멎고, 줄 선 전원의 신호가
            // 일제히 끊긴다 — 리더만 미상으로 보면 여기서 안 멈춘다.
            if (dataStale || state.soldOut() || !state.stockKnown()) {
                resumeAt.put(couponId, tick + resumeDelayTicks);
                return;
            }
            // **풀린 뒤 유예만큼 건너뛴다.** 그 구간은 밀렸던 폴링이
            // 아직 안 왔다. 한 틱만 쉬면 신호를 못 채운 사람을 걷는다.
            Long at = resumeAt.get(couponId);
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

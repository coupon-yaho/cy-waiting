package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.CouponState;
import java.util.ArrayList;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 잘못 쓸면 되돌릴 수 없다 — 이탈자로 잘못 판정하면 재입장이 새 score 다 (7.4.8·7.4.9). */
public final class SweepGate {

    /** 멈춘 쿠폰과 다시 쓸 수 있게 되는 틱. <b>한 틱이 아니다</b> — 아래 팩토리 참조. */
    private final Map<String, Long> resumeAt = new HashMap<>();

    // **`long` 이다.** 1ms 틱이면 `int` 는 25일 만에 넘치고, 그 경계에서
    // 더한 값이 음수가 되어 유예가 통째로 풀린다.
    private long tick;

    private final long resumeDelayTicks;

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
     * 이번 틱에 쓸어도 되는 쿠폰들.
     *
     * @param dataStale 재료가 낡았는가. <b>노드 전체에 걸리는 조건</b>이라
     *                  쿠폰별이 아니다
     */
    public List<String> sweepable(Map<String, CouponState> coupons, boolean dataStale) {
        tick++;
        // **갓 만들어진 게이트는 승계 직후다** (CY-822). 유예는 이 객체 안에만
        // 있어 리더가 바뀌면 사라지고, 새 리더는 그 쿠폰의 신호가 얼마나 오래
        // 멎어 있었는지 모른다. 모른다는 것이 걷을 이유가 되면 안 된다 —
        // 걷힌 사람은 새 score 로 다시 서므로 순번이 뒤로 간다.
        //
        // **쿠폰별이 아니라 노드 전체에 건다.** 모르는 것이 이 쿠폰 하나가
        // 아니기 때문이다. 그동안 이탈자가 줄에 남아 크레딧이 허공에 나가지만,
        // 그건 되돌릴 수 있고 순번 역행은 못 되돌린다.
        if (tick <= resumeDelayTicks) {
            resumeAt.keySet().retainAll(coupons.keySet());
            return List.of();
        }
        List<String> sweepable = new ArrayList<>();
        coupons.forEach((couponId, state) -> {
            // **매진 중에는 멈춘다.** 7.1 이 매진 조회를 게이트웨이에서
            // 종결하면서 그 쿠폰의 폴링은 생존 신호를 안 갱신한다. 갱신처가
            // 거기 하나뿐이라, 매진으로 보이는 동안 줄 선 전원의 신호가
            // 일제히 멎는다 — 장애 문단과 글자 그대로 같은 사슬이다.
            //
            // **재고를 모르는 동안도 같다** (CY-702). 표시가 노드에 안 닿는
            // 구간이 있다 — 발행이 상한을 넘어 버렸거나, 롤아웃 중 옛 노드가
            // 예약 자리를 건너뛸 때다. 그 노드는 매진으로 읽고 종결하므로
            // 신호가 멎는데, 리더만 미상으로 보면 여기서 안 멈춘다.
            if (dataStale || state.soldOut() || !state.stockKnown()) {
                resumeAt.put(couponId, tick + resumeDelayTicks);
                return;
            }
            // **풀린 뒤 유예만큼 건너뛴다** (7.4.9). 그 구간은 밀렸던 폴링이
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

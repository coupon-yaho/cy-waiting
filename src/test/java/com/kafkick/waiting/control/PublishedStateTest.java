package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.QueueMode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 발행이 무엇을 싣는가 (CY-962 · CY-963).
 *
 * <p>승계 시나리오의 판정이 전부 발행 해시에서 나온다. 그 해시가 어느 회차의 상태를 싣는지가 그 판정 전부의
 * 전제라, 여기서 못 박는다.
 */
class PublishedStateTest {

    private static final Instant 지금 = Instant.parse("2026-09-18T00:00:00Z");

    private static final List<CouponDemand> 수요 =
            List.of(new CouponDemand("c1", 100, 1_000_000, QueueMode.ADAPTIVE));

    private final AtomicReference<Map<String, String>> 발행된_것 = new AtomicReference<>();

    private final AtomicLong 관측 = new AtomicLong();

    private final SnapshotCodec codec = SnapshotCodec.create();

    /** 회차 사이에 이월되는 평활 상태. 매번 새로 시작하면 계수가 먹은 값이 안 나온다. */
    private final AtomicReference<CreditSmoother.Snapshot> 평활 =
            new AtomicReference<>(CreditSmoother.Snapshot.empty());

    private AllocationRound 회차() {
        return AllocationRound.of(() -> true,
                () -> Mono.just(new TimedDemands(수요, 지금.getEpochSecond())),
                관측::get, () -> 1,
                grant -> Mono.just(grant.credit()),
                hash -> {
                    발행된_것.set(hash);
                    평활.set(codec.smoothing(hash));
                    return Mono.empty();
                },
                () -> 지금,
                () -> Mono.just(CreditSmoother.restore(0.3, 평활.get())),
                codec, () -> 0);
    }

    /**
     * <b>발행에 실리는 평활은 그 회차의 것이다.</b> 앞 회차 값을 실으면 그것을 읽는 시나리오의 시계열이
     * 통째로 한 칸 밀리는데, 밀린 시계열에서도 모양 판정은 전부 성립해 아무도 못 본다.
     */
    @Test
    @DisplayName("발행은_그_회차의_평활을_싣는다")
    void 발행은_그_회차의_평활을_싣는다() {
        관측.set(1_000);
        회차().run().block();
        // 두 회차를 돌려 계수가 먹은 값으로 잰다. 첫 회차는 관측이 그대로 초기값이라 둘이 안 갈린다.
        관측.set(5_000);
        AllocationRound 둘째 = 회차();

        둘째.run().block();

        assertThat(codec.smoothing(발행된_것.get()).value())
                .as("앞 회차 값을 실으면 시계열이 한 칸 밀린다")
                .isEqualTo(둘째.smoothedCredit());
        assertThat(codec.smoothing(발행된_것.get()).value())
                .as("계수가 먹은 값이라 관측과 다르다").isNotEqualTo(5_000.0);
    }

    /**
     * <b>지금은 빈 값이다.</b> 히스테리시스를 배선하는 날 발행 쪽을 같이 안 고치면 스냅샷이 매 틱 이월을
     * 덮어써, 승계마다 큐 켜짐꺼짐이 다시 처음부터 선다.
     */
    @Test
    @DisplayName("발행은_아직_빈_히스테리시스를_싣는다")
    void 발행은_아직_빈_히스테리시스를_싣는다() {
        관측.set(1_000);

        회차().run().block();

        assertThat(codec.hysteresis(발행된_것.get()))
                .as("제품이 히스테리시스를 돌리기 시작하면 이 자리를 같이 고친다")
                .isEqualTo(QueueingHysteresis.Snapshot.empty());
    }
}

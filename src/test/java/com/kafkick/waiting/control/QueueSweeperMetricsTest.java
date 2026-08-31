package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 청소 계수. <b>G7.5 판정이 이 숫자 하나에 걸려 있다</b> (7.4.6).
 *
 * <p>걷은 수가 곧 게이트의 합불이므로, 그 값이 남의 값을 읽거나 아예 안 오르면
 * 게이트가 무엇을 지켰는지 말할 수 없게 된다.
 */
class QueueSweeperMetricsTest {

    private static final String METRIC = "waiting.sweep";

    /** 갈래마다 다른 수를 돌려준다 — 같으면 서로를 읽는 배선이 안 잡힌다. */
    private static final QueueSweeper.SweepResult 결과 =
            new QueueSweeper.SweepResult(3, 5, 7, 11);

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private QueueSweeper sweeper(QueueSweeper.SweepResult r) {
        return QueueSweeper.of(
                SweepGates.warmed(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                (ids, limit, removeFront) -> Mono.just(r), meters);
    }

    private double 계수(String kind) {
        return meters.counter(METRIC, "kind", kind).count();
    }

    private Map<String, CouponState> 줄이_있는_쿠폰() {
        return Map.of("c1", CouponStates.queueing(10, 1_000, 100));
    }

    @Test
    @DisplayName("갈래마다_자기_수를_센다")
    void 갈래마다_자기_수를_센다() {
        sweeper(결과).run(줄이_있는_쿠폰(), false).block();

        assertThat(계수("swept")).as("걷은 수").isEqualTo(3);
        assertThat(계수("expired-signal")).as("만료 신호").isEqualTo(5);
        assertThat(계수("expired-grace")).as("낡은 기록").isEqualTo(7);
        assertThat(계수("failed")).as("실패").isEqualTo(11);
    }

    /**
     * <b>걷을 게 없어서 0 과 터져서 0 을 가른다.</b> 안 가르면 청소가 멎은 것이
     * 정상으로 보인다 — 스위퍼가 한 명도 안 걷던 구간이 정확히 그 모양이었다.
     */
    @Test
    @DisplayName("터지면_실패로_세고_걷은_수는_안_올린다")
    void 터지면_실패로_세고_걷은_수는_안_올린다() {
        QueueSweeper 터지는_것 = QueueSweeper.of(
                SweepGates.warmed(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                (ids, limit, removeFront) -> Mono.error(new IllegalStateException("레디스가 끊겼다")),
                meters);

        assertThat(터지는_것.run(줄이_있는_쿠폰(), false).block())
                .as("배분을 막지 않는다").isEqualTo(QueueSweeper.SweepResult.NOTHING);

        assertThat(계수("failed")).as("실패").isEqualTo(1);
        assertThat(계수("swept")).as("걷은 수").isZero();
    }

    /** 쓸 대상이 없으면 왕복도 계수도 없다. 그 판을 세면 평시 값이 부푼다. */
    @Test
    @DisplayName("쓸_대상이_없으면_아무것도_안_센다")
    void 쓸_대상이_없으면_아무것도_안_센다() {
        sweeper(결과).run(줄이_있는_쿠폰(), true).block();

        assertThat(계수("swept")).as("낡은 재료에서는 안 쓴다").isZero();
        assertThat(계수("failed")).as("실패도 아니다").isZero();
    }
}

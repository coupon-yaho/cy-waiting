package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 대기열을 꺼도 <b>이미 줄 선 사람을 추월시키지 않는다</b> (불변식 4).
 *
 * <p>{@code mode} 는 운영자 정책이고 {@code waiting} 은 기계 관측이라 서로
 * 독립이다. 둘이 어긋난 상태 — <b>줄이 있는데 꺼진</b> — 가 실제로 생기고,
 * 그때 우회를 그대로 적용하면 낡은 스냅샷 경로에서 막은 것을 여기서 뚫는다.
 */
class OffModeQueueTest {

    private static final long NOW = 1_800_000_000L;

    private static AdmissionDecider 판정기() {
        return AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(1000), 0.1);
    }

    private static AdmissionRequest 신규유입(CouponState state) {
        return new AdmissionRequest("c1", state, new SnapshotMeta(1000, 1),
                false, false, false, NOW, 300);
    }

    @Test
    @DisplayName("줄이_있는데_꺼져도_신규유입은_우회하지_않는다")
    void 줄이_있는데_꺼져도_신규유입은_우회하지_않는다() {
        // 운영자가 껐지만 줄에 2만 명이 남아 있다. 우회시키면 그 2만 명을
        // 신규 유입이 통째로 추월하고, 재고도 신규 유입이 먹는다.
        CouponState 줄이있는데꺼짐 = new CouponState(
                QueueMode.OFF, RuntimeState.QUEUEING, 500, 10_000, 20_000, 1.0);

        assertThat(판정기().decide(신규유입(줄이있는데꺼짐)))
                .isNotEqualTo(AdmissionDecision.PASS_BYPASS);
    }

    @Test
    @DisplayName("줄이_있는데_꺼지면_뒤에_세운다")
    void 줄이_있는데_꺼지면_뒤에_세운다() {
        // 뒤에 세워야 기존 줄이 빠지는 동안 순서가 유지된다. OFF 는 배분에
        // 관여하지 않으므로 줄은 정상적으로 빠지고, 비면 그때 우회가 산다.
        CouponState 줄이있는데꺼짐 = new CouponState(
                QueueMode.OFF, RuntimeState.QUEUEING, 500, 10_000, 100, 1.0);

        assertThat(판정기().decide(신규유입(줄이있는데꺼짐)))
                .isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }

    @Test
    @DisplayName("줄이_비면_꺼진_쿠폰은_우회한다")
    void 줄이_비면_꺼진_쿠폰은_우회한다() {
        // 이것이 OFF 의 본래 목적이다 — 막으면 기능이 사라진다.
        CouponState 줄이빈꺼짐 = new CouponState(
                QueueMode.OFF, RuntimeState.IDLE, 0, 10_000, 0, 1.0);

        assertThat(판정기().decide(신규유입(줄이빈꺼짐)))
                .isEqualTo(AdmissionDecision.PASS_BYPASS);
    }

    @Test
    @DisplayName("방금_줄_선_사람도_우회로_빠지지_않는다")
    void 방금_줄_선_사람도_우회로_빠지지_않는다() {
        // waiting 은 아직 0 인데 이 요청이 방금 줄에 들어갔다. 우회시키면
        // 자기가 방금 선 줄을 자기가 추월한다.
        CouponState 줄이빈꺼짐 = new CouponState(
                QueueMode.OFF, RuntimeState.IDLE, 0, 10_000, 0, 1.0);
        AdmissionRequest 방금등록 = new AdmissionRequest("c1", 줄이빈꺼짐,
                new SnapshotMeta(1000, 1), false, false, true, NOW, 300);

        assertThat(판정기().decide(방금등록))
                .isNotEqualTo(AdmissionDecision.PASS_BYPASS);
    }
}

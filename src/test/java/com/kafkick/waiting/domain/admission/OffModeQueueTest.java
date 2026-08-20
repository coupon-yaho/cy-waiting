package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
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
        // 부정 단언은 나머지 13개 판정값 아무거나 통과시킨다 (TS-11).
        // 용량이 넉넉하면 뒤에 서고, 크레딧이 말라 용량이 0 이면 가득 참이다.
        CouponState 여유있음 = CouponStates.offWithQueue(500, 10_000, 20_000);
        CouponState 크레딧없음 = CouponStates.offWithQueue(0, 10_000, 20_000);

        assertThat(판정기().decide(신규유입(여유있음)))
                .isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
        assertThat(판정기().decide(신규유입(크레딧없음)))
                .isEqualTo(AdmissionDecision.REJECT_QUEUE_FULL);
    }

    @Test
    @DisplayName("줄이_있는데_꺼지면_뒤에_세운다")
    void 줄이_있는데_꺼지면_뒤에_세운다() {
        // 뒤에 세워야 기존 줄이 빠지는 동안 순서가 유지된다. OFF 는 배분에
        // 관여하지 않으므로 줄은 정상적으로 빠지고, 비면 그때 우회가 산다.
        CouponState 줄이있는데꺼짐 = CouponStates.offWithQueue(500, 10_000, 100);

        assertThat(판정기().decide(신규유입(줄이있는데꺼짐)))
                .isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }


    @Test
    @DisplayName("낡은_구간에서는_꺼진_쿠폰도_상한_안에서만_통과한다")
    void 낡은_구간에서는_꺼진_쿠폰도_상한_안에서만_통과한다() {
        // **fail-open 상한을 우회하면 안 된다.** 스케줄러가 멎은 구간은
        // 상태를 모르는 구간이고, 그래서 상한이 있다. 꺼진 쿠폰만 무제한으로
        // 뒷단에 꽂히면 그 상한이 있으나 마나다.
        AdmissionDecider decider = 판정기();
        CouponState 줄이빈꺼짐 = CouponStates.off(10_000);

        int 통과 = 0;
        for (int i = 0; i < 5_000; i++) {
            AdmissionRequest 낡음 = new AdmissionRequest("c1", 줄이빈꺼짐,
                    new SnapshotMeta(1000, 1), true, false, false, NOW, 300);
            if (decider.decide(낡음) != AdmissionDecision.REJECT_OVERLOAD) {
                통과++;
            }
        }

        assertThat(통과)
                .withFailMessage("낡은 구간에서 %d 건이 나갔다 — 상한을 우회했다", 통과)
                .isLessThanOrEqualTo(1_000);
    }

    @Test
    @DisplayName("낡지_않았으면_꺼진_쿠폰은_상한과_무관하게_통과한다")
    void 낡지_않았으면_꺼진_쿠폰은_상한과_무관하게_통과한다() {
        // 이것이 OFF 의 본래 목적이다 — 상한을 걸면 기능이 사라진다.
        AdmissionDecider decider = 판정기();

        for (int i = 0; i < 5_000; i++) {
            assertThat(decider.decide(신규유입(CouponStates.off(10_000))))
                    .isEqualTo(AdmissionDecision.PASS_BYPASS);
        }
    }

    @Test
    @DisplayName("줄이_빠지면_우회가_다시_산다")
    void 줄이_빠지면_우회가_다시_산다() {
        // "남은 줄은 정상적으로 빠지고 비는 순간 우회가 다시 산다" 는 주장은
        // 회복까지 봐야 검증된 것이다. 줄이 있는 동안과 빠진 뒤를 잇는다.
        assertThat(판정기().decide(신규유입(CouponStates.offWithQueue(500, 10_000, 100))))
                .isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);

        assertThat(판정기().decide(신규유입(CouponStates.off(10_000))))
                .isEqualTo(AdmissionDecision.PASS_BYPASS);
    }

    @Test
    @DisplayName("줄이_비면_꺼진_쿠폰은_우회한다")
    void 줄이_비면_꺼진_쿠폰은_우회한다() {
        // 이것이 OFF 의 본래 목적이다 — 막으면 기능이 사라진다.
        CouponState 줄이빈꺼짐 = CouponStates.off(10_000);

        assertThat(판정기().decide(신규유입(줄이빈꺼짐)))
                .isEqualTo(AdmissionDecision.PASS_BYPASS);
    }

    @Test
    @DisplayName("방금_줄_선_사람도_우회로_빠지지_않는다")
    void 방금_줄_선_사람도_우회로_빠지지_않는다() {
        // waiting 은 아직 0 인데 이 요청이 방금 줄에 들어갔다. 우회시키면
        // 자기가 방금 선 줄을 자기가 추월한다.
        AdmissionRequest 방금등록 = new AdmissionRequest("c1", CouponStates.off(10_000),
                new SnapshotMeta(1000, 1), false, false, true, NOW, 300);

        assertThat(판정기().decide(방금등록))
                .isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }
}

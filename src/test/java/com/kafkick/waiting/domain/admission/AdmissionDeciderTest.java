package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 판정 사다리 — 순서가 곧 정책이다.
 *
 * <p>각 줄에는 앞줄보다 먼저 와야 하는 이유가 있다. 이전 구현이 무너진 곳도
 * 정확히 여기였다.
 */
class AdmissionDeciderTest {

    private static final SnapshotMeta META = new SnapshotMeta(1000, 10);
    private static final double IDLE_RATIO = 0.7;

    private AdmissionDecider decider() {
        return new AdmissionDecider(SecondWindowLimiter.withMaxKeys(1000), IDLE_RATIO);
    }

    private AdmissionRequest request(CouponState state) {
        return new AdmissionRequest("c1", state, META, false, false, false, 0, 100);
    }

    @Test
    @DisplayName("재고가_없으면_스냅샷이_낡아도_매진으로_종결한다")
    void 재고가_없으면_스냅샷이_낡아도_매진으로_종결한다() {
        // 1번이 맨 앞이어야 한다. dataStale 뒤에 두면 매진 쿠폰이
        // fail-open 상한을 갉아먹는다.
        AdmissionRequest req = request(CouponStates.closed(100)).withDataStale(true);

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.REJECT_SOLD_OUT);
    }

    @Test
    @DisplayName("토큰을_든_사람은_상태와_무관하게_통과한다")
    void 토큰을_든_사람은_상태와_무관하게_통과한다() {
        AdmissionRequest req = request(CouponStates.queueing(100, 500, 3000)).withValidToken(true);

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.PASS_TOKEN);
    }

    @Test
    @DisplayName("대기열이_꺼져_있으면_붐벼도_통과한다")
    void 대기열이_꺼져_있으면_붐벼도_통과한다() {
        assertThat(decider().decide(request(CouponStates.off(500))))
                .isEqualTo(AdmissionDecision.PASS_BYPASS);
    }

    @Test
    @DisplayName("스냅샷이_낡고_줄이_비어_있으면_상한_안에서_통과시킨다")
    void 스냅샷이_낡고_줄이_비어_있으면_상한_안에서_통과시킨다() {
        AdmissionRequest req = request(CouponStates.idle(500)).withDataStale(true);

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.PASS_FAIL_OPEN);
    }

    @Test
    @DisplayName("스냅샷이_낡아도_줄_선_사람이_있으면_추월시키지_않는다")
    void 스냅샷이_낡아도_줄_선_사람이_있으면_추월시키지_않는다() {
        // F1 — 이전 구현이 무너진 지점. 상태를 모른다는 것이 추월의 사유가 아니다.
        AdmissionRequest req = request(CouponStates.queueing(100, 500, 5000)).withDataStale(true);

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.ENQUEUE_STALE);
    }

    @Test
    @DisplayName("큐가_꽉_차면_큐로_가는_경로보다_먼저_거절한다")
    void 큐가_꽉_차면_큐로_가는_경로보다_먼저_거절한다() {
        // credit 100 · maxEta 100 → 용량 10000. 그보다 많이 서 있다.
        AdmissionRequest req = request(CouponStates.queueing(100, 500, 20_000));

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.REJECT_QUEUE_FULL);
    }

    @Test
    @DisplayName("항상_큐_모드는_한산해도_줄을_세운다")
    void 항상_큐_모드는_한산해도_줄을_세운다() {
        CouponState always =
                new CouponState(QueueMode.ALWAYS, RuntimeState.IDLE, 0, 500, 0, 1.0);

        assertThat(decider().decide(request(always))).isEqualTo(AdmissionDecision.ENQUEUE_ALWAYS);
    }

    @Test
    @DisplayName("이미_붐비면_뒤에_선다")
    void 이미_붐비면_뒤에_선다() {
        assertThat(decider().decide(request(CouponStates.queueing(100, 500, 3000))))
                .isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }

    @Test
    @DisplayName("방금_큐로_보냈으면_스냅샷이_IDLE이어도_막는다")
    void 방금_큐로_보냈으면_스냅샷이_IDLE이어도_막는다() {
        // 래치. 스냅샷이 따라잡기 전 한 틱 동안 추월이 생긴다.
        AdmissionRequest req = request(CouponStates.idle(500)).withJustEnqueued(true);

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }

    @Test
    @DisplayName("한산한_쿠폰은_credit이_0이어도_대기열_없이_통과한다")
    void 한산한_쿠폰은_credit이_0이어도_대기열_없이_통과한다() {
        // R1 — 이 제품의 존재 이유. G2.1 이 판정하는 자리다.
        CouponState idle = CouponStates.idle(500);

        assertThat(idle.credit()).isZero();
        assertThat(decider().decide(request(idle))).isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
    }

    @Test
    @DisplayName("토큰을_들어도_노드_상한을_넘으면_큐가_아니라_재시도다")
    void 토큰을_들어도_노드_상한을_넘으면_큐가_아니라_재시도다() {
        // F8 — 축적된 토큰이 회복 직후 한꺼번에 들어온다. 그렇다고 큐 뒤로
        // 보내면 이미 차례가 온 사람의 허가가 "아마도" 가 된다.
        AdmissionDecider d = decider();
        AdmissionRequest req =
                request(CouponStates.queueing(100, 500, 3000)).withValidToken(true);

        for (int i = 0; i < 100; i++) {
            assertThat(d.decide(req)).isEqualTo(AdmissionDecision.PASS_TOKEN);
        }

        assertThat(d.decide(req)).isEqualTo(AdmissionDecision.RETRY_TOKEN);
    }

    @Test
    @DisplayName("낡은_상태의_fail_open도_상한을_넘으면_거절한다")
    void 낡은_상태의_fail_open도_상한을_넘으면_거절한다() {
        // 무제한 통과가 아니다. 상한이 없으면 fail-open 이 곧 전면 개방이다.
        AdmissionDecider d = decider();
        AdmissionRequest req = request(CouponStates.idle(500)).withDataStale(true);

        for (int i = 0; i < 100; i++) {
            assertThat(d.decide(req)).isEqualTo(AdmissionDecision.PASS_FAIL_OPEN);
        }

        assertThat(d.decide(req)).isEqualTo(AdmissionDecision.REJECT_OVERLOAD);
    }

    @Test
    @DisplayName("노드_예산이_먼저_마르면_전역_사유로_큐에_간다")
    void 노드_예산이_먼저_마르면_전역_사유로_큐에_간다() {
        // 쿠폰 상한(70)보다 노드 상한이 작으면 전역이 먼저 마른다.
        // 대응이 다르다 — 이때는 노드를 늘려야 한다.
        AdmissionDecider d = new AdmissionDecider(SecondWindowLimiter.withMaxKeys(1000), 5.0);
        CouponState idle = CouponStates.idle(500);

        for (int i = 0; i < 100; i++) {
            assertThat(d.decide(request(idle))).isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
        }

        assertThat(d.decide(request(idle)))
                .isEqualTo(AdmissionDecision.ENQUEUE_RATE_GLOBAL);
    }

    @Test
    @DisplayName("한산한_쿠폰도_상한을_넘으면_초과분만_큐로_간다")
    void 한산한_쿠폰도_상한을_넘으면_초과분만_큐로_간다() {
        // globalCredit 1000 / 노드 10 × 0.7 = 70 이 상한이다.
        AdmissionDecider d = decider();
        CouponState idle = CouponStates.idle(500);

        int passed = 0;
        for (int i = 0; i < 200; i++) {
            if (d.decide(request(idle)) == AdmissionDecision.PASS_UNDER_CAP) {
                passed++;
            }
        }

        assertThat(passed).isEqualTo(70);
        assertThat(d.decide(request(idle))).isEqualTo(AdmissionDecision.ENQUEUE_RATE_COUPON);
    }
}

package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.coupon.CouponState;
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
        return AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(1000), IDLE_RATIO);
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
    @DisplayName("대기열이_꺼져_있고_줄이_비면_통과한다")
    void 대기열이_꺼져_있고_줄이_비면_통과한다() {
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
        assertThat(decider().decide(request(CouponStates.always(500))))
                .isEqualTo(AdmissionDecision.ENQUEUE_ALWAYS);
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
        AdmissionDecider d = AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(1000), 5.0);
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

    @Test
    @DisplayName("줄_길이가_큐_상한과_정확히_같으면_거절한다")
    void 줄_길이가_큐_상한과_정확히_같으면_거절한다() {
        // credit 100 · maxEta 100 → 용량 10000. 딱 그만큼 서 있다.
        // 경계를 초과로만 잡으면 상한을 한 명씩 넘긴다.
        AdmissionRequest req = request(CouponStates.queueing(100, 500, 10_000));

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.REJECT_QUEUE_FULL);
    }

    @Test
    @DisplayName("줄_길이가_큐_상한보다_하나_적으면_받는다")
    void 줄_길이가_큐_상한보다_하나_적으면_받는다() {
        AdmissionRequest req = request(CouponStates.queueing(100, 500, 9_999));

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }

    @Test
    @DisplayName("잘못된_설정은_만들_때_막는다")
    void 잘못된_설정은_만들_때_막는다() {
        // 비율은 10번 줄에서만 쓰인다. 여기서 안 막으면 잘못된 설정으로도
        // 토큰·bypass·fail-open 이 정상으로 돌아가다가, 한산한 쿠폰 요청
        // 하나에서 원인과 먼 곳에서 터진다.
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10);

        assertThatThrownBy(() -> AdmissionDecider.of(null, 0.7))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AdmissionDecider.of(limiter, -0.1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AdmissionDecider.of(limiter, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("쿠폰_키가_전역_키와_같아도_예산이_합쳐지지_않는다")
    void 쿠폰_키가_전역_키와_같아도_예산이_합쳐지지_않는다() {
        // 접두사가 없으면 쿠폰 ID 하나가 전역 키와 같아지는 순간 두 예산이
        // 한 카운터로 합쳐져, 다른 쿠폰의 전역 트래픽이 이 쿠폰 몫을 먹는다.
        AdmissionDecider d = decider();
        CouponState idle = CouponStates.idle(500);
        AdmissionRequest req =
                new AdmissionRequest("node:", idle, META, false, false, false, 0, 100);

        int passed = 0;
        for (int i = 0; i < 200; i++) {
            if (d.decide(req) == AdmissionDecision.PASS_UNDER_CAP) {
                passed++;
            }
        }

        // 합쳐졌다면 min(70, 100) = 70 이 아니라 절반인 35 만 통과한다
        assertThat(passed).isEqualTo(70);
    }

    @Test
    @DisplayName("한산_비율_0은_유효한_설정이다")
    void 한산_비율_0은_유효한_설정이다() {
        // 운영자가 한산 통과를 완전히 잠그는 값이다. 거부하면 그 조작이 막힌다.
        AdmissionDecider d = AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(10), 0.0);

        assertThat(d.decide(request(CouponStates.idle(500))))
                .isEqualTo(AdmissionDecision.ENQUEUE_RATE_COUPON);
    }

    @Test
    @DisplayName("쿠폰이_다르면_유휴_예산도_따로다")
    void 쿠폰이_다르면_유휴_예산도_따로다() {
        // 키를 뭉개면 먼저 온 쿠폰이 전체 유휴 몫을 먹고 나머지가 굶는다.
        AdmissionDecider d = decider();
        CouponState idle = CouponStates.idle(500);
        AdmissionRequest first =
                new AdmissionRequest("c1", idle, META, false, false, false, 0, 100);
        AdmissionRequest second =
                new AdmissionRequest("c2", idle, META, false, false, false, 0, 100);

        for (int i = 0; i < 70; i++) {
            assertThat(d.decide(first)).isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
        }

        // c1 이 자기 몫을 다 썼어도 c2 는 아직 자기 몫이 남아 있다
        assertThat(d.decide(first)).isEqualTo(AdmissionDecision.ENQUEUE_RATE_COUPON);
        assertThat(d.decide(second)).isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
    }

    @Test
    @DisplayName("래치는_풀리면_무대기_통과를_되돌려준다")
    void 래치는_풀리면_무대기_통과를_되돌려준다() {
        // G2.17 — 래치가 죽은 분기를 만들면 그 노드에서 R1 이 영영 죽는다.
        // 같은 상태에서 래치만 내리면 통과가 복귀해야 한다.
        AdmissionDecider d = decider();
        AdmissionRequest latched = request(CouponStates.idle(500)).withJustEnqueued(true);

        assertThat(d.decide(latched)).isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
        assertThat(d.decide(latched.withJustEnqueued(false)))
                .isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
    }

    @Test
    @DisplayName("토큰_보유자는_쿠폰_상한이_말라도_통과한다")
    void 토큰_보유자는_쿠폰_상한이_말라도_통과한다() {
        // G2.14 — 배분 시점에 이미 크레딧을 썼다. 여기서 쿠폰 상한을 또 걸면
        // 차례가 온 사람이 자기 몫을 못 쓰고 되돌려진다.
        AdmissionDecider d = decider();
        CouponState idle = CouponStates.idle(500);

        // 이 쿠폰의 유휴 몫(70)을 먼저 말린다
        for (int i = 0; i < 70; i++) {
            assertThat(d.decide(request(idle))).isEqualTo(AdmissionDecision.PASS_UNDER_CAP);
        }
        assertThat(d.decide(request(idle))).isEqualTo(AdmissionDecision.ENQUEUE_RATE_COUPON);

        // 토큰을 든 사람은 그것과 무관하게 통과한다
        assertThat(d.decide(request(idle).withValidToken(true)))
                .isEqualTo(AdmissionDecision.PASS_TOKEN);
    }

    /** META 는 전역 크레딧 1,000 을 게이트웨이 10대가 나눠 쓴다 — 노드 몫은 100 이다. */
    @Test
    @DisplayName("배수_속도를_알면_그것이_줄_길이_상한이다")
    void 배수_속도를_알면_그것이_줄_길이_상한이다() {
        CouponState 줄선_쿠폰 = CouponStates.queueing(3, 1_000, 10);

        assertThat(AdmissionDecider.queueCapacity(줄선_쿠폰, META, 600)).isEqualTo(1_800);
    }

    /**
     * 배분을 아직 못 받은 구간이다. 0 을 그대로 상한으로 쓰면 줄이 한 번도
     * 안 생기고 쿠폰이 그 상태에 갇힌다.
     */
    @Test
    @DisplayName("배수_속도를_모르면_노드_몫으로_잰다")
    void 배수_속도를_모르면_노드_몫으로_잰다() {
        assertThat(CouponStates.idle(1_000).queueCapacity(600)).isZero();

        // 100 × 600. 상한 없음(-1)도 0 도 아닌, 노드가 감당하는 양이다.
        assertThat(AdmissionDecider.queueCapacity(CouponStates.idle(1_000), META, 600))
                .isEqualTo(60_000);
    }

    /**
     * 스냅샷이 낡은 동안에는 대기 인원이 영영 0 으로 보인다. 여기서 상한을
     * 없애면 장애가 지속되는 내내 줄이 무한히 자란다 (R5).
     */
    @Test
    @DisplayName("노드_몫이_0_이라도_상한은_양수다")
    void 노드_몫이_0_이라도_상한은_양수다() {
        SnapshotMeta 굶은_노드 = new SnapshotMeta(1, 10);

        assertThat(AdmissionDecider.globalCap(굶은_노드)).isZero();
        // **값으로 못 박는다.** 0 보다 크다고만 재면 하한을 1,000 으로 올려도
        // 아무도 안 막는다 — 못 빼는 줄에 1,000 명을 받는 것이 이 자리를 만든
        // 이유와 정면으로 어긋난다.
        assertThat(AdmissionDecider.queueCapacity(CouponStates.idle(1_000), 굶은_노드, 600))
                .isEqualTo(1);
    }

    /**
     * 줄이 이미 선 쿠폰은 폴백을 안 쓴다. 배분이 살아나면 다음 틱에 크레딧을
     * 받으므로 갇히는 고리가 없고, 뺄 수 없다고 아는 줄에 더 세우지 않는다.
     */
    @Test
    @DisplayName("줄이_이미_섰는데_배수를_못_하면_거절한다")
    void 줄이_이미_섰는데_배수를_못_하면_거절한다() {
        // 배분 적용이 실패하면 크레딧 0 이 실린 채로 발행된다.
        AdmissionRequest req = request(CouponStates.offWithQueue(0, 1_000, 1));

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.REJECT_QUEUE_FULL);
    }

    /**
     * 원 함수는 최대 대기 시간이 0 이하면 0 을 준다 — 받아 줄 줄이 없다는 뜻이다.
     * 래퍼가 그 0 을 "배수 속도를 모른다" 로 읽으면 가드가 뒤집혀 폴백으로 내려가고,
     * 하한 1 때문에 <b>아무도 안 받아야 할 자리에서 한 명을 받는다.</b>
     */
    @Test
    @DisplayName("최대_대기_시간이_0_이하면_아무도_안_받는다")
    void 최대_대기_시간이_0_이하면_아무도_안_받는다() {
        assertThat(CouponStates.idle(1_000).queueCapacity(0)).isZero();

        assertThat(AdmissionDecider.queueCapacity(CouponStates.idle(1_000), META, 0)).isZero();
        assertThat(AdmissionDecider.queueCapacity(CouponStates.idle(1_000), META, -600)).isZero();
    }

    @Test
    @DisplayName("곱이_넘쳐도_음수가_되지_않는다")
    void 곱이_넘쳐도_음수가_되지_않는다() {
        // 음수가 되면 스크립트가 오류를 낸다. 그 오류는 fail-open 으로 흘러
        // **닫히는 게 아니라 열린다** — 줄 선 사람을 추월한다.
        SnapshotMeta 거대한_노드 = new SnapshotMeta(Long.MAX_VALUE, 1);

        assertThat(AdmissionDecider.queueCapacity(CouponStates.idle(1_000), 거대한_노드, 600))
                .isEqualTo(Long.MAX_VALUE);
    }
}

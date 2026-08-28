package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.coupon.Tunables;
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

        assertThat(AdmissionDecider.queueCapacity(줄선_쿠폰, 600)).isEqualTo(1_800);
    }

    /**
     * 배분을 아직 못 받은 구간이다. 0 을 그대로 상한으로 쓰면 줄이 한 번도
     * 안 생기고 쿠폰이 그 상태에 갇힌다.
     */
    @Test
    @DisplayName("배수_속도를_모르면_최소_배수로_잰다")
    void 배수_속도를_모르면_최소_배수로_잰다() {
        assertThat(CouponStates.idle(1_000).queueCapacity(600)).isZero();

        // 초당 한 명. 아는 것이 없을 때 가정할 수 있는 가장 낮은 배수 속도다.
        assertThat(AdmissionDecider.queueCapacity(CouponStates.idle(1_000), 600))
                .isEqualTo(600);
    }

    /**
     * 줄이 이미 선 쿠폰은 폴백을 안 쓴다. credit 0 으로 굳는 구간은 있지만
     * 그때도 뺄 수 없다고 아는 줄에 더 세우느니 거절이 맞다.
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
     * 가드가 없으면 음수가 폴백을 그대로 타고 나가고, 스크립트가 오류를 내고,
     * 그 오류는 fail-open 으로 흘러 <b>닫히는 게 아니라 열린다.</b>
     */
    @Test
    @DisplayName("최대_대기_시간이_0_이하면_아무도_안_받는다")
    void 최대_대기_시간이_0_이하면_아무도_안_받는다() {
        // 0 은 이 가드가 없어도 0 이다 — 곱이 대신 지킨다. 가드가 지탱하는 것은 음수다.
        assertThat(AdmissionDecider.queueCapacity(CouponStates.idle(1_000), -600)).isZero();
    }

    /**
     * <b>운영자가 켠 대기열은 낡음이 못 끈다.</b> 3번이 앞서면 `ALWAYS` 쿠폰이
     * 낡은 구간에서 통째로 우회한다 — 그리고 아무도 큐에 안 들어가니
     * {@code hasQueue} 가 영영 거짓이라 그 상태가 스스로 유지된다.
     *
     * <p>운영자가 `ALWAYS` 를 거는 순간이 바로 리더가 흔들리는 오픈 직후다.
     */
    @Test
    @DisplayName("낡아도_항상_대기는_줄을_세운다")
    void 낡아도_항상_대기는_줄을_세운다() {
        AdmissionRequest 낡음 = request(CouponStates.always(10_000)).withDataStale(true);

        assertThat(decider().decide(낡음)).isEqualTo(AdmissionDecision.ENQUEUE_ALWAYS);
    }

    /** 꺼진 쿠폰은 반대다. 낡은 구간의 상한이 그 우회보다 앞에 있어야 한다. */
    @Test
    @DisplayName("낡으면_꺼진_쿠폰은_상한을_먼저_탄다")
    void 낡으면_꺼진_쿠폰은_상한을_먼저_탄다() {
        AdmissionRequest 낡음 = request(CouponStates.off(10_000)).withDataStale(true);

        assertThat(decider().decide(낡음)).isEqualTo(AdmissionDecision.PASS_FAIL_OPEN);
    }

    /**
     * <b>항상 대기라도 찬 줄에는 안 세운다.</b> 안 걸면 거절 대상 하나하나가
     * 레디스 왕복을 시도하고, 레디스가 느린 구간에서는 그 왕복이 전부 타임아웃해
     * fail-open 으로 흘러 뒷단 트래픽 생성기가 된다.
     *
     * <p><b>경계에서 잰다.</b> 여유가 많은 줄로 재면 이 줄이 상한을 어떻게
     * 산출하든 통과한다.
     */
    @Test
    @DisplayName("항상_대기라도_줄이_차면_거절한다")
    void 항상_대기라도_줄이_차면_거절한다() {
        // credit 0 이면 용량은 폴백(1 × 600)이다. 딱 그만큼 찬 줄을 만든다.
        CouponState 찬_줄 = CouponStates.alwaysWithQueue(0, 10_000, 600);
        AdmissionRequest req = new AdmissionRequest("c1", 찬_줄, META, false, false, false, 0, 600);

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.REJECT_QUEUE_FULL);
    }

    /**
     * <b>배분이 아직 안 돈 구간에서 한 명 때문에 전원이 막히면 안 된다.</b>
     * 6번의 참을 그대로 쓰면 `credit == 0` 에서 용량이 0 이 되어, 대기자 하나에
     * `ALWAYS` 가 하는 일이 사라진다 — 운영자가 이 값을 거는 오픈 직후가 정확히
     * 그 구간이다 (C-8).
     */
    @Test
    @DisplayName("배분_전에도_항상_대기는_줄을_세운다")
    void 배분_전에도_항상_대기는_줄을_세운다() {
        CouponState 배분_전 = CouponStates.alwaysWithQueue(0, 10_000, 1);
        AdmissionRequest req = new AdmissionRequest("c1", 배분_전, META, false, false, false, 0, 600);

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.ENQUEUE_ALWAYS);
    }

    /** 안 찼으면 그대로 세운다. 위 시험만 있으면 항상 거절해도 통과한다. */
    @Test
    @DisplayName("항상_대기는_줄이_안_차면_세운다")
    void 항상_대기는_줄이_안_차면_세운다() {
        CouponState 여유 = CouponStates.alwaysWithQueue(100, 10_000, 100);
        AdmissionRequest req = new AdmissionRequest("c1", 여유, META, false, false, false, 0, 600);

        assertThat(decider().decide(req)).isEqualTo(AdmissionDecision.ENQUEUE_ALWAYS);
    }

    /**
     * <b>한산 통과의 밑변은 쿠폰 credit 이 아니다.</b> 9번이 통과시키는 것은
     * IDLE 쿠폰뿐이고 IDLE 이면 credit 이 0 이다 (I1). credit 으로 재는 순간
     * 한산한 쿠폰일수록 조여진다 — 이전 구현의 핵심 버그다.
     */
    @Test
    @DisplayName("한산_통과가_쓴_예산은_한산_몫이다")
    void 한산_통과가_쓴_예산은_한산_몫이다() {
        CouponState 한산 = CouponStates.idle(500);

        assertThat(decider().admittedRatePerSec(AdmissionDecision.PASS_UNDER_CAP, 한산, META))
                .isEqualTo(한산.idleCap(META, IDLE_RATIO))
                .isGreaterThan(한산.credit());
    }

    /**
     * <b>2번 줄은 쿠폰별 상한을 안 건다</b> (B-14). 배분 시점에 이미 크레딧을 썼기
     * 때문이다. 여기서 쿠폰 몫을 돌려주면 격벽이 사다리가 안 건 상한을 새로 걸어,
     * 차례가 온 사람이 자기 차례에 다시 막힌다.
     */
    @Test
    @DisplayName("토큰_통과가_쓴_예산은_노드_예산이다")
    void 토큰_통과가_쓴_예산은_노드_예산이다() {
        CouponState 줄선_것 = CouponStates.queueing(100, 500, 3000);

        assertThat(decider().admittedRatePerSec(AdmissionDecision.PASS_TOKEN, 줄선_것, META))
                .isEqualTo(AdmissionDecider.globalCap(META))
                // 쿠폰 몫과 다른 값이어야 한다. 같으면 무엇을 재는지 알 수 없다.
                .isNotEqualTo(줄선_것.contendedCap(META.effectiveGatewayCount()));
    }

    /**
     * 4·5번은 쿠폰별 예산을 안 거친다. 노드 예산이 그 경로의 정직한 상한이다 —
     * 여기서 0 을 돌려주면 낡음 구간의 통과가 통째로 막힌다.
     */
    @Test
    @DisplayName("우회와_장애_개방이_쓴_예산은_노드_예산이다")
    void 우회와_장애_개방이_쓴_예산은_노드_예산이다() {
        assertThat(decider().admittedRatePerSec(
                AdmissionDecision.PASS_BYPASS, CouponStates.off(500), META))
                .isEqualTo(AdmissionDecider.globalCap(META));
        assertThat(decider().admittedRatePerSec(
                AdmissionDecision.PASS_FAIL_OPEN, CouponStates.idle(500), META))
                .isEqualTo(AdmissionDecider.globalCap(META));
    }

    /** 통과가 아닌 판정에 예산을 물으면 부르는 쪽이 틀린 것이다. 조용히 0 을 주면 전면 차단이다. */
    @Test
    @DisplayName("통과가_아닌_판정에는_예산이_없다")
    void 통과가_아닌_판정에는_예산이_없다() {
        assertThatThrownBy(() -> decider().admittedRatePerSec(
                AdmissionDecision.ENQUEUE_BACKLOG, CouponStates.queueing(1, 10, 5), META))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>배포 없이 되돌릴 수 있어야 롤백이 성립합니다</b> (P-1). 재료에 실려 온
     * 한산 몫이 기동 설정을 이깁니다 — 그 전파 경로가 스냅샷입니다.
     */
    @Test
    @DisplayName("실려_온_한산_몫이_기동값을_이긴다")
    void 실려_온_한산_몫이_기동값을_이긴다() {
        CouponState 한산 = CouponStates.idle(500);
        SnapshotMeta 실려_온_것 = new SnapshotMeta(META.globalCredit(), META.gatewayCount(),
                new Tunables(0.2, 3));

        assertThat(decider().admittedRatePerSec(
                AdmissionDecision.PASS_UNDER_CAP, 한산, 실려_온_것))
                .isEqualTo(한산.idleCap(실려_온_것, 0.2))
                .isNotEqualTo(한산.idleCap(META, IDLE_RATIO));
    }

}

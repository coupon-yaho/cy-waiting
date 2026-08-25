package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.AdmissionDecision;
import com.kafkick.waiting.domain.admission.AdmissionRequest;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 발행이 실은 모드가 <b>판정까지 살아 간다</b>.
 *
 * <p>모드 필드만 재면 사슬의 한 마디만 본 것이다. 배분이 만든 상태를 코덱에
 * 태워 판정기에 먹여야, 운영자 설정이 실제로 무엇을 바꾸는지가 잡힌다.
 */
class PublishedModeDecisionTest {

    private static final long NOW = 1_800_000_000L;

    private final Map<String, Map<String, String>> 발행 = new LinkedHashMap<>();

    private CouponState 한_판_돌린다(CouponDemand 수요, long 전역_크레딧) {
        AllocationRound.of(
                        () -> true,
                        () -> Mono.just(List.of(수요)),
                        () -> 전역_크레딧, () -> 1,
                        grant -> Mono.just(grant.credit()),
                        hash -> {
                            발행.put("last", hash);
                            return Mono.empty();
                        },
                        () -> Instant.ofEpochSecond(1_700_000_000L),
                        () -> Mono.just(CreditSmoother.of(1.0)),
                        SnapshotCodec.create(), () -> 0L)
                .run().block();
        return SnapshotCodec.create().decode(발행.get("last")).coupons().get(수요.couponId());
    }

    private AdmissionDecision 신규유입(CouponState state) {
        return AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(1000), 0.1)
                .decide(new AdmissionRequest("c1", state, new SnapshotMeta(1000, 1),
                        false, false, false, NOW, 300));
    }

    /**
     * <b>이 분기는 처음 도달 가능해진 지 얼마 안 됐다.</b> 전에는 배분이 줄 없는 쿠폰을
     * 늘 적응형으로 실어서, 사다리 5번이 운영에서 죽은 줄이었다.
     */
    @Test
    @DisplayName("꺼진_쿠폰의_빈_줄은_우회로_간다")
    void 꺼진_쿠폰의_빈_줄은_우회로_간다() {
        CouponState 실린_것 =
                한_판_돌린다(new CouponDemand("c1", 0, 10_000, QueueMode.OFF), 10_000);

        assertThat(신규유입(실린_것)).isEqualTo(AdmissionDecision.PASS_BYPASS);
    }

    /** 7번도 같다. 발행이 ALWAYS 를 못 실으면 운영자가 켠 대기열이 안 켜진다. */
    @Test
    @DisplayName("항상_대기_쿠폰은_한산해도_줄을_세운다")
    void 항상_대기_쿠폰은_한산해도_줄을_세운다() {
        CouponState 실린_것 =
                한_판_돌린다(new CouponDemand("c1", 0, 10_000, QueueMode.ALWAYS), 10_000);

        assertThat(신규유입(실린_것)).isEqualTo(AdmissionDecision.ENQUEUE_ALWAYS);
    }

    /** 껐어도 줄이 남아 있으면 뒤에 세운다. 새 팩토리에서도 불변식 4 가 산다. */
    @Test
    @DisplayName("꺼진_쿠폰도_줄이_남으면_뒤에_세운다")
    void 꺼진_쿠폰도_줄이_남으면_뒤에_세운다() {
        CouponState 실린_것 =
                한_판_돌린다(new CouponDemand("c1", 500, 10_000, QueueMode.OFF), 10_000);

        assertThat(신규유입(실린_것)).isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);
    }

    /**
     * <b>줄이 빠지는 그 판이 이 티켓의 실제 위험이다.</b> 같은 쿠폰이 한 틱 만에
     * "뒤에 서라" 에서 "그냥 지나가라" 로 바뀐다. 그 전이가 실제로 일어나는지를
     * 여기서 본다 — 노드 사이의 공백은 CY-582 로 따로 뗐다.
     */
    @Test
    @DisplayName("줄이_빠지면_다음_판에_우회가_열린다")
    void 줄이_빠지면_다음_판에_우회가_열린다() {
        CouponState 줄_있음 =
                한_판_돌린다(new CouponDemand("c1", 500, 10_000, QueueMode.OFF), 10_000);
        assertThat(신규유입(줄_있음)).isEqualTo(AdmissionDecision.ENQUEUE_BACKLOG);

        CouponState 줄_빠짐 =
                한_판_돌린다(new CouponDemand("c1", 0, 10_000, QueueMode.OFF), 10_000);

        assertThat(신규유입(줄_빠짐)).isEqualTo(AdmissionDecision.PASS_BYPASS);
    }
}

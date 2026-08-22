package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 리더가 발행한 것을 각 노드가 그대로 읽어야 한다.
 *
 * <p>평활화 상태도 같이 싣는다. 리더가 바뀔 때마다 0 에서 다시 시작하면 그 순간
 * 표시 ETA 가 튀는데, <b>회복 직후가 진동하기 가장 쉬운 구간</b>이라 하필 그때
 * 흔들린다 (F9).
 */
class SnapshotEncodeTest {

    private final SnapshotCodec codec = SnapshotCodec.create();

    private static CouponState 대기(long credit, long waiting) {
        return CouponState.offWithQueue(credit, 1_000, waiting);
    }

    @Test
    @DisplayName("쓴_것을_그대로_읽는다")
    void 쓴_것을_그대로_읽는다() {
        GatewaySnapshot 원본 = new GatewaySnapshot(
                Map.of("c1", 대기(3, 10)),
                new SnapshotMeta(50, 4),
                Instant.ofEpochSecond(1_700_000_000L));

        GatewaySnapshot 되읽음 = codec.decode(codec.encode(원본, CreditSmoother.Snapshot.empty()));

        assertThat(되읽음.coupons()).containsOnlyKeys("c1");
        assertThat(되읽음.coupons().get("c1")).isEqualTo(원본.coupons().get("c1"));
        assertThat(되읽음.meta()).isEqualTo(원본.meta());
        assertThat(되읽음.publishedAt()).isEqualTo(원본.publishedAt());
    }

    @Test
    @DisplayName("평활화_상태를_실어_다음_리더에게_넘긴다")
    void 평활화_상태를_실어_다음_리더에게_넘긴다() {
        CreditSmoother 앞선_리더 = CreditSmoother.of(0.3);
        앞선_리더.observe(100);
        앞선_리더.observe(40);

        Map<String, String> 실린_것 = codec.encode(
                new GatewaySnapshot(Map.of(), new SnapshotMeta(0, 1), Instant.EPOCH),
                앞선_리더.snapshot());
        CreditSmoother 새_리더 = CreditSmoother.restore(0.3, codec.smoothing(실린_것));

        // 이월받았으면 다음 관측이 평활화되고, 못 받았으면 그 값이 그대로 초기값이 된다.
        assertThat(새_리더.observe(40)).isEqualTo(앞선_리더.observe(40));
    }

    @Test
    @DisplayName("평활화_상태가_없으면_안_받은_것으로_본다")
    void 평활화_상태가_없으면_안_받은_것으로_본다() {
        // 첫 리더이거나 옛 형식이다. 0 을 받은 것으로 세면 첫 몇 틱이 실제보다
        // 한참 낮게 나가고 그동안 표시 ETA 가 몇 배로 뛴다.
        assertThat(codec.smoothing(Map.of())).isEqualTo(CreditSmoother.Snapshot.empty());
    }

    @Test
    @DisplayName("평활화_상태가_깨졌으면_안_받은_것으로_본다")
    void 평활화_상태가_깨졌으면_안_받은_것으로_본다() {
        // 이월받은 값이 NaN 이면 그 순간부터 평활화가 영영 NaN 이고 표시 ETA 도
        // 같이 죽는다. 리더가 바뀐 뒤에야 드러난다.
        assertThat(codec.smoothing(Map.of("#ewma", "NaN", "#ewmaSeeded", "1")))
                .isEqualTo(CreditSmoother.Snapshot.empty());
        assertThat(codec.smoothing(Map.of("#ewma", "-1", "#ewmaSeeded", "1")))
                .isEqualTo(CreditSmoother.Snapshot.empty());
        assertThat(codec.smoothing(Map.of("#ewma", "여덟", "#ewmaSeeded", "1")))
                .isEqualTo(CreditSmoother.Snapshot.empty());
    }

    @Test
    @DisplayName("관측_전_상태는_값을_안_싣는다")
    void 관측_전_상태는_값을_안_싣는다() {
        // 관측 전인데 값이 0 이 아니면 도메인이 거부한다. 실을 때부터 안 맞으면
        // 다음 리더가 이월을 통째로 버린다.
        Map<String, String> 실린_것 = codec.encode(
                new GatewaySnapshot(Map.of(), new SnapshotMeta(0, 1), Instant.EPOCH),
                CreditSmoother.Snapshot.empty());

        assertThat(codec.smoothing(실린_것).seeded()).isFalse();
        assertThat(codec.smoothing(실린_것).value()).isZero();
    }

    @Test
    @DisplayName("쿠폰_ID_가_전역값을_덮지_않는다")
    void 쿠폰_ID_가_전역값을_덮지_않는다() {
        // 예약 접두사를 단 쿠폰 하나로 전 쿠폰의 몫이 0 이 된다.
        //
        // **전역값을 나중에 쓴다는 사실에 기대면 안 된다.** 그러면 아는 이름만
        // 덮이고, 모르는 예약 이름은 쓰레기 필드로 남아 전 노드가 영영 걸러야 한다.
        GatewaySnapshot 원본 = new GatewaySnapshot(
                Map.of("#credit", 대기(3, 10), "#훗날쓸값", 대기(1, 2)),
                new SnapshotMeta(50, 4),
                Instant.ofEpochSecond(1_700_000_000L));

        Map<String, String> 실린_것 = codec.encode(원본, CreditSmoother.Snapshot.empty());

        assertThat(codec.decode(실린_것).meta().globalCredit()).isEqualTo(50);
        assertThat(실린_것).doesNotContainKey("#훗날쓸값");
    }

    @Test
    @DisplayName("발행하면_발행됨으로_보인다")
    void 발행하면_발행됨으로_보인다() {
        Map<String, String> 실린_것 = codec.encode(
                new GatewaySnapshot(Map.of(), new SnapshotMeta(0, 1),
                        Instant.ofEpochSecond(1_700_000_000L)),
                CreditSmoother.Snapshot.empty());

        assertThat(codec.isPublished(실린_것)).isTrue();
    }

    @Test
    @DisplayName("노드_수_경계를_지킨다")
    void 노드_수_경계를_지킨다() {
        // long→int 축소가 조용히 0 을 만들면 전 노드가 "내가 유일하다" 고 믿어
        // 크레딧을 노드 수만큼 초과 배분한다.
        assertThat(codec.decode(Map.of("#nodes", "1")).meta().gatewayCount()).isEqualTo(1);
        assertThat(codec.decode(Map.of("#nodes", "0")).meta().gatewayCount()).isEqualTo(1);
        assertThat(codec.decode(Map.of("#nodes", String.valueOf(Integer.MAX_VALUE)))
                .meta().gatewayCount()).isEqualTo(Integer.MAX_VALUE);
        assertThat(codec.decode(Map.of("#nodes", String.valueOf(Integer.MAX_VALUE + 1L)))
                .meta().gatewayCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("발행_시각_경계를_지킨다")
    void 발행_시각_경계를_지킨다() {
        // 0 이하나 표현 범위 밖이면 어떤 임계로도 낡음이어야 한다. 그래야
        // 스케줄러가 멎은 것을 각 노드가 알아챈다.
        assertThat(codec.decode(Map.of("#published", "1")).publishedAt())
                .isEqualTo(Instant.ofEpochSecond(1));
        assertThat(codec.decode(Map.of("#published", "0")).publishedAt()).isEqualTo(Instant.EPOCH);
        assertThat(codec.decode(Map.of("#published", "-1")).publishedAt()).isEqualTo(Instant.EPOCH);
        assertThat(codec.decode(Map.of("#published", String.valueOf(Instant.MAX.getEpochSecond())))
                .publishedAt()).isEqualTo(Instant.ofEpochSecond(Instant.MAX.getEpochSecond()));
        assertThat(codec.decode(Map.of("#published",
                String.valueOf(Instant.MAX.getEpochSecond() + 1))).publishedAt())
                .isEqualTo(Instant.EPOCH);
    }

    @Test
    @DisplayName("못_읽는_수는_기본값으로_본다")
    void 못_읽는_수는_기본값으로_본다() {
        // 모르는데 크게 잡으면 초과 배분이다. 크레딧은 0, 노드 수는 1 이다.
        assertThat(codec.decode(Map.of("#credit", " 12 ")).meta().globalCredit()).isEqualTo(12);
        assertThat(codec.decode(Map.of("#credit", "열둘")).meta().globalCredit()).isZero();
        assertThat(codec.decode(Map.of("#nodes", "열둘")).meta().gatewayCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("모드와_상태를_그대로_싣는다")
    void 모드와_상태를_그대로_싣는다() {
        GatewaySnapshot 원본 = new GatewaySnapshot(
                Map.of("c1", new CouponState(QueueMode.ALWAYS, RuntimeState.DRAINING, 9, 100, 5, 2.0)),
                new SnapshotMeta(9, 1), Instant.ofEpochSecond(1_700_000_000L));

        CouponState 되읽음 = codec.decode(codec.encode(원본, CreditSmoother.Snapshot.empty()))
                .coupons().get("c1");

        assertThat(되읽음.mode()).isEqualTo(QueueMode.ALWAYS);
        assertThat(되읽음.runtime()).isEqualTo(RuntimeState.DRAINING);
        assertThat(되읽음.pollScale()).isEqualTo(2.0);
    }
}

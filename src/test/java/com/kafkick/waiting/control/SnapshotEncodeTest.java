package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.time.Instant;
import java.util.HashMap;
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

        GatewaySnapshot 되읽음 = codec.decode(codec.encode(원본, CreditSmoother.Snapshot.empty(),
                QueueingHysteresis.Snapshot.empty()));

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
                앞선_리더.snapshot(), QueueingHysteresis.Snapshot.empty());
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
    @DisplayName("히스테리시스_상태가_배선을_왕복한다")
    void 히스테리시스_상태가_배선을_왕복한다() {
        // 배선 이름을 리터럴로 못 박는다. 인코더와 디코더가 같은 상수를 쓰므로
        // 오타가 나도 왕복은 맞아떨어진다 — 옛 리더가 실은 것을 새 배포가
        // 못 읽는 것은 그때 드러난다.
        Map<String, String> 실린_것 = codec.encode(
                new GatewaySnapshot(Map.of(), new SnapshotMeta(0, 1), Instant.EPOCH),
                CreditSmoother.Snapshot.empty(),
                new QueueingHysteresis.Snapshot(true, 2));

        assertThat(실린_것).containsEntry("#queueing", "1").containsEntry("#belowExitTicks", "2");
        assertThat(codec.hysteresis(실린_것))
                .isEqualTo(new QueueingHysteresis.Snapshot(true, 2));
    }

    @Test
    @DisplayName("히스테리시스_상태가_없으면_안_받은_것으로_본다")
    void 히스테리시스_상태가_없으면_안_받은_것으로_본다() {
        // 첫 리더이거나 옛 형식이다.
        assertThat(codec.hysteresis(Map.of()))
                .isEqualTo(QueueingHysteresis.Snapshot.empty());
    }

    @Test
    @DisplayName("유지_틱만_깨졌으면_붙잡던_것은_유지한다")
    void 유지_틱만_깨졌으면_붙잡던_것은_유지한다() {
        // 유지 틱을 못 읽었다고 대기열까지 놓으면 그 순간 꺼졌다 켜진다 —
        // 히스테리시스가 막으려던 진동이 리더 교체마다 난다.
        assertThat(codec.hysteresis(Map.of("#queueing", "1", "#belowExitTicks", "여덟")))
                .isEqualTo(new QueueingHysteresis.Snapshot(true, 0));
        assertThat(codec.hysteresis(Map.of("#queueing", "1")))
                .isEqualTo(new QueueingHysteresis.Snapshot(true, 0));
        assertThat(codec.hysteresis(Map.of("#queueing", "1", "#belowExitTicks", "-1")))
                .isEqualTo(new QueueingHysteresis.Snapshot(true, 0));
    }

    @Test
    @DisplayName("모순된_조합은_안_받은_것으로_본다")
    void 모순된_조합은_안_받은_것으로_본다() {
        // 안 붙잡는데 유지 틱이 쌓여 있다. 그대로 받으면 도메인이 던지고,
        // 던지면 리더가 바뀔 때마다 배분이 멎는다.
        assertThat(codec.hysteresis(Map.of("#queueing", "0", "#belowExitTicks", "2")))
                .isEqualTo(QueueingHysteresis.Snapshot.empty());
    }

    @Test
    @DisplayName("관측_전_상태는_값을_안_싣는다")
    void 관측_전_상태는_값을_안_싣는다() {
        // 관측 전인데 값이 0 이 아니면 도메인이 거부한다. 실을 때부터 안 맞으면
        // 다음 리더가 이월을 통째로 버린다.
        Map<String, String> 실린_것 = codec.encode(
                new GatewaySnapshot(Map.of(), new SnapshotMeta(0, 1), Instant.EPOCH),
                CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());

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

        Map<String, String> 실린_것 = codec.encode(원본, CreditSmoother.Snapshot.empty(),
                QueueingHysteresis.Snapshot.empty());

        assertThat(codec.decode(실린_것).meta().globalCredit()).isEqualTo(50);
        assertThat(실린_것).doesNotContainKey("#훗날쓸값");
    }

    @Test
    @DisplayName("발행하면_발행됨으로_보인다")
    void 발행하면_발행됨으로_보인다() {
        Map<String, String> 실린_것 = codec.encode(
                new GatewaySnapshot(Map.of(), new SnapshotMeta(0, 1),
                        Instant.ofEpochSecond(1_700_000_000L)),
                CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());

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

    /**
     * <b>옛 노드가 읽을 수 있는 모양으로 싣는다.</b>
     *
     * <p>읽는 쪽만 관대하게 만들면 절반만 열린다. 이미 떠 있는 노드는 여섯
     * 필드를 기대하므로, 새 리더가 다섯을 발행하면 그 노드가 전 쿠폰을 떨군다.
     * 관대한 디코더가 먼저 배포된 뒤에 줄인다.
     */
    @Test
    @DisplayName("쿠폰_값이_옛_필드_수를_지킨다")
    void 쿠폰_값이_옛_필드_수를_지킨다() {
        GatewaySnapshot 원본 = new GatewaySnapshot(
                Map.of("c1", new CouponState(QueueMode.ALWAYS, RuntimeState.DRAINING, 9, 100, 5)),
                new SnapshotMeta(9, 1), Instant.ofEpochSecond(1_700_000_000L));

        String 실린_값 = codec.encode(원본, CreditSmoother.Snapshot.empty(),
                QueueingHysteresis.Snapshot.empty()).get("c1");

        assertThat(실린_값.split(":")).as("옛 노드가 기대하는 필드 수").hasSize(6);
        assertThat(실린_값).isEqualTo("ALWAYS:DRAINING:9:100:5:1.0");
    }

    /**
     * <b>옛 노드도 배수를 지킨다.</b>
     *
     * <p>여섯 번째 자리에 상수를 박으면 롤아웃 구간 내내 옛 파드 전부가 배수 없이
     * 폴링한다. 파드 대부분이 아직 옛것인 구간이 있으므로, 새 리더가 "보호가
     * 걸렸다" 고 보고하는 동안 클러스터는 예산을 한참 넘긴 채로 돈다.
     */
    // 옛 노드는 이 자리를 그 쿠폰의 배수로 읽는다. 전역값을 그대로 실으면 그
    // 노드의 계산이 새 노드와 같아진다 — 읽는 쪽이 달라도 답은 같다.
    @Test
    @DisplayName("옛_자리에_실제_배수를_싣는다")
    void 옛_자리에_실제_배수를_싣는다() {
        GatewaySnapshot 배수가_걸린_판 = new GatewaySnapshot(
                Map.of("c1", new CouponState(QueueMode.ALWAYS, RuntimeState.QUEUEING, 9, 100, 50)),
                new SnapshotMeta(9, 1, null, 3.5), Instant.ofEpochSecond(1_700_000_000L));

        String 실린_값 = codec.encode(배수가_걸린_판, CreditSmoother.Snapshot.empty(),
                QueueingHysteresis.Snapshot.empty()).get("c1");

        assertThat(실린_값).as("여섯 번째 자리가 전역 배수와 같다")
                .isEqualTo("ALWAYS:QUEUEING:9:100:50:3.5");
    }

    @Test
    @DisplayName("모드와_상태를_그대로_싣는다")
    void 모드와_상태를_그대로_싣는다() {
        GatewaySnapshot 원본 = new GatewaySnapshot(
                Map.of("c1", new CouponState(QueueMode.ALWAYS, RuntimeState.DRAINING, 9, 100, 5)),
                new SnapshotMeta(9, 1, null, 2.0), Instant.ofEpochSecond(1_700_000_000L));

        GatewaySnapshot 되읽은_판 = codec.decode(codec.encode(원본,
                CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty()));
        CouponState 되읽음 = 되읽은_판.coupons().get("c1");

        assertThat(되읽음.mode()).isEqualTo(QueueMode.ALWAYS);
        assertThat(되읽음.runtime()).isEqualTo(RuntimeState.DRAINING);
        assertThat(되읽은_판.meta().pollScale()).isEqualTo(2.0);
    }

    /**
     * <b>미상은 옛 자리에 안 싣고 새 자리에 싣는다.</b>
     *
     * <p>옛 디코더의 생성자는 음수 재고를 거부하므로 그 항목이 {@code null} 이
     * 되어 스냅샷에서 빠지고, 없는 쿠폰은 판정에서 매진으로 보인다. 그렇다고
     * 양수를 실으면 옛 노드가 그것을 재입고로 읽어 매진 방패를 푼다.
     */
    // 옛 자리에는 0 을 싣는다 — 옛 노드가 오늘 하던 그대로 한다. 미상이라는
    // 사실은 남는 자리에 싣고, 새 노드만 그것을 읽는다 (E-12).
    @Test
    @DisplayName("미상은_옛_노드가_오늘처럼_읽을_값으로_실린다")
    void 미상은_옛_노드가_오늘처럼_읽을_값으로_실린다() {
        Map<String, String> 실린것 = 미상을_싣는다();

        // **자리를 직접 본다.** 되읽기만 보면 새 디코더가 새 자리를 읽어
        // 통과하고, 정작 옛 노드가 무엇을 보는지는 못 잡는다.
        assertThat(실린것.get("c1").split(":"))
                .as("옛 디코더는 여섯에서 끊어 쪼갠다 — 일곱째는 여섯째를 깨뜨린다")
                .hasSize(6);
        assertThat(실린것.get("c1").split(":")[3])
                .as("양수면 옛 노드가 재입고로 읽어 매진 방패를 푼다")
                .isEqualTo("0");
    }

    /** 새 노드는 미상을 미상으로 되읽는다. 못 읽으면 방패도 못 지킨다. */
    @Test
    @DisplayName("미상은_미상으로_되읽힌다")
    void 미상은_미상으로_되읽힌다() {
        CouponState 되읽음 = codec.decode(미상을_싣는다()).coupons().get("c1");

        assertThat(되읽음.stockKnown()).as("해제 가드가 이것을 본다").isFalse();
        assertThat(되읽음.soldOut()).as("모르는 것은 매진이 아니다").isFalse();
        assertThat(되읽음.credit()).as("몫은 그대로 온다").isEqualTo(3);
    }

    /**
     * <b>선에서 미상을 받지는 않는다.</b> 인코더가 안 싣는 모양을 디코더가 받아
     * 주면 계약이 한쪽으로만 열린다 — 그 자리로 들어온 값은 아무도 안 검증한다.
     */
    @Test
    @DisplayName("재고_자리의_음수는_그_쿠폰만_버린다")
    void 재고_자리의_음수는_그_쿠폰만_버린다() {
        Map<String, String> 손상 = new HashMap<>(미상을_싣는다());
        손상.put("c1", "ADAPTIVE:QUEUEING:3:-1:10:1.0");

        assertThat(codec.decode(손상).coupons()).doesNotContainKey("c1");
    }

    /**
     * <b>옛 리더의 판을 미상으로 읽지 않는다.</b> 옛 리더는 미상을 0 으로 접어
     * 보내고 예약 자리를 안 싣는다. 그 판을 미상으로 읽으면 그 리더가 말한
     * 매진이 전부 무시되고, 매진 방패가 통째로 안 걸린다.
     */
    @Test
    @DisplayName("예약_자리가_없으면_아는_것으로_읽는다")
    void 예약_자리가_없으면_아는_것으로_읽는다() {
        Map<String, String> 옛판 = new HashMap<>(미상을_싣는다());
        옛판.keySet().removeIf(f -> f.startsWith("#u:"));

        CouponState 되읽음 = codec.decode(옛판).coupons().get("c1");

        assertThat(되읽음.stockKnown()).isTrue();
        assertThat(되읽음.soldOut()).as("옛 리더가 말한 매진이 그대로 선다").isTrue();
    }

    /**
     * <b>전역 자리가 미상 표시를 흉내 내면 안 된다.</b> {@code #u:total} 같은
     * 전역이 생기면 {@code total} 이라는 쿠폰이 영구히 미상으로 읽히고, 그
     * 쿠폰만 매진 방패가 영영 안 걸린다 — 아무 오류 없이 조용히.
     */
    @Test
    @DisplayName("전역_자리는_미상_표시로_시작하지_않는다")
    void 전역_자리는_미상_표시로_시작하지_않는다() {
        GatewaySnapshot 원본 = new GatewaySnapshot(
                Map.of("c1", CouponStates.queueing(3, 100, 10)),
                new SnapshotMeta(50, 4), Instant.ofEpochSecond(1_700_000_000L));

        Map<String, String> 실린것 = codec.encode(원본, new CreditSmoother.Snapshot(1.0, true),
                new QueueingHysteresis.Snapshot(true, 2));

        assertThat(실린것.keySet())
                .filteredOn(f -> f.startsWith(SnapshotCodec.STOCK_UNKNOWN_FIELD))
                .as("미상 표시는 쿠폰마다 하나뿐이고, 이 판에는 미상이 없다")
                .isEmpty();
    }

    private Map<String, String> 미상을_싣는다() {
        return codec.encode(new GatewaySnapshot(
                        Map.of("c1", CouponState.stockUnknown(QueueMode.ADAPTIVE, 3, 10)),
                        new SnapshotMeta(50, 4), Instant.ofEpochSecond(1_700_000_000L)),
                CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
    }
}

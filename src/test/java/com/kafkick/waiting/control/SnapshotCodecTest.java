package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.QueueMode;
import com.kafkick.waiting.domain.coupon.RuntimeState;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 스냅샷 해시를 판정 재료로 옮긴다.
 *
 * <p><b>밖에서 쓰는 키다.</b> 스케줄러가 쓰고 게이트웨이가 읽는데 둘의 배포
 * 시점이 다르다 — 모르는 필드, 깨진 값, 빠진 값이 <b>정상 입력</b>이다.
 */
class SnapshotCodecTest {

    private static final Instant publishedAtOf = Instant.parse("2026-08-20T00:00:00Z");

    /** 쿠폰 하나: {@code mode:runtime:credit:stock:waiting:pollScale} */
    private static Map<String, String> 해시(String... pairs) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put(pairs[i], pairs[i + 1]);
        }
        return m;
    }

    @Test
    @DisplayName("쿠폰과_전역값을_읽는다")
    void 쿠폰과_전역값을_읽는다() {
        GatewaySnapshot s = SnapshotCodec.create().decode(해시(
                "#credit", "1000",
                "#nodes", "3",
                "#published", "1787184000",
                "#pollScale", "2.5",
                "c1", "ADAPTIVE:QUEUEING:100:500:2000"));

        assertThat(s.meta().globalCredit()).isEqualTo(1000);
        assertThat(s.meta().gatewayCount()).isEqualTo(3);
        // 배수는 판 전체를 보고 나온 값 하나라 전역 항목에 실린다.
        assertThat(s.meta().pollScale()).isEqualTo(2.5);
        assertThat(s.publishedAt()).isEqualTo(publishedAtOf);
        // **레코드 전체를 본다.** 두 필드만 보면 credit·stock 이 조용히
        // 틀려도 초록이다 — 각각 배분 몫과 매진 판정이다.
        assertThat(s.coupons()).containsOnlyKeys("c1");
        assertThat(s.coupons().get("c1")).isEqualTo(new CouponState(
                QueueMode.ADAPTIVE, RuntimeState.QUEUEING, 100, 500, 2000));
    }

    @Test
    @DisplayName("예약_필드는_쿠폰으로_세지_않는다")
    void 예약_필드는_쿠폰으로_세지_않는다() {
        // '#' 로 시작하는 것은 전역값이다. 쿠폰으로 세면 판정이 없는 쿠폰을
        // 만들어 내고, 그 쿠폰은 매진으로 보인다.
        GatewaySnapshot s = SnapshotCodec.create().decode(해시(
                "#credit", "1000", "#nodes", "2", "#published", "1787184000",
                // **값이 쿠폰 모양인 예약 필드**여야 가드를 가른다. 값이
                // 안 읽히면 어차피 버려져서 가드를 지워도 통과한다.
                "#기본값", "ADAPTIVE:IDLE:0:500:0",
                "#뒷판이_추가한_필드", "무엇이든",
                "c1", "ADAPTIVE:IDLE:0:500:0"));

        assertThat(s.coupons()).containsOnlyKeys("c1");
    }

    @Test
    @DisplayName("깨진_쿠폰_하나가_나머지를_버리지_않는다")
    void 깨진_쿠폰_하나가_나머지를_버리지_않는다() {
        // 여기서 던지면 뒷단 하나의 버그가 게이트웨이 전체를 세운다.
        GatewaySnapshot s = SnapshotCodec.create().decode(해시(
                "#credit", "1000", "#nodes", "1", "#published", "1787184000",
                "깨짐", "이건:숫자가:아니다",
                "필드부족", "ADAPTIVE:IDLE",
                // 필드가 남는 것은 여기 없다 — 옛 배포가 실은 재료라
                // 받아 준다 (E-12). `쿠폰에_모르는_필드가_붙어_와도_읽는다`.
                "모르는모드", "그런모드:IDLE:0:1:0",
                "불변식위반", "ADAPTIVE:IDLE:999:500:0",
                // **배분기가 두 값을 다른 시점에 재면 이 조합이 나온다.**
                // credit 을 잰 뒤 waiting 을 재는 사이에 사람이 빠지면
                // QUEUEING 인데 credit >= waiting 이 된다. 떨어지는 것이
                // 맞지만, 떨어진 쿠폰은 판정에서 없는 쿠폰이 되므로
                // 배분기가 한 쌍에서 유도해야 한다 (계획서 4.6.10).
                "런타임모순", "ADAPTIVE:QUEUEING:500:10000:100",
                "c1", "ADAPTIVE:IDLE:0:500:0"));

        assertThat(s.coupons()).containsOnlyKeys("c1");
    }

    @Test
    @DisplayName("전역값이_없으면_보수적으로_읽는다")
    void 전역값이_없으면_보수적으로_읽는다() {
        // 크레딧을 모르면 0 이다. 모르는데 크게 잡으면 초과 배분이 된다.
        GatewaySnapshot s = SnapshotCodec.create().decode(해시("c1", "ADAPTIVE:IDLE:0:500:0"));

        assertThat(s.meta().globalCredit()).isZero();
        assertThat(s.publishedAt()).isEqualTo(Instant.EPOCH);
        assertThat(s.coupons()).containsOnlyKeys("c1");
    }

    @Test
    @DisplayName("빈_해시는_빈_스냅샷이다")
    void 빈_해시는_빈_스냅샷이다() {
        GatewaySnapshot s = SnapshotCodec.create().decode(Map.of());

        assertThat(s.coupons()).isEmpty();
        assertThat(s.publishedAt()).isEqualTo(Instant.EPOCH);
    }


    @Test
    @DisplayName("전역값이_깨져도_스냅샷을_통째로_잃지_않는다")
    void 전역값이_깨져도_스냅샷을_통째로_잃지_않는다() {
        // **여기서 던지면 갱신이 영구히 멎는다.** 스케줄러가 그 필드를 고칠
        // 때까지 매 틱 같은 자리에서 실패하고, 재시작한 노드는 빈 스냅샷에
        // 갇혀 모든 쿠폰이 매진으로 보인다.
        GatewaySnapshot 음수크레딧 = SnapshotCodec.create().decode(해시(
                "#credit", "-1", "#nodes", "2", "#published", "1787184000",
                "c1", "ADAPTIVE:IDLE:0:500:0"));
        assertThat(음수크레딧.meta().globalCredit()).isZero();
        assertThat(음수크레딧.coupons()).containsOnlyKeys("c1");

        GatewaySnapshot 범위밖시각 = SnapshotCodec.create().decode(해시(
                "#credit", "10", "#nodes", "1", "#published", "99999999999999999",
                "c1", "ADAPTIVE:IDLE:0:500:0"));
        assertThat(범위밖시각.publishedAt()).isEqualTo(Instant.EPOCH);
        assertThat(범위밖시각.coupons()).containsOnlyKeys("c1");
    }

    @Test
    @DisplayName("노드_수는_int_범위_밖이면_기본값이다")
    void 노드_수는_int_범위_밖이면_기본값이다() {
        // (int) 축소는 조용히 0 을 만든다 — 4294967296L 이 0 이 된다.
        // 그러면 전 노드가 "내가 유일하다" 고 믿어 크레딧을 노드 수만큼
        // 초과 배분한다.
        GatewaySnapshot s = SnapshotCodec.create().decode(해시(
                "#credit", "1000", "#nodes", "4294967296", "#published", "1787184000",
                "c1", "ADAPTIVE:IDLE:0:500:0"));

        assertThat(s.meta().gatewayCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("콜론이_많아도_전량_할당하지_않는다")
    void 콜론이_많아도_전량_할당하지_않는다() {
        // 길이 검사가 split 뒤면 100만 원소를 만들고 버린다. Redis 값 상한이
        // 512MB 라 필드 하나로 갱신 스레드를 OOM 으로 몰 수 있다.
        String 콜론폭탄 = ":".repeat(200_000);

        assertThat(SnapshotCodec.create().decode(해시("c1", 콜론폭탄)).coupons()).isEmpty();
    }

    @Test
    @DisplayName("빈_해시는_발행된_것으로_보지_않는다")
    void 빈_해시는_발행된_것으로_보지_않는다() {
        // 빈 해시는 장애가 아니라 흔한 상태다 — 데이터 없는 복제본 승격,
        // 키 만료, 리더 재선출 중 재작성. 그때 성공 응답으로 들고 있던 것을
        // 덮으면 "빈 값으로 덮지 않는다" 가 실패 경로에서만 참이 된다.
        assertThat(SnapshotCodec.create().isPublished(Map.of())).isFalse();
        assertThat(SnapshotCodec.create().isPublished(해시("c1", "ADAPTIVE:IDLE:0:9:0")))
                .isFalse();
        assertThat(SnapshotCodec.create().isPublished(해시(
                "#published", "1787184000", "c1", "ADAPTIVE:IDLE:0:9:0"))).isTrue();
    }

    /**
     * <b>옛 배포가 실은 재료도 읽는다.</b>
     *
     * <p>배포는 한 순간에 안 끝난다. 구·신 버전이 섞이는 구간에 형식이 갈리면
     * 신버전이 옛 재료를 통째로 버리고, 그 노드는 발행된 스냅샷을 하나도
     * 못 받아 준비 상태가 안 된다 — 롤아웃이 그 자리에서 멈춘다.
     *
     * <p>E-12 가 같은 결함을 정책 값에서 이미 겪었다. 모르는 필드는 무시한다.
     */
    @Test
    @DisplayName("쿠폰에_모르는_필드가_붙어_와도_읽는다")
    void 쿠폰에_모르는_필드가_붙어_와도_읽는다() {
        // 배수를 쿠폰마다 싣던 옛 형식이다. 여섯 번째를 무시하고 읽어야 한다.
        GatewaySnapshot s = SnapshotCodec.create().decode(해시(
                "#credit", "1000", "#nodes", "3", "#published", "1787184000",
                "c1", "ADAPTIVE:QUEUEING:100:500:2000:2.5"));

        assertThat(s.coupons()).containsOnlyKeys("c1");
        assertThat(s.coupons().get("c1")).isEqualTo(new CouponState(
                QueueMode.ADAPTIVE, RuntimeState.QUEUEING, 100, 500, 2000));
        // 쿠폰에 실려 온 배수는 안 읽는다. 전역 항목이 없으면 배수는 없는 것이다.
        assertThat(s.meta().pollScale()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("필드가_모자라면_그_쿠폰만_뺀다")
    void 필드가_모자라면_그_쿠폰만_뺀다() {
        // 관대함이 한 방향이라는 것을 못 박는다. 모자란 것은 깨진 값이라
        // 뺀다 — 여기까지 받아 주면 자리가 밀린 값을 그대로 믿게 된다.
        GatewaySnapshot s = SnapshotCodec.create().decode(해시(
                "#credit", "1000", "#nodes", "3", "#published", "1787184000",
                "c1", "ADAPTIVE:QUEUEING:100:500",
                "c2", "ADAPTIVE:QUEUEING:100:500:2000"));

        assertThat(s.coupons()).containsOnlyKeys("c2");
    }

    @Test
    @DisplayName("배수가_깨져_있으면_없는_것으로_읽는다")
    void 배수가_깨져_있으면_없는_것으로_읽는다() {
        // 여기서 던지면 그 필드가 고쳐질 때까지 전 노드의 갱신이 멎는다.
        // 크게 잡아도 안 된다 — 예산이 멀쩡한데 전원이 뜸하게 묻고, 그만큼
        // 차례가 온 사실을 늦게 안다.
        SnapshotCodec codec = SnapshotCodec.create();

        assertThat(codec.decode(해시("#pollScale", "열둘", "c1", "ADAPTIVE:IDLE:0:500:0"))
                .meta().pollScale()).as("숫자가 아님").isEqualTo(1.0);
        assertThat(codec.decode(해시("#pollScale", "NaN", "c1", "ADAPTIVE:IDLE:0:500:0"))
                .meta().pollScale()).as("NaN").isEqualTo(1.0);
        assertThat(codec.decode(해시("#pollScale", "Infinity", "c1", "ADAPTIVE:IDLE:0:500:0"))
                .meta().pollScale()).as("무한").isEqualTo(1.0);
        assertThat(codec.decode(해시("c1", "ADAPTIVE:IDLE:0:500:0"))
                .meta().pollScale()).as("안 실려 옴").isEqualTo(1.0);
    }

    @Test
    @DisplayName("모드와_상태는_대소문자를_가리지_않는다")
    void 모드와_상태는_대소문자를_가리지_않는다() {
        // 밖에서 쓰는 값이라 표기가 흔들린다. 여기서 관대한 것이 값싸다.
        GatewaySnapshot s = SnapshotCodec.create().decode(해시(
                "#credit", "10", "#nodes", "1", "#published", "1787184000",
                "c1", "always:idle:0:500:0"));

        assertThat(s.coupons().get("c1").mode()).isEqualTo(QueueMode.ALWAYS);
        assertThat(s.coupons().get("c1").runtime()).isEqualTo(RuntimeState.IDLE);
    }
}

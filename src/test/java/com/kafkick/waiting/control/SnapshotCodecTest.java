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
                "c1", "ADAPTIVE:QUEUEING:100:500:2000:2.5"));

        assertThat(s.meta().globalCredit()).isEqualTo(1000);
        assertThat(s.meta().gatewayCount()).isEqualTo(3);
        assertThat(s.publishedAt()).isEqualTo(publishedAtOf);
        // **레코드 전체를 본다.** 두 필드만 보면 credit·stock·pollScale 이
        // 조용히 틀려도 초록이다 — 각각 배분 몫·매진 판정·폴링 예산이다.
        assertThat(s.coupons()).containsOnlyKeys("c1");
        assertThat(s.coupons().get("c1")).isEqualTo(new CouponState(
                QueueMode.ADAPTIVE, RuntimeState.QUEUEING, 100, 500, 2000, 2.5));
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
                "#기본값", "ADAPTIVE:IDLE:0:500:0:1.0",
                "#뒷판이_추가한_필드", "무엇이든",
                "c1", "ADAPTIVE:IDLE:0:500:0:1.0"));

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
                "필드초과", "ADAPTIVE:IDLE:0:500:0:1.0:뒷판이_늘린_필드",
                "모르는모드", "그런모드:IDLE:0:1:0:1.0",
                "불변식위반", "ADAPTIVE:IDLE:999:500:0:1.0",
                // **배분기가 두 값을 다른 시점에 재면 이 조합이 나온다.**
                // credit 을 잰 뒤 waiting 을 재는 사이에 사람이 빠지면
                // QUEUEING 인데 credit >= waiting 이 된다. 떨어지는 것이
                // 맞지만, 떨어진 쿠폰은 판정에서 없는 쿠폰이 되므로
                // 배분기가 한 쌍에서 유도해야 한다 (계획서 4.6.10).
                "런타임모순", "ADAPTIVE:QUEUEING:500:10000:100:1.0",
                "c1", "ADAPTIVE:IDLE:0:500:0:1.0"));

        assertThat(s.coupons()).containsOnlyKeys("c1");
    }

    @Test
    @DisplayName("전역값이_없으면_보수적으로_읽는다")
    void 전역값이_없으면_보수적으로_읽는다() {
        // 크레딧을 모르면 0 이다. 모르는데 크게 잡으면 초과 배분이 된다.
        GatewaySnapshot s = SnapshotCodec.create().decode(해시("c1", "ADAPTIVE:IDLE:0:500:0:1.0"));

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
                "c1", "ADAPTIVE:IDLE:0:500:0:1.0"));
        assertThat(음수크레딧.meta().globalCredit()).isZero();
        assertThat(음수크레딧.coupons()).containsOnlyKeys("c1");

        GatewaySnapshot 범위밖시각 = SnapshotCodec.create().decode(해시(
                "#credit", "10", "#nodes", "1", "#published", "99999999999999999",
                "c1", "ADAPTIVE:IDLE:0:500:0:1.0"));
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
                "c1", "ADAPTIVE:IDLE:0:500:0:1.0"));

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
        assertThat(SnapshotCodec.create().isPublished(해시("c1", "ADAPTIVE:IDLE:0:9:0:1.0")))
                .isFalse();
        assertThat(SnapshotCodec.create().isPublished(해시(
                "#published", "1787184000", "c1", "ADAPTIVE:IDLE:0:9:0:1.0"))).isTrue();
    }

    @Test
    @DisplayName("모드와_상태는_대소문자를_가리지_않는다")
    void 모드와_상태는_대소문자를_가리지_않는다() {
        // 밖에서 쓰는 값이라 표기가 흔들린다. 여기서 관대한 것이 값싸다.
        GatewaySnapshot s = SnapshotCodec.create().decode(해시(
                "#credit", "10", "#nodes", "1", "#published", "1787184000",
                "c1", "always:idle:0:500:0:1.0"));

        assertThat(s.coupons().get("c1").mode()).isEqualTo(QueueMode.ALWAYS);
        assertThat(s.coupons().get("c1").runtime()).isEqualTo(RuntimeState.IDLE);
    }
}

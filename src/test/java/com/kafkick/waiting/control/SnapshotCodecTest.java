package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

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

    private static final Instant 발행시각 = Instant.parse("2026-08-20T00:00:00Z");

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
                "c1", "ADAPTIVE:QUEUEING:100:500:2000:1.0"));

        assertThat(s.meta().globalCredit()).isEqualTo(1000);
        assertThat(s.meta().gatewayCount()).isEqualTo(3);
        assertThat(s.publishedAt()).isEqualTo(발행시각);
        assertThat(s.coupons()).containsOnlyKeys("c1");
        assertThat(s.coupons().get("c1").runtime()).isEqualTo(RuntimeState.QUEUEING);
        assertThat(s.coupons().get("c1").waiting()).isEqualTo(2000);
    }

    @Test
    @DisplayName("예약_필드는_쿠폰으로_세지_않는다")
    void 예약_필드는_쿠폰으로_세지_않는다() {
        // '#' 로 시작하는 것은 전역값이다. 쿠폰으로 세면 판정이 없는 쿠폰을
        // 만들어 내고, 그 쿠폰은 매진으로 보인다.
        GatewaySnapshot s = SnapshotCodec.create().decode(해시(
                "#credit", "1000", "#nodes", "2", "#published", "1787184000",
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
                "모르는모드", "그런모드:IDLE:0:1:0:1.0",
                "불변식위반", "ADAPTIVE:IDLE:999:500:0:1.0",
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

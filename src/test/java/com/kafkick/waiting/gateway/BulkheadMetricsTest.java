package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.Bulkhead;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 격벽이 지금 얼마나 차 있는지.
 *
 * <p><b>막은 횟수만으로는 부족합니다.</b> 그 값은 이미 막힌 뒤에야 오릅니다.
 * 차오르는 중인지 비어 있는지를 보려면 지금 걸려 있는 수가 필요하고, 그것이
 * 있어야 상한을 올릴지 뒷단을 늘릴지 판단할 수 있습니다.
 */
class BulkheadMetricsTest {

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private final Bulkhead bulkhead = Bulkhead.withMaxKeys(10);

    private void 지표를_건다() {
        BulkheadMetrics.bind(bulkhead, meters);
    }

    /** 지금 걸려 있는 수. 이 값이 상한에 붙으면 곧 막히기 시작합니다. */
    @Test
    @DisplayName("걸려_있는_건수를_낸다")
    void 걸려_있는_건수를_낸다() {
        지표를_건다();

        bulkhead.tryEnter("c1", 5);
        bulkhead.tryEnter("c1", 5);
        bulkhead.tryEnter("c2", 5);

        assertThat(meters.get("waiting.bulkhead.in.flight").gauge().value()).isEqualTo(3);
    }

    /**
     * <b>몇 개 쿠폰이 자리를 쥐고 있는지도 봅니다.</b> 맵이 상한에 붙으면 새 쿠폰이
     * 아예 못 들어가는데, 걸려 있는 수만 보면 그 순간이 안 보입니다.
     */
    @Test
    @DisplayName("자리를_쥔_쿠폰_수를_낸다")
    void 자리를_쥔_쿠폰_수를_낸다() {
        지표를_건다();

        bulkhead.tryEnter("c1", 5);
        bulkhead.tryEnter("c2", 5);

        assertThat(meters.get("waiting.bulkhead.coupons").gauge().value()).isEqualTo(2);
    }

    /** 나가면 줄어듭니다. 안 줄면 지표가 실제와 갈려 아무 뜻이 없습니다. */
    @Test
    @DisplayName("나가면_줄어든다")
    void 나가면_줄어든다() {
        지표를_건다();
        bulkhead.tryEnter("c1", 5);

        bulkhead.exit("c1");

        assertThat(meters.get("waiting.bulkhead.in.flight").gauge().value()).isZero();
        assertThat(meters.get("waiting.bulkhead.coupons").gauge().value()).isZero();
    }

    /**
     * <b>대상이 수거되어도 값을 계속 냅니다.</b> 약한 참조로 등록하면 첫 GC 에
     * 영원히 {@code NaN} 을 내는데, 프로메테우스는 그 줄을 그대로 내보냅니다.
     */
    @Test
    @DisplayName("수거되어도_값을_계속_낸다")
    void 수거되어도_값을_계속_낸다() {
        지표를_건다();
        bulkhead.tryEnter("c1", 5);

        System.gc();

        assertThat(meters.get("waiting.bulkhead.in.flight").gauge().value()).isEqualTo(1);
    }
}

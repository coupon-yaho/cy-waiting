package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.Bulkhead;
import java.lang.ref.WeakReference;
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
        BulkheadMetrics.bind(bulkhead, 10, meters);
    }

    /** 지금 걸려 있는 수. 이 값이 상한에 붙으면 곧 막히기 시작합니다. */
    @Test
    @DisplayName("걸려_있는_건수를_낸다")
    void 걸려_있는_건수를_낸다() {
        지표를_건다();

        bulkhead.tryEnter("c1", 5);
        bulkhead.tryEnter("c1", 5);
        bulkhead.tryEnter("c2", 5);

        assertThat(meters.get("waiting.bulkhead.inflight").gauge().value()).isEqualTo(3);
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

        assertThat(meters.get("waiting.bulkhead.inflight").gauge().value()).isZero();
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

        수거를_강제한다();

        assertThat(meters.get("waiting.bulkhead.inflight").gauge().value()).isEqualTo(1);
    }

    /**
     * <b>쿠폰 식별자를 라벨에 안 넣습니다.</b> 밖에서 오는 값이라 가짓수에 상한이
     * 없고, 하나 붙는 순간 지표 하나가 메모리를 밀어냅니다 (LG-4).
     *
     * <p>클래스 주석이 이 약속을 내세우는데 그것을 지키는 시험이 없었습니다 —
     * 라벨 한 줄을 넣어 보니 시험 세 벌이 그대로 통과했습니다.
     */
    @Test
    @DisplayName("게이지에_라벨을_안_붙인다")
    void 게이지에_라벨을_안_붙인다() {
        Bulkhead bulkhead = Bulkhead.withMaxKeys(10);
        bulkhead.tryEnter("c1", 5);
        BulkheadMetrics.bind(bulkhead, 10, meters);

        assertThat(meters.getMeters())
                .filteredOn(m -> m.getId().getName().startsWith("waiting.bulkhead"))
                .hasSize(3)
                .allSatisfy(m -> assertThat(m.getId().getTags())
                        .as("%s 의 라벨", m.getId().getName())
                        .isEmpty());
    }

    /**
     * <b>분자만 내면 판단이 안 됩니다.</b> 800 이라는 값만 보고는 여유인지 임박인지
     * 모릅니다. 분모가 코드 상수로만 있으면 알람이 그 숫자를 베껴 적고, 상수를
     * 바꾸는 날 조용히 갈라집니다.
     */
    @Test
    @DisplayName("담을_수_있는_쿠폰_수도_낸다")
    void 담을_수_있는_쿠폰_수도_낸다() {
        BulkheadMetrics.bind(Bulkhead.withMaxKeys(7), 7, meters);

        assertThat(meters.get("waiting.bulkhead.max.coupons").gauge().value())
                .isEqualTo(7.0);
    }

    /**
     * <b>두 번 걸면 두 번째가 조용히 버려집니다.</b> 게이지는 첫 격벽을 계속
     * 읽습니다 — 프로덕션은 싱글턴이라 지금은 맞지만, 시험에서 같은 레지스트리로
     * 두 번째 필터를 만들면 엉뚱한 격벽을 읽으면서 초록이 뜹니다.
     */
    @Test
    @DisplayName("두_번_걸면_두_번째는_버려진다")
    void 두_번_걸면_두_번째는_버려진다() {
        Bulkhead 첫째 = Bulkhead.withMaxKeys(10);
        Bulkhead 둘째 = Bulkhead.withMaxKeys(10);
        둘째.tryEnter("c1", 5);
        둘째.tryEnter("c2", 5);

        BulkheadMetrics.bind(첫째, 10, meters);
        BulkheadMetrics.bind(둘째, 10, meters);

        assertThat(meters.get("waiting.bulkhead.inflight").gauges()).hasSize(1);
        assertThat(meters.get("waiting.bulkhead.inflight").gauge().value())
                .as("첫 격벽을 계속 읽는다")
                .isZero();
    }

    /**
     * <b>수거가 실제로 일어났는지 먼저 잽니다.</b> {@code System.gc()} 는 힌트라,
     * {@code -XX:+DisableExplicitGC} 가 붙는 순간 이 시험은 아무것도 안 재면서
     * 영구히 초록이 됩니다 — 장애를 못 주입하는 하네스와 같은 구조입니다.
     */
    private void 수거를_강제한다() {
        WeakReference<Object> 카나리아 = new WeakReference<>(new Object());
        for (int i = 0; i < 50 && 카나리아.get() != null; i++) {
            System.gc();
        }
        assertThat(카나리아.get())
                .as("GC 가 약한 참조를 안 걷었다 — 이 시험은 아무것도 안 재고 있다")
                .isNull();
    }
}

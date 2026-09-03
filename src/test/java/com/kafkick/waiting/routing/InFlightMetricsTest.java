package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 물려 있는 수가 지표로 나온다.
 *
 * <p><b>누수는 값이 안 내려가는 것으로만 보인다.</b> 부하가 끝났는데 0 이
 * 아니면 감소를 어디선가 놓친 것이다 (G9.3).
 */
@Tag("unit")
class InFlightMetricsTest {

    private static final long 지금 = 1_800_000_000_000L;

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private final InFlightRegistry 레지스트리 = InFlightRegistry.of(Duration.ofSeconds(30));

    private void 지표를_건다() {
        InFlightMetrics.bind(레지스트리, () -> 지금, meters);
    }

    @Test
    @DisplayName("물린_수를_낸다")
    void 물린_수를_낸다() {
        지표를_건다();

        레지스트리.started("be-1", 지금);
        레지스트리.started("be-1", 지금);
        레지스트리.started("be-2", 지금);

        assertThat(meters.get("waiting.routing.inflight").gauge().value()).isEqualTo(3);
    }

    /** 합만 보면 한 대에 몰린 것과 고루 퍼진 것이 구분이 안 된다. */
    @Test
    @DisplayName("가장_바쁜_대도_낸다")
    void 가장_바쁜_대도_낸다() {
        지표를_건다();

        레지스트리.started("be-1", 지금);
        레지스트리.started("be-1", 지금);
        레지스트리.started("be-2", 지금);

        assertThat(meters.get("waiting.routing.inflight.busiest").gauge().value())
                .isEqualTo(2);
    }

    /** 카운터를 들고 있는 대의 수. 안 줄면 사라진 대가 남은 것이다. */
    @Test
    @DisplayName("인스턴스_수를_낸다")
    void 인스턴스_수를_낸다() {
        지표를_건다();
        레지스트리.started("be-1", 지금);
        레지스트리.started("be-2", 지금);

        assertThat(meters.get("waiting.routing.instances").gauge().value()).isEqualTo(2);
    }

    /** 끝나면 내려간다. 안 내려가면 지표가 실제와 갈려 아무 뜻이 없다. */
    @Test
    @DisplayName("끝나면_내려간다")
    void 끝나면_내려간다() {
        지표를_건다();
        InFlightRegistry.Ticket 표 = 레지스트리.started("be-1", 지금);

        표.finished();

        assertThat(meters.get("waiting.routing.inflight").gauge().value()).isZero();
        assertThat(meters.get("waiting.routing.inflight.busiest").gauge().value()).isZero();
    }

    /**
     * <b>인스턴스 식별자를 라벨에 안 붙인다.</b> 재기동마다 새로 오므로 시계열이
     * 무한히 늘고, 하나 붙는 순간 지표가 메모리를 밀어낸다 (LG-4).
     */
    @Test
    @DisplayName("게이지에_라벨을_안_붙인다")
    void 게이지에_라벨을_안_붙인다() {
        지표를_건다();
        레지스트리.started("be-1", 지금);

        assertThat(meters.getMeters())
                .filteredOn(m -> m.getId().getName().startsWith("waiting.routing"))
                .hasSize(3)
                .allSatisfy(m -> assertThat(m.getId().getTags())
                        .as("%s 의 라벨", m.getId().getName())
                        .isEmpty());
    }
}

package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Set;
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

    private final InstanceOutliers 배제기 =
            InstanceOutliers.of(3, Duration.ofSeconds(10), Duration.ofSeconds(60));

    private void 지표를_건다() {
        InFlightMetrics.bind(레지스트리, 배제기, () -> 지금, meters);
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
                // 수를 못 박는 것은 새 지표를 걸 때 이 규칙을 다시 읽게 하려는 것이다.
                .hasSize(10)
                .allSatisfy(m -> assertThat(m.getId().getTags())
                        .as("%s 의 라벨", m.getId().getName())
                        .isEmpty());
    }

    /**
     * <b>되돌리는 구간이 이름으로 잡혀야 한다.</b> 배제 게이지는 배제 창만 세므로,
     * 이 넷이 없으면 그 대가 회복을 마쳤는지를 운영에서 물을 수단이 없다.
     */
    @Test
    @DisplayName("되돌리는_구간의_지표가_선다")
    void 되돌리는_구간의_지표가_선다() {
        지표를_건다();

        assertThat(meters.getMeters())
                .extracting(m -> m.getId().getName())
                .contains("waiting.routing.ramping", "waiting.routing.ramp.suppressed",
                        "waiting.routing.ejections.first", "waiting.routing.ejections.reentry",
                        "waiting.routing.ramp.completed");
    }

    /**
     * <b>표시된 수와 실제로 걸러진 수는 다를 수 있다.</b> 전부가 대상이면 하나도
     * 안 빼므로, 이 값이 전체 대수와 같아지는 것이 뒷단 전체가 앓는다는 신호다.
     */
    @Test
    @DisplayName("배제된_대의_수를_낸다")
    void 배제된_대의_수를_낸다() {
        지표를_건다();

        assertThat(meters.get("waiting.routing.ejected").gauge().value()).isZero();

        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 지금);
        }
        배제기.retain(Set.of("be-1", "be-2"), 지금);

        assertThat(meters.get("waiting.routing.ejected").gauge().value()).isEqualTo(1);
        assertThat(meters.get("waiting.routing.seen").gauge().value())
                .as("견줄 대상").isEqualTo(2);
    }
}

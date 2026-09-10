package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
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

    /**
     * 이 클래스가 거는 지표 전부. <b>목록으로 두면 새 지표를 걸 때 여기를 고치게
     * 되고</b>, 그때 라벨 규칙과 이름 규약을 한 번 더 읽는다.
     */
    private static final List<String> 라우팅_지표 = List.of(
            "waiting.routing.inflight",
            "waiting.routing.inflight.busiest",
            "waiting.routing.instances",
            "waiting.routing.ejected",
            "waiting.routing.seen",
            "waiting.routing.ramping",
            "waiting.routing.ramp.suppressed",
            "waiting.routing.ramp.completed",
            "waiting.routing.ejections.started",
            "waiting.routing.ejections.repeated",
            "waiting.routing.ejections.overridden");

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
                .hasSize(라우팅_지표.size())
                .allSatisfy(m -> assertThat(m.getId().getTags())
                        .as("%s 의 라벨", m.getId().getName())
                        .isEmpty());
    }

    /**
     * <b>되돌리는 구간이 값으로 나와야 한다.</b> 배제 게이지는 배제 창만 세므로,
     * 이 다섯이 없으면 그 대가 회복을 마쳤는지를 운영에서 물을 수단이 없다.
     */
    @Test
    @DisplayName("되돌리는_구간의_지표가_값을_낸다")
    void 되돌리는_구간의_지표가_값을_낸다() {
        지표를_건다();

        assertThat(meters.get("waiting.routing.ramping").gauge().value())
                .as("앓은 대가 없으면 0 이다").isZero();
        // 셋을 빼되 끝을 다르게 낸다 — 값이 겹치면 배선을 바꿔 걸어도 안 빨개진다.
        셋을_빼고_다르게_끝낸다();

        assertThat(meters.getMeters())
                .as("건 지표 전부")
                .extracting(m -> m.getId().getName())
                .containsExactlyInAnyOrderElementsOf(라우팅_지표);
        assertThat(meters.get("waiting.routing.ramping").gauge().value())
                .as("되돌리는 중은 하나뿐이다").isEqualTo(1);
        assertThat(meters.get("waiting.routing.ramp.suppressed").gauge().value())
                .as("램프 사분의 일이면 사분의 삼이 남는다").isCloseTo(0.75, within(0.01));
        assertThat(meters.get("waiting.routing.ejections.started").functionCounter().count())
                .isEqualTo(3);
        assertThat(meters.get("waiting.routing.ejections.repeated").functionCounter().count())
                .isEqualTo(1);
        assertThat(meters.get("waiting.routing.ramp.completed").functionCounter().count())
                .isEqualTo(2);
        assertThat(meters.get("waiting.routing.ejections.overridden").functionCounter().count())
                .as("도로 넣은 적이 없다").isZero();
    }

    /**
     * be-1 은 되돌리다 다시 빠져 지금 램프 사분의 일 지점이고, be-2·be-3 은 램프를
     * 마쳤다. 계수 셋이 3 · 1 · 2 로 갈려 서로 바꿔 걸면 드러난다.
     */
    private void 셋을_빼고_다르게_끝낸다() {
        long 먼저 = 지금 - Duration.ofSeconds(40).toMillis();
        long 다시 = 지금 - Duration.ofSeconds(25).toMillis();
        long 마친_대들 = 지금 - Duration.ofSeconds(71).toMillis();
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 먼저);
        }
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 다시);
        }
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-2", 마친_대들);
            배제기.failed("be-3", 마친_대들);
        }
        배제기.retain(Set.of("be-1", "be-2", "be-3"), 지금);
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

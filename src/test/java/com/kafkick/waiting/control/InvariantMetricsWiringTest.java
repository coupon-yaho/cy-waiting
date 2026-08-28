package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 불변식 선행 지표가 <b>실제로 걸렸는가</b> (6.9.1).
 */
// 등록하는 코드가 있는 것과 실제로 걸린 것은 다르다. 대시보드 검사는 코드에
// 이름 문자열이 있는지만 봐서 배선이 빠져도 통과한다 — 이 브랜치가 실제로
// 그 상태였고, 사람 리뷰가 잡았다.
@Tag("context")
@SpringBootTest(properties = "waiting.scheduler.enabled=true")
class InvariantMetricsWiringTest {

    @Autowired
    private PrometheusMeterRegistry registry;

    @Test
    @DisplayName("선행_지표_넷이_스크레이프에_나온다")
    void 선행_지표_넷이_스크레이프에_나온다() {
        assertThat(registry.scrape())
                .contains("waiting_allocation_budget_overshoot_total")
                .contains("waiting_allocation_entered_overshoot_total")
                .contains("waiting_poll_budget_overshoot_ticks_total")
                .contains("waiting_snapshot_clock_floor_applied_total");
    }

    /**
     * <b>라벨을 안 붙인다.</b> 쿠폰 식별자는 가짓수에 상한이 없다 (LG-4).
     *
     * <p>프리픽스를 하나만 훑으면 다른 이름으로 들어온 지표가 검사를 통째로
     * 비껴간다 — 실제로 폴링 예산 지표가 그 상태로 들어왔다.
     */
    @Test
    @DisplayName("선행_지표에_라벨을_안_붙인다")
    void 선행_지표에_라벨을_안_붙인다() {
        assertThat(registry.getMeters())
                .filteredOn(m -> m.getId().getName().startsWith("waiting.allocation.")
                        || m.getId().getName().startsWith("waiting.poll.")
                        || m.getId().getName().startsWith("waiting.snapshot.clock."))
                .isNotEmpty()
                .allSatisfy(m -> assertThat(m.getId().getTags())
                        .as("%s 의 라벨", m.getId().getName())
                        .allMatch(tag -> tag.getKey().equals("application")));
    }
}

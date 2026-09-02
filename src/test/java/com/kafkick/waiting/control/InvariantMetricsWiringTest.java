package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.List;
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

    // **약한 참조는 여기서 못 잡는다.** 함수형 계측기는 상태 객체를 약한 참조로
    // 잡으므로, 부르는 자리에서 만든 람다를 넘기면 GC 뒤에 그 계수가 0 으로
    // 굳는다. 그런데 굳기 전에는 등록도 스크레이프도 정상이라 아래 시험들이
    // 통과하고, GC 를 불러 재면 수거 시점이 안 보장돼 간헐로 통과한다.
    // 막는 것은 시험이 아니라 InvariantMetrics 가 형제와 같은 상태 객체를
    // 쓰는 구조다 — 그 근거는 그 필드의 주석에 있다.

    /** 이름을 한 곳에 둔다 — 두 시험이 각자 나열하면 하나만 늘어난다. */
    private static final List<String> 선행_지표 = List.of(
            "waiting.allocation.budget.overshoot",
            "waiting.allocation.entered.overshoot",
            "waiting.poll.budget.overshoot.ticks",
            "waiting.snapshot.clock.floor.applied",
            "waiting.allocation.admitted",
            "waiting.allocation.stock.unknown.ticks",
            "waiting.snapshot.stock.unknown.dropped");

    @Test
    @DisplayName("선행_지표가_스크레이프에_나온다")
    void 선행_지표가_스크레이프에_나온다() {
        assertThat(registry.scrape())
                .contains("waiting_allocation_budget_overshoot_total")
                .contains("waiting_allocation_entered_overshoot_total")
                .contains("waiting_poll_budget_overshoot_ticks_total")
                .contains("waiting_snapshot_clock_floor_applied_total")
                // 크레딧 낭비의 분모다 (G7.5). 밖에서 못 읽으면 못 잰다.
                .contains("waiting_allocation_admitted_total")
                // 재고 키를 잃은 사실과, 그것이 거짓 매진으로 나간 사실 (CY-702).
                .contains("waiting_allocation_stock_unknown_ticks_total")
                .contains("waiting_snapshot_stock_unknown_dropped_total")
                // 서킷을 보고 있는가 (F3 · CY-788). 안 보는 것과 닫혀 있는 것이
                // 같은 값을 내므로, 이 게이지가 없으면 배선이 빠진 것을 못 안다.
                .contains("waiting_circuit_wired");
    }

    /**
     * <b>라벨을 안 붙인다.</b> 쿠폰 식별자는 가짓수에 상한이 없다 (LG-4).
     */
    // 프리픽스를 나열하면 네 번째 이름 공간으로 들어온 지표가 또 비껴간다 —
    // 실제로 폴링 예산 지표가 그 상태로 들어왔고, 프리픽스를 하나 더 적는
    // 방식으로 고치면 다음 번에 같은 일이 난다. `waiting.` 하나로 넓힌다.
    @Test
    @DisplayName("선행_지표에_라벨을_안_붙인다")
    void 선행_지표에_라벨을_안_붙인다() {
        assertThat(registry.getMeters())
                .filteredOn(m -> 선행_지표.contains(m.getId().getName()))
                .as("이름이 바뀌면 여기가 먼저 빈다")
                .hasSize(선행_지표.size())
                .allSatisfy(m -> assertThat(m.getId().getTags())
                        .as("%s 의 라벨", m.getId().getName())
                        .allMatch(tag -> tag.getKey().equals("application")));
    }
}

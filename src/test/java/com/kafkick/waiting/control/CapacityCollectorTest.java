package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 뒷단이 스스로 보고한 여유를 모아 전역 크레딧을 만든다.
 *
 * <p><b>콜드 인스턴스는 자기 여유를 과대 보고한다.</b> 재기동 직후엔 커넥션 풀이
 * 비어 있어 "유휴 200" 이 나오는데, 실제로는 JIT 콜드와 캐시 미스로 느려서 즉시
 * 포화된다. 그대로 믿고 트래픽을 보내면 뒷단이 무너진다.
 */
class CapacityCollectorTest {

    private static final Duration RAMP_UP = Duration.ofSeconds(60);
    private static final long FLOOR = 10;
    private static final long NOW = 1_800_000_000L;

    private CapacityCollector collector() {
        return CapacityCollector.of(RAMP_UP, FLOOR);
    }

    private static CapacityReport report(String id, long credits, long reportedAt) {
        return new CapacityReport(id, credits, reportedAt);
    }

    @Test
    @DisplayName("신선한_보고를_더해_전역_크레딧을_만든다")
    void 신선한_보고를_더해_전역_크레딧을_만든다() {
        CapacityCollector collector = collector();
        collector.firstSeen("a", NOW - 60);
        collector.firstSeen("b", NOW - 60);

        long credit = collector.collect(
                List.of(report("a", 100, NOW), report("b", 200, NOW)), NOW);

        assertThat(credit).isEqualTo(300);
    }

    @Test
    @DisplayName("처음_본_인스턴스는_램프업_비율만_받는다")
    void 처음_본_인스턴스는_램프업_비율만_받는다() {
        // **자기 상태를 가장 잘 아는 쪽이 먼저 램프를 건다.** 그래도 게이트웨이가
        // 한 겹 더 건다 — 상대 구현이 늦어도 보호가 남아야 한다.
        CapacityCollector collector = collector();
        collector.firstSeen("cold", NOW);

        long credit = collector.collect(List.of(report("cold", 200, NOW)), NOW + 15);

        // 60초 중 15초 지났으므로 4분의 1이다.
        assertThat(credit).isEqualTo(50);
    }

    @Test
    @DisplayName("램프업이_끝나면_보고를_그대로_쓴다")
    void 램프업이_끝나면_보고를_그대로_쓴다() {
        CapacityCollector collector = collector();
        collector.firstSeen("warm", NOW);

        long credit = collector.collect(List.of(report("warm", 200, NOW)), NOW + 60);

        assertThat(credit).isEqualTo(200);
    }

    @Test
    @DisplayName("양쪽이_램프를_걸면_곡선이_가팔라진다")
    void 양쪽이_램프를_걸면_곡선이_가팔라진다() {
        // **게이트웨이는 보고값 하나만 보고 전체 용량을 모른다.** 그래서 "둘 중
        // 작은 쪽" 을 고를 두 번째 값이 없다 — 계획서 문구를 이번에 정정했다.
        //
        // 인스턴스가 이미 램프를 걸어 100 을 보고했으면 여기서 또 비율이 걸려
        // 같은 60초 창 안에서 곡선이 가팔라진다. 방향이 과소라 안전하고 창이
        // 끝나면 스스로 풀린다.
        CapacityCollector collector = collector();
        collector.firstSeen("both", NOW);

        long credit = collector.collect(List.of(report("both", 100, NOW)), NOW + 30);

        assertThat(credit).isEqualTo(50);
    }

    @Test
    @DisplayName("낡은_보고는_안_센다")
    void 낡은_보고는_안_센다() {
        // **TTL 만 믿으면 안 된다.** TTL 은 지우는 시점이지 신선한 시점이 아니다.
        // 죽은 인스턴스의 마지막 보고가 TTL 이 남아 있는 동안 계속 세어진다.
        CapacityCollector collector = collector();
        collector.firstSeen("stale", NOW - 600);
        collector.firstSeen("fresh", NOW - 600);

        long credit = collector.collect(
                List.of(report("stale", 500, NOW - 61), report("fresh", 100, NOW)), NOW);

        assertThat(credit).isEqualTo(100);
    }

    @Test
    @DisplayName("신선한_보고가_없으면_하한을_쓴다")
    void 신선한_보고가_없으면_하한을_쓴다() {
        // 0 을 내면 전 쿠폰이 전면 차단된다. 뒷단이 안 보고한다고 게이트웨이가
        // 서비스를 멈출 이유는 없다.
        CapacityCollector collector = collector();

        assertThat(collector.collect(List.of(), NOW)).isEqualTo(FLOOR);
    }

    @Test
    @DisplayName("보고가_음수면_그_항목만_버린다")
    void 보고가_음수면_그_항목만_버린다() {
        // 한 인스턴스의 버그가 전역 크레딧을 통째로 망치면 안 된다.
        CapacityCollector collector = collector();
        collector.firstSeen("broken", NOW - 600);
        collector.firstSeen("ok", NOW - 600);

        long credit = collector.collect(
                List.of(report("broken", -5, NOW), report("ok", 70, NOW)), NOW);

        assertThat(credit).isEqualTo(70);
    }

    @Test
    @DisplayName("처음_본_시각을_모르면_콜드로_본다")
    void 처음_본_시각을_모르면_콜드로_본다() {
        // 게이트웨이가 재기동하면 관측 기록이 없다. 그때 보고를 그대로 믿으면
        // 콜드 인스턴스가 과대 보고한 값이 곧바로 전역 크레딧이 된다.
        CapacityCollector collector = collector();

        long credit = collector.collect(List.of(report("unknown", 200, NOW)), NOW);

        assertThat(credit).isEqualTo(FLOOR);
    }

    @Test
    @DisplayName("설정이_0_이하면_기동에_실패한다")
    void 설정이_0_이하면_기동에_실패한다() {
        assertThatThrownBy(() -> CapacityCollector.of(Duration.ZERO, FLOOR))
                .hasMessageContaining("rampUp");
        assertThatThrownBy(() -> CapacityCollector.of(RAMP_UP, 0))
                .hasMessageContaining("floor");
    }
}

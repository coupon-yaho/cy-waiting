package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.ServerClock;
import com.kafkick.waiting.control.CapacityCollector;
import com.kafkick.waiting.control.CapacityReport;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 복제본이 <b>뒤진 시계로 승격</b>하고, 시계가 앞선 뒷단이 죽는다.
 *
 * <p>둘 다 신선도 판정이 기준 시각에 기대기 때문에 생긴다. 앞엣것은 크레딧을
 * 하한에 박고, 뒤엣것은 없는 인스턴스 몫을 회복 첫 구간에 싣는다.
 */
@Tag("chaos")
class ClockBackAndGhostTest {

    private static final long NOW = 1_800_000_000L;
    private static final Duration 램프 = Duration.ofSeconds(60);
    private static final Duration 신선도 = Duration.ofSeconds(3);
    private static final long 하한 = 10;

    private CapacityCollector collector() {
        return CapacityCollector.of(램프, 신선도, 하한, 100_000);
    }

    /**
     * 진입 — 승격 직후 서버 시각이 30 초 뒤로 간다. <b>단조 가드가 걸린다.</b>
     *
     * <p>안 걸면 뒷단 보고가 전부 미래가 되어 한꺼번에 낡음이 된다.
     */
    @Test
    @DisplayName("진입_시계가_뒤로_가면_바닥값이_걸린다")
    void 진입_시계가_뒤로_가면_바닥값이_걸린다() {
        ServerClock clock = ServerClock.create();
        clock.observe(NOW);

        long 승격_직후 = clock.observe(NOW - 30);

        assertThat(승격_직후).isEqualTo(NOW);
        assertThat(clock.skew().appliedCount()).isPositive();
        assertThat(clock.skew().maxSkewMicros()).isEqualTo(30_000_000L);
    }

    /**
     * 유지 — 보정한 시각으로 걷으면 크레딧이 유지된다. 보정 없이 뒤진 시각을
     * 그대로 쓰면 신선한 보고가 0 건이 되어 하한에 박힌다.
     */
    @Test
    @DisplayName("유지_보정하면_크레딧이_안_떨어진다")
    void 유지_보정하면_크레딧이_안_떨어진다() {
        ServerClock clock = ServerClock.create();
        clock.observe(NOW);
        CapacityCollector collector = collector();
        collector.collect(List.of(new CapacityReport("i1", 10_000, NOW)), NOW, 1);
        long 정상 = collector.lastKnown();

        // 승격한 복제본이 30 초 뒤진 시각을 준다. 뒷단은 계속 정상 시각을 쓴다.
        long 보정된 = clock.observe(NOW - 30);
        long 승격_뒤 = collector.collect(
                List.of(new CapacityReport("i1", 10_000, NOW + 1)), 보정된, 1);

        assertThat(승격_뒤).isEqualTo(정상);
        // 보정을 안 하면 이렇게 된다 — 비교 대상을 값으로 남긴다.
        assertThat(collector().collect(
                List.of(new CapacityReport("i2", 10_000, NOW + 1)), NOW - 30, 1))
                .isEqualTo(하한);
    }

    /**
     * 회복 — 시계가 따라잡으면 바닥값을 놓는다. 안 놓으면 영영 옛 시각에 머문다.
     */
    @Test
    @DisplayName("회복_따라잡으면_바닥값을_놓는다")
    void 회복_따라잡으면_바닥값을_놓는다() {
        ServerClock clock = ServerClock.create();
        clock.observe(NOW);
        clock.observe(NOW - 30);

        assertThat(clock.observe(NOW + 1)).isEqualTo(NOW + 1);
    }

    /**
     * <b>유령은 신선도 창 + 허용치까지만 산다.</b> 시계가 앞선 뒷단이 죽으면
     * 마지막 보고가 그만큼 더 신선해 보인다 — 그 몫이 회복 첫 구간에, 뒷단이
     * 가장 차가울 때 실린다 (F6·RC4).
     */
    @Test
    @DisplayName("유령_인스턴스는_창_안에서만_세어진다")
    void 유령_인스턴스는_창_안에서만_세어진다() {
        CapacityCollector collector = collector();
        // 시계가 1 초 앞선 뒷단. 마지막 보고를 남기고 죽는다.
        long 마지막_보고 = NOW + 1;
        collector.collect(List.of(new CapacityReport("유령", 10_000, 마지막_보고)), NOW, 1);

        // 창 안에서는 아직 세어진다.
        long 창_안 = collector.collect(
                List.of(new CapacityReport("유령", 10_000, 마지막_보고)),
                마지막_보고 + 신선도.toSeconds(), 1);
        // 창을 넘기면 사라진다.
        long 창_밖 = collector.collect(
                List.of(new CapacityReport("유령", 10_000, 마지막_보고)),
                마지막_보고 + 신선도.toSeconds() + 1, 1);

        assertThat(창_안).isEqualTo(10_000);
        assertThat(창_밖).isEqualTo(하한);
    }
}

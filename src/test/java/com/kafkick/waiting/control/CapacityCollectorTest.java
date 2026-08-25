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
    /** R-2 가 정한 값이다 — 보고 주기 1초, 3회 연속 누락이면 없는 것으로 본다. */
    private static final Duration FRESHNESS = Duration.ofSeconds(3);
    private static final long FLOOR = 10;
    private static final long CAP = 100_000;
    private static final long NOW = 1_800_000_000L;

    private CapacityCollector collector() {
        return CapacityCollector.of(RAMP_UP, FRESHNESS, FLOOR, CAP);
    }

    private static CapacityReport report(String id, long credits, long reportedAt) {
        return new CapacityReport(id, credits, reportedAt);
    }

    /**
     * 램프를 끝내 둔다 — 램프가 아니라 다른 것을 재는 시험들이 쓴다.
     *
     * <p><b>보고를 계속 심는다.</b> 램프가 걸리려면 그 창 동안 관측이 이어져야
     * 한다 — 운영에서 보고는 1초 주기다.
     */
    private static long warm(CapacityCollector collector, String id, long credits) {
        for (long t = NOW; t <= NOW + RAMP_UP.toSeconds(); t += FRESHNESS.toSeconds()) {
            collector.collect(List.of(report(id, credits, t)), t, 1);
        }
        return NOW + RAMP_UP.toSeconds();
    }

    @Test
    @DisplayName("신선한_보고를_더해_전역_크레딧을_만든다")
    void 신선한_보고를_더해_전역_크레딧을_만든다() {
        CapacityCollector collector = collector();
        collector.collect(List.of(report("a", 100, NOW), report("b", 200, NOW)), NOW, 1);
        long warmed = NOW + RAMP_UP.toSeconds();

        long credit = collector.collect(
                List.of(report("a", 100, warmed), report("b", 200, warmed)), warmed, 1);

        assertThat(credit).isEqualTo(300);
    }

    @Test
    @DisplayName("처음_본_인스턴스는_램프업_비율만_받는다")
    void 처음_본_인스턴스는_램프업_비율만_받는다() {
        // **자기 상태를 가장 잘 아는 쪽이 먼저 램프를 건다.** 그래도 게이트웨이가
        // 한 겹 더 건다 — 상대 구현이 늦어도 보호가 남아야 한다.
        CapacityCollector collector = collector();
        // **첫 판은 이미 돌던 무리다.** 승계 직후를 콜드로 보면 크레딧이 0 이 된다.
        // 진짜 새 인스턴스는 그 뒤에 나타난 쪽이다.
        collector.collect(List.of(report("warm", 100, NOW)), NOW, 1);
        collector.collect(List.of(report("warm", 100, NOW), report("cold", 200, NOW)), NOW, 1);

        long credit = collector.collect(
                List.of(report("warm", 100, NOW + 15), report("cold", 200, NOW + 15)),
                NOW + 15, 1);

        // 이미 돌던 100 은 온전히, 콜드 200 은 60초 중 15초라 4분의 1이다.
        assertThat(credit).isEqualTo(150);
    }

    @Test
    @DisplayName("등록을_따로_안_불러도_램프가_걸린다")
    void 등록을_따로_안_불러도_램프가_걸린다() {
        // **두 번 불러야 하는 계약을 두면 빠뜨렸을 때 조용히 0 을 낸다.** 보고가
        // 관측됐다는 것이 곧 처음 본 시각이 있다는 뜻이라, 그 상태는 운영에
        // 존재할 수 없는데 시험만 만들 수 있게 된다.
        CapacityCollector collector = collector();
        // 첫 판은 이미 돌던 무리로 본다. 진짜 새것은 그 뒤에 나타난 쪽이다.
        collector.collect(List.of(report("warm", 0, NOW)), NOW, 1);

        long first = collector.collect(
                List.of(report("warm", 0, NOW), report("new", 600, NOW)), NOW, 1);

        // 방금 처음 봤으므로 경과 0 이라 그 인스턴스 몫은 0 이다. 다만 합이 0 이
        // 된 이유가 램프라면 하한을 쓴다 — 우리가 만든 0 이지 뒷단이 말한 0 이 아니다.
        assertThat(first).isEqualTo(FLOOR);
    }

    @Test
    @DisplayName("램프업이_끝나면_보고를_그대로_쓴다")
    void 램프업이_끝나면_보고를_그대로_쓴다() {
        CapacityCollector collector = collector();
        long warmed = warm(collector, "warm", 200);

        assertThat(collector.collect(List.of(report("warm", 200, warmed)), warmed, 1))
                .isEqualTo(200);
    }

    @Test
    @DisplayName("사라진_인스턴스는_기록에서_지운다")
    void 사라진_인스턴스는_기록에서_지운다() {
        // **이름이 고정된 파드가 재기동하면 옛 기록이 남아 램프가 안 걸린다.**
        // 콜드 복귀가 램프를 거는 유일한 이유인데 거기서만 안 걸리는 것이다.
        // 기록이 자라는 것도 같은 뿌리다.
        CapacityCollector collector = collector();
        long warmed = warm(collector, "pod-0", 200);
        assertThat(collector.collect(List.of(report("pod-0", 200, warmed)), warmed, 1))
                .isEqualTo(200);

        // **램프 창을 넘겨 안 보인다.** 몇 초 빠진 것으로는 안 지운다 — 그건 관측
        // 실패지 사라진 것이 아니고, 지우면 정상 인스턴스가 램프를 다시 탄다.
        long gone = warmed + RAMP_UP.toSeconds() + 1;
        collector.collect(List.of(), gone, 1);

        // 기록이 지워진 뒤라 처음 보는 것과 같다. 램프가 다시 걸린다 — 다만 합이
        // 0 이 된 이유가 램프라서 발행값은 하한이다.
        long back = gone + 1;
        assertThat(collector.collect(List.of(report("pod-0", 200, back)), back, 1))
                .isEqualTo(FLOOR);
    }

    @Test
    @DisplayName("낡은_보고는_안_센다")
    void 낡은_보고는_안_센다() {
        // **TTL 만 믿으면 안 된다.** TTL 은 지우는 시점이지 신선한 시점이 아니다.
        // 죽은 인스턴스의 마지막 보고가 TTL 이 남아 있는 동안 계속 세어진다.
        CapacityCollector collector = collector();
        long warmed = warm(collector, "fresh", 100);
        collector.collect(List.of(report("stale", 500, NOW)), NOW, 1);

        long credit = collector.collect(
                List.of(report("stale", 500, warmed - FRESHNESS.toSeconds() - 1),
                        report("fresh", 100, warmed)), warmed, 1);

        assertThat(credit).isEqualTo(100);
    }

    @Test
    @DisplayName("한_틱_빠져도_워밍업이_안_날아간다")
    void 한_틱_빠져도_워밍업이_안_날아간다() {
        // 이번 판에 없다고 지우면 정상 인스턴스가 틱마다 램프를 다시 탄다.
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 200);

        collector.collect(List.of(), warmed, 1);

        assertThat(collector.collect(List.of(report("a", 200, warmed + 1)), warmed + 1, 1))
                .isEqualTo(200);
    }

    @Test
    @DisplayName("신선도는_램프와_다른_노브다")
    void 신선도는_램프와_다른_노브다() {
        // 하나로 묶으면 한 값이 반대 방향 두 사고를 함께 조종한다 — 크게 잡으면
        // 죽은 인스턴스가 오래 세어지고, 작게 잡으면 틱 한 번 밀려도 전면 억제다.
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 100);

        // 램프(60초) 안이지만 신선도(3초) 밖이다.
        assertThat(collector.collect(List.of(report("a", 100, warmed - 30)), warmed, 1))
                .isEqualTo(FLOOR);
    }

    @Test
    @DisplayName("여유_0_보고는_그대로_존중한다")
    void 여유_0_보고는_그대로_존중한다() {
        // **합이 0 인 것과 아무도 안 보고한 것은 다르다.** 뒷단이 신선하게
        // "여유 0" 을 보고했으면 그건 정확한 백프레셔다 — 거기에 하한을 얹으면
        // 명시적 신호를 무시하고 초당 하한만큼 계속 밀어넣는다.
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 100);

        assertThat(collector.collect(List.of(report("a", 0, warmed)), warmed, 1)).isZero();
    }

    @Test
    @DisplayName("신선한_보고가_없으면_하한을_쓴다")
    void 신선한_보고가_없으면_하한을_쓴다() {
        // 0 을 내면 전 쿠폰이 전면 차단된다.
        assertThat(collector().collect(List.of(), NOW, 1)).isEqualTo(FLOOR);
    }

    @Test
    @DisplayName("읽기_실패는_직전_값을_지킨다")
    void 읽기_실패는_직전_값을_지킨다() {
        // 보고가 0건인 것과 못 읽은 것은 다르다. 레디스가 안 되면 모든 노드가
        // 같이 실패하는데 여기서 하한으로 떨어뜨리면 전면 억제가 된다.
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 300);
        collector.collect(List.of(report("a", 300, warmed)), warmed, 1);

        collector.observationFailed();

        assertThat(collector.lastKnown()).isEqualTo(300);
    }

    @Test
    @DisplayName("보고가_음수면_그_항목만_버린다")
    void 보고가_음수면_그_항목만_버린다() {
        CapacityCollector collector = collector();
        collector.collect(List.of(report("broken", -5, NOW), report("ok", 70, NOW)), NOW, 1);
        long warmed = NOW + RAMP_UP.toSeconds();

        long credit = collector.collect(
                List.of(report("broken", -5, warmed), report("ok", 70, warmed)), warmed, 1);

        assertThat(credit).isEqualTo(70);
    }

    @Test
    @DisplayName("상한을_넘는_보고는_잘린다")
    void 상한을_넘는_보고는_잘린다() {
        // 단위 착오나 설정 실수가 무제한 통과가 된다. 음수만 막는 것은 절반이다.
        CapacityCollector collector = collector();
        long warmed = warm(collector, "huge", Long.MAX_VALUE);

        assertThat(collector.collect(List.of(report("huge", Long.MAX_VALUE, warmed)), warmed, 1))
                .isEqualTo(CAP);
    }

    @Test
    @DisplayName("큰_보고에도_램프가_넘치지_않는다")
    void 큰_보고에도_램프가_넘치지_않는다() {
        // 곱하고 나누면 넘쳐서 음수가 되고, 그러면 다른 인스턴스 몫을 상쇄해
        // 전역 크레딧이 하한으로 떨어진다.
        CapacityCollector collector = CapacityCollector.of(
                RAMP_UP, FRESHNESS, FLOOR, Long.MAX_VALUE);
        collector.collect(List.of(report("seed", 0, NOW)), NOW, 1);
        collector.collect(List.of(report("huge", Long.MAX_VALUE, NOW)), NOW, 1);

        long credit = collector.collect(
                List.of(report("huge", Long.MAX_VALUE, NOW + 30)), NOW + 30, 1);

        // **양수인지만 보면 아무 값이나 통과한다.** 30초는 창의 절반이므로
        // 정확히 절반이어야 한다 — 넘침을 막느라 값이 틀어지면 그것도 결함이다.
        assertThat(credit).isEqualTo(Long.MAX_VALUE / 2);
    }

    @Test
    @DisplayName("음수만_보고되면_하한을_쓴다")
    void 음수만_보고되면_하한을_쓴다() {
        // **음수를 관측으로 세면 안 된다.** 세면 "신선한 보고가 있다" 가 되어
        // 하한이 안 걸리고 전역 크레딧이 0 이 된다 — 전면 차단이다.
        CapacityCollector collector = collector();

        assertThat(collector.collect(List.of(report("broken", -5, NOW)), NOW, 1))
                .isEqualTo(FLOOR);
    }

    @Test
    @DisplayName("합이_넘쳐도_음수가_안_된다")
    void 합이_넘쳐도_음수가_안_된다() {
        // 인스턴스가 많고 각자 상한에 가까우면 합이 넘친다. 넘치면 음수가 되어
        // 전역 크레딧이 0 이 된다.
        CapacityCollector collector = CapacityCollector.of(
                RAMP_UP, FRESHNESS, FLOOR, Long.MAX_VALUE);
        long warmed = NOW + RAMP_UP.toSeconds();
        for (long t = NOW; t <= warmed; t += FRESHNESS.toSeconds()) {
            collector.collect(
                    List.of(report("a", Long.MAX_VALUE, t), report("b", Long.MAX_VALUE, t)), t, 1);
        }

        assertThat(collector.collect(
                List.of(report("a", Long.MAX_VALUE, warmed), report("b", Long.MAX_VALUE, warmed)),
                warmed, 1)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    @DisplayName("초_미만_설정은_기동에_실패한다")
    void 초_미만_설정은_기동에_실패한다() {
        // 초 단위로 재는데 500ms 를 주면 조용히 0 이 되어 나눗셈이 터지거나
        // 임계가 사라진다.
        assertThatThrownBy(() -> CapacityCollector.of(Duration.ofMillis(500), FRESHNESS, FLOOR, CAP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("초 단위");
        assertThatThrownBy(() -> CapacityCollector.of(RAMP_UP, Duration.ofMillis(1500), FLOOR, CAP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("초 단위");
    }

    @Test
    @DisplayName("창이_너무_길면_기동에_실패한다")
    void 창이_너무_길면_기동에_실패한다() {
        // **램프 나머지항이 창의 제곱으로 커진다.** 창을 하루로 묶으면 어떤
        // 보고값이 와도 넘칠 수 없다 — 곱셈을 감싸는 것보다 근본적이다.
        assertThatThrownBy(() -> CapacityCollector.of(
                Duration.ofDays(2), FRESHNESS, FLOOR, CAP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("이하여야 한다");
    }

    @Test
    @DisplayName("창이_상한이어도_넘치지_않는다")
    void 창이_상한이어도_넘치지_않는다() {
        // 상한과 최대 보고값을 함께 줘도 계산이 성립해야 한다.
        Duration window = Duration.ofDays(1);
        CapacityCollector collector = CapacityCollector.of(
                window, FRESHNESS, FLOOR, Long.MAX_VALUE);
        long half = window.toSeconds() / 2;
        // 첫 판은 웜으로 잡히므로 램프를 재려면 그 뒤에 나타나야 한다.
        collector.collect(List.of(report("seed", 0, NOW)), NOW, 1);
        for (long t = NOW; t <= NOW + half; t += FRESHNESS.toSeconds()) {
            collector.collect(List.of(report("a", Long.MAX_VALUE, t)), t, 1);
        }

        long credit = collector.collect(
                List.of(report("a", Long.MAX_VALUE, NOW + half)), NOW + half, 1);

        assertThat(credit).isEqualTo(Long.MAX_VALUE / 2);
    }

    @Test
    @DisplayName("같은_인스턴스가_두_번_와도_한_번_센다")
    void 같은_인스턴스가_두_번_와도_한_번_센다() {
        // 버전별 키를 함께 읽으면 같은 인스턴스가 두 번 온다. 세면 두 배다.
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 100);

        long credit = collector.collect(
                List.of(report("a", 100, warmed), report("a", 100, warmed)), warmed, 1);

        assertThat(credit).isEqualTo(100);
    }

    @Test
    @DisplayName("설정이_0_이하면_기동에_실패한다")
    void 설정이_0_이하면_기동에_실패한다() {
        assertThatThrownBy(() -> CapacityCollector.of(Duration.ZERO, FRESHNESS, FLOOR, CAP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rampUp");
        assertThatThrownBy(() -> CapacityCollector.of(Duration.ofSeconds(-1), FRESHNESS, FLOOR, CAP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rampUp");
        assertThatThrownBy(() -> CapacityCollector.of(RAMP_UP, Duration.ZERO, FLOOR, CAP))
                .hasMessageContaining("freshness");
        assertThatThrownBy(() -> CapacityCollector.of(RAMP_UP, FRESHNESS, 0, CAP))
                .hasMessageContaining("floor");
        assertThatThrownBy(() -> CapacityCollector.of(RAMP_UP, FRESHNESS, FLOOR, 0))
                .hasMessageContaining("perInstanceCap");
    }

    /**
     * <b>하한은 살아 있는 분모를 따라야 한다.</b> 설정값으로만 재면 노드가 그보다
     * 늘었을 때 노드당 몫이 다시 0 이 되고, 하한을 둔 이유가 사라진다.
     */
    @Test
    @DisplayName("하한이_노드_수를_따른다")
    void 하한이_노드_수를_따른다() {
        CapacityCollector collector = collector();

        // 보고가 하나도 없다. 설정 하한은 5 지만 노드가 열이면 그것으로 부족하다.
        long credit = collector.collect(List.of(), NOW, 10);

        assertThat(credit).isEqualTo(10L * CapacityCollector.IDLE_DIVISOR);
    }

    /**
     * <b>리더가 바뀐 것이 뒷단이 새로 뜬 것은 아니다.</b> 램프 기록은 리더 로컬이라
     * 승계하면 비어 있는데, 그때 전 인스턴스에 램프를 걸면 크레딧이 0 이 된다 —
     * 신선한 보고가 있어 하한도 안 걸린다. 차례가 온 사람이 되돌아가고 신규는
     * 큐도 못 선다.
     */
    @Test
    @DisplayName("처음_본_무리는_이미_돌던_것으로_본다")
    void 처음_본_무리는_이미_돌던_것으로_본다() {
        CapacityCollector collector = collector();

        // 승계 직후 첫 판. 뒷단 셋이 신선하게 보고한다.
        long credit = collector.collect(
                List.of(report("a", 100, NOW), report("b", 100, NOW), report("c", 100, NOW)),
                NOW, 1);

        assertThat(credit).isEqualTo(300);
    }

    /**
     * 첫 판 뒤에 나타난 인스턴스는 진짜 새것이다. 그때는 램프를 건다 — 콜드
     * 인스턴스에 제 몫을 그대로 주면 뜨자마자 무너진다 (F6).
     */
    @Test
    @DisplayName("뒤에_나타난_인스턴스는_램프를_탄다")
    void 뒤에_나타난_인스턴스는_램프를_탄다() {
        CapacityCollector collector = collector();
        collector.collect(List.of(report("a", 100, NOW)), NOW, 1);

        long credit = collector.collect(
                List.of(report("a", 100, NOW + 1), report("b", 100, NOW + 1)), NOW + 1, 1);

        // a 는 온전히 100. b 는 처음 본 순간이라 데운 시간이 0 이므로 몫도 0 이다.
        // **범위로 두지 않는다.** 콜드 인스턴스가 첫 판에 제 몫을 다 받는 결함이
        // 범위 안에 숨는다 — 그게 이 시험이 막으려는 바로 그것이다.
        assertThat(credit).isEqualTo(100);
    }

    /**
     * <b>못 본 것과 새로 뜬 것은 다르다.</b> 보고가 몇 초 끊겼다고 램프 기록을
     * 지우면, 돌아오는 첫 판에 전원이 램프를 다시 타 크레딧이 하한보다도 낮아진다.
     * 정보가 없을 때보다 정보가 돌아온 순간이 더 나빠진다.
     */
    @Test
    @DisplayName("잠깐_못_봐도_램프를_다시_안_탄다")
    void 잠깐_못_봐도_램프를_다시_안_탄다() {
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 100);

        // **틱은 계속 돈다.** 보고만 안 들어온다 — 뒷단 GC 나 뒷단↔레디스 순단이다.
        long 복귀 = warmed + FRESHNESS.toSeconds() * 4;
        for (long t = warmed + 1; t < 복귀; t++) {
            collector.collect(List.of(), t, 1);
        }

        long credit = collector.collect(List.of(report("a", 100, 복귀)), 복귀, 1);

        assertThat(credit).isEqualTo(100);
    }

    /**
     * <b>램프가 만든 0 은 관측이 아니다.</b> 뒷단이 정직하게 "여유 0" 을 보고한
     * 것과, 게이트웨이 자신의 램프 계수가 0 을 만든 것은 다르다. 뒤엣것에 하한을
     * 안 걸면 복귀 첫 판이 하한보다 낮아진다 — 창을 아무리 늘려도 그 너머에서
     * 같은 일이 난다.
     */
    @Test
    @DisplayName("램프가_만든_0_에는_하한을_쓴다")
    void 램프가_만든_0_에는_하한을_쓴다() {
        CapacityCollector collector = collector();
        // 첫 판을 웜으로 안 보게 한 뒤, 처음 보는 인스턴스가 신선하게 보고한다.
        collector.collect(List.of(report("seed", 0, NOW)), NOW, 1);

        long credit = collector.collect(
                List.of(report("seed", 0, NOW), report("cold", 500, NOW)), NOW, 2);

        // 램프 때문에 합이 0 이다. 그래도 하한 아래로는 안 간다.
        assertThat(credit).isEqualTo(FLOOR);
    }

    /**
     * 뒷단이 신선하게 "여유 0" 을 보고한 것은 정확한 백프레셔다. 거기에 하한을
     * 얹으면 명시적 신호를 무시하고 계속 민다.
     */
    @Test
    @DisplayName("보고한_0_에는_하한을_안_쓴다")
    void 보고한_0_에는_하한을_안_쓴다() {
        CapacityCollector collector = collector();

        long credit = collector.collect(List.of(report("a", 0, NOW)), NOW, 2);

        assertThat(credit).isZero();
    }

    /** 창과 정확히 같은 공백은 아직 산다. 경계를 한 칸 옮겨도 안 죽으면 안 잰 것이다. */
    @Test
    @DisplayName("창과_같은_공백은_아직_산다")
    void 창과_같은_공백은_아직_산다() {
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 100);

        long edge = warmed + RAMP_UP.toSeconds();
        collector.collect(List.of(), edge, 1);
        long back = edge + 1;

        assertThat(collector.collect(List.of(report("a", 100, back)), back, 1)).isEqualTo(100);
    }
}

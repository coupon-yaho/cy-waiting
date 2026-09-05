package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.routing.InstanceAddress;
import com.kafkick.waiting.domain.routing.InstanceRouting;
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
        // **첫 회차는 이미 돌던 무리다.** 승계 직후를 콜드로 보면 크레딧이 0 이 된다.
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
    @DisplayName("빈_회차는_첫_회차_표시를_안_태운다")
    void 빈_회차는_첫_회차_표시를_안_태운다() {
        // **첫 회차 표시는 신선한 보고를 처음 본 회차에 쓴다.** 보고가 신선도
        // 창을 한 번 넘긴 회차가 그 표시를 태우면, 다음 회차에 보고가 신선해져도
        // 이미 돌던 무리로 못 보고 램프를 0 부터 다시 탄다. 그동안 크레딧이
        // 하한에 묶여 한산 통과 상한이 0 이다.
        CapacityCollector collector = collector();
        collector.collect(List.of(), NOW, 1);

        long credit = collector.collect(List.of(report("warm", 300, NOW + 1)), NOW + 1, 1);

        assertThat(credit).as("신선한 보고를 처음 본 회차가 첫 회차다").isEqualTo(300);
    }

    @Test
    @DisplayName("낡은_보고만_온_회차도_안_태운다")
    void 낡은_보고만_온_회차도_안_태운다() {
        // 보고가 오긴 왔는데 전부 낡았다. 관측이 없었던 것과 같다.
        CapacityCollector collector = collector();
        long 낡은 = NOW - FRESHNESS.toSeconds() - 1;
        collector.collect(List.of(report("warm", 300, 낡은)), NOW, 1);

        long credit = collector.collect(List.of(report("warm", 300, NOW + 1)), NOW + 1, 1);

        assertThat(credit).isEqualTo(300);
    }

    @Test
    @DisplayName("등록을_따로_안_불러도_램프가_걸린다")
    void 등록을_따로_안_불러도_램프가_걸린다() {
        // **두 번 불러야 하는 계약을 두면 빠뜨렸을 때 조용히 0 을 낸다.** 보고가
        // 관측됐다는 것이 곧 처음 본 시각이 있다는 뜻이라, 그 상태는 운영에
        // 존재할 수 없는데 시험만 만들 수 있게 된다.
        CapacityCollector collector = collector();
        // 첫 회차는 이미 돌던 무리로 본다. 진짜 새것은 그 뒤에 나타난 쪽이다.
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
        // 이번 회차에 없다고 지우면 정상 인스턴스가 틱마다 램프를 다시 탄다.
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

        collector.observationFailed(1);

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
        // 첫 회차는 웜으로 잡히므로 램프를 재려면 그 뒤에 나타나야 한다.
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

        // 승계 직후 첫 회차. 뒷단 셋이 신선하게 보고한다.
        long credit = collector.collect(
                List.of(report("a", 100, NOW), report("b", 100, NOW), report("c", 100, NOW)),
                NOW, 1);

        assertThat(credit).isEqualTo(300);
    }

    /**
     * 첫 회차 뒤에 나타난 인스턴스는 진짜 새것이다. 그때는 램프를 건다 — 콜드
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
        // **범위로 두지 않는다.** 콜드 인스턴스가 첫 회차에 제 몫을 다 받는 결함이
        // 범위 안에 숨는다 — 그게 이 시험이 막으려는 바로 그것이다.
        assertThat(credit).isEqualTo(100);
    }

    /**
     * <b>못 본 것과 새로 뜬 것은 다르다.</b> 보고가 몇 초 끊겼다고 램프 기록을
     * 지우면, 돌아오는 첫 회차에 전원이 램프를 다시 타 크레딧이 하한보다도 낮아진다.
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
     * 안 걸면 복귀 첫 회차가 하한보다 낮아진다 — 창을 아무리 늘려도 그 너머에서
     * 같은 일이 난다.
     */
    @Test
    @DisplayName("램프가_만든_0_에는_하한을_쓴다")
    void 램프가_만든_0_에는_하한을_쓴다() {
        CapacityCollector collector = collector();
        // 첫 회차를 웜으로 안 보게 한 뒤, 처음 보는 인스턴스가 신선하게 보고한다.
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

    /**
     * <b>램프가 만든 것은 0 이 아닐 때도 우리가 만든 값이다.</b> 하한을 0 에만
     * 걸면, 램프 중 합이 하한과 0 사이인 구간이 통째로 비어 버린다. 그 구간에서
     * 노드당 몫이 {@code IDLE_DIVISOR} 아래로 내려가 한산 통과 상한이 0 이 되고,
     * 리미터는 상한 0 을 무조건 거절로 처리한다 — 안 몰리는 쿠폰의 요청이 전
     * 노드에서 막힌다 (R1).
     */
    @Test
    @DisplayName("램프가_만든_부족분에도_하한을_쓴다")
    void 램프가_만든_부족분에도_하한을_쓴다() {
        CapacityCollector collector = collector();
        // 첫 회차를 웜으로 안 보게 한 뒤, 처음 보는 뒷단이 넉넉하게 보고한다.
        collector.collect(List.of(report("seed", 0, NOW)), NOW, 8);
        collector.collect(
                List.of(report("seed", 0, NOW + 1), report("fresh", 1_000, NOW + 1)), NOW + 1, 8);

        // 램프 1초. 1000 중 16 만 쓸 수 있다 — 0 이 아니라 그냥 적다.
        long credit = collector.collect(
                List.of(report("seed", 0, NOW + 2), report("fresh", 1_000, NOW + 2)), NOW + 2, 8);

        // 노드가 여덟이면 하한도 여덟을 받쳐야 한다.
        assertThat(credit).isEqualTo(8L * CapacityCollector.IDLE_DIVISOR);
    }

    /**
     * <b>램프가 깎았으면 남은 값의 크기는 안 본다.</b> 깎기 전 합이 하한보다
     * 적어도 마찬가지다 — 뒷단이 못 받겠다고 말한 것이 아니라 우리가 아직 안
     * 믿기로 한 것뿐이다. 여기서 하한을 안 걸면 노드가 여럿일 때 한산 통과가
     * 통째로 막힌다.
     */
    @Test
    @DisplayName("깎기_전_합이_하한보다_적어도_하한을_쓴다")
    void 깎기_전_합이_하한보다_적어도_하한을_쓴다() {
        CapacityCollector collector = collector();
        collector.collect(List.of(report("seed", 0, NOW)), NOW, 8);

        // 처음 보는 뒷단이 12 를 보고한다. 노드 여덟의 하한 40 에는 못 미친다.
        long credit = collector.collect(
                List.of(report("seed", 0, NOW + 1), report("cold", 12, NOW + 1)), NOW + 1, 8);

        assertThat(credit).isEqualTo(8L * CapacityCollector.IDLE_DIVISOR);
    }

    /**
     * <b>하한이 없는 여유를 만들어 내지는 않는다.</b> 램프가 손대지 않은 값은
     * 뒷단이 실제로 가진 것이다. 그것이 하한보다 적다고 올리면 뒷단이 못 받는
     * 만큼을 우리가 지어내는 셈이다.
     */
    @Test
    @DisplayName("램프_밖의_부족은_그대로_둔다")
    void 램프_밖의_부족은_그대로_둔다() {
        CapacityCollector collector = collector();
        // 다 데워진 뒷단이 3 을 보고한다. 램프는 여기 손대지 않았다.
        long warmed = warm(collector, "a", 3);

        long credit = collector.collect(List.of(report("a", 3, warmed + 1)), warmed + 1, 2);

        assertThat(credit).isEqualTo(3);
    }

    /**
     * <b>못 읽은 값에는 시효가 있어야 한다.</b> 직전 값을 지키는 것은 한 회차가
     * 실패했을 때 맞는 답이지만, 그것이 무기한이면 뒷단이 통째로 죽어도 옛
     * 크레딧으로 계속 민다. 분모(노드 수)는 유지가 과소 방향이라 안전하지만
     * 분자(크레딧)는 과다 방향이다.
     */
    @Test
    @DisplayName("읽기_실패가_이어지면_값이_줄어든다")
    void 읽기_실패가_이어지면_값이_줄어든다() {
        CapacityCollector collector = collector();
        long 관측 = collector.collect(List.of(report("a", 10_000, NOW)), NOW, 1);

        // 유예 안에서는 그대로다. 한 회차 실패했다고 조이면 순단마다 흔들린다.
        for (int i = 0; i < CapacityCollector.HOLD_ROUNDS; i++) {
            collector.observationFailed(1);
        }
        assertThat(collector.lastKnown()).isEqualTo(관측);

        collector.observationFailed(1);

        assertThat(collector.lastKnown()).isEqualTo(관측 / 2);
    }

    /**
     * 아무리 줄어도 바닥 아래로는 안 간다. 그 아래는 전면 차단이다.
     *
     * <p><b>바닥은 걷을 때와 같은 값이어야 한다.</b> 설정값만 보면 노드가 그보다
     * 늘었을 때 노드당 몫이 유휴 비율 아래로 내려가 한산 통과가 전 노드에서 막힌다.
     */
    @Test
    @DisplayName("줄어도_노드를_받치는_바닥_아래로는_안_간다")
    void 줄어도_노드를_받치는_바닥_아래로는_안_간다() {
        CapacityCollector collector = collector();
        // **설정 하한이 지배하지 않을 만큼 노드를 늘린다.** 안 그러면 이 시험이
        // 노드 수가 아니라 설정값을 재게 된다.
        int 노드 = (int) (FLOOR / CapacityCollector.IDLE_DIVISOR) + 4;
        collector.collect(List.of(report("a", 10_000, NOW)), NOW, 노드);

        for (int i = 0; i < 100; i++) {
            collector.observationFailed(노드);
        }

        assertThat(collector.lastKnown())
                .isEqualTo((long) 노드 * CapacityCollector.IDLE_DIVISOR);
    }

    /**
     * <b>뒷단이 스스로 0 이라고 말한 뒤에는 감쇠가 그것을 올리지 않는다.</b>
     * 그건 관측이고, 거기에 바닥을 얹으면 죽었다고 말한 뒷단에 다시 밀어넣는다.
     * 서킷이 half-open 으로 갈 때 시험 트래픽이 아니라 상시 유입이 도달해 있게 된다.
     */
    @Test
    @DisplayName("보고한_0_은_감쇠가_안_올린다")
    void 보고한_0_은_감쇠가_안_올린다() {
        CapacityCollector collector = collector();
        assertThat(collector.collect(List.of(report("a", 0, NOW)), NOW, 1)).isZero();

        for (int i = 0; i < 100; i++) {
            collector.observationFailed(1);
        }

        assertThat(collector.lastKnown()).isZero();
    }

    /**
     * 비리더 구간의 실패는 남의 회차다. 이월하면 재승계 첫 회차에 유예를 건너뛰고
     * 곧바로 반토막을 내는데, 그 대상은 몇 분 전 관측치다.
     */
    @Test
    @DisplayName("리더를_다시_잡으면_유예가_처음부터다")
    void 리더를_다시_잡으면_유예가_처음부터다() {
        CapacityCollector collector = collector();
        long 관측 = collector.collect(List.of(report("a", 10_000, NOW)), NOW, 1);
        // **성공 회차를 사이에 두지 않는다.** 성공이 카운터를 되돌리므로, 그러면
        // 승계 알림을 지워도 이 시험이 통과한다.
        for (int i = 0; i < CapacityCollector.HOLD_ROUNDS; i++) {
            collector.observationFailed(1);
        }

        collector.leadershipAcquired();
        for (int i = 0; i < CapacityCollector.HOLD_ROUNDS; i++) {
            collector.observationFailed(1);
        }

        assertThat(collector.lastKnown()).isEqualTo(관측);
    }

    /**
     * 못 읽는 동안 노드 수가 바뀔 수 있다. 옛 바닥을 들고 있으면 양쪽으로 다
     * 틀린다 — 늘면 그만큼 낮아 한산 통과가 막히고, 줄면 그만큼 높아 장애 중에
     * 실제 바닥보다 많이 민다.
     */
    @Test
    @DisplayName("감쇠_바닥은_지금_노드_수를_따른다")
    void 감쇠_바닥은_지금_노드_수를_따른다() {
        int 노드_많음 = (int) (FLOOR / CapacityCollector.IDLE_DIVISOR) + 4;
        CapacityCollector 늘어난_쪽 = collector();
        늘어난_쪽.collect(List.of(report("a", 10_000, NOW)), NOW, 1);
        CapacityCollector 줄어든_쪽 = collector();
        줄어든_쪽.collect(List.of(report("a", 10_000, NOW)), NOW, 노드_많음);

        for (int i = 0; i < 100; i++) {
            늘어난_쪽.observationFailed(노드_많음);
            줄어든_쪽.observationFailed(1);
        }

        assertThat(늘어난_쪽.lastKnown())
                .isEqualTo((long) 노드_많음 * CapacityCollector.IDLE_DIVISOR);
        // 노드가 줄면 설정 하한까지 내려간다. 옛 바닥(50)에 멎으면 안 된다.
        assertThat(줄어든_쪽.lastKnown()).isEqualTo(FLOOR);
    }

    /** 한 회차라도 성공하면 유예가 다시 찬다. 안 그러면 순단이 쌓여 조여진다. */
    @Test
    @DisplayName("한_회차_성공하면_유예가_다시_찬다")
    void 한_회차_성공하면_유예가_다시_찬다() {
        CapacityCollector collector = collector();
        long 관측 = collector.collect(List.of(report("a", 10_000, NOW)), NOW, 1);
        for (int i = 0; i < CapacityCollector.HOLD_ROUNDS + 1; i++) {
            collector.observationFailed(1);
        }

        long 다시 = collector.collect(List.of(report("a", 10_000, NOW + 1)), NOW + 1, 1);
        for (int i = 0; i < CapacityCollector.HOLD_ROUNDS; i++) {
            collector.observationFailed(1);
        }

        assertThat(관측).isEqualTo(다시);
        assertThat(collector.lastKnown()).isEqualTo(다시);
    }

    /**
     * <b>뒷단이 조금 앞선 것은 죽은 것이 아니다.</b> 나이를 두 벽시계의 차로
     * 재므로 뒷단 시계가 1초만 앞서도 나이가 음수가 되고, 지금은 그것을 낡음으로
     * 봐서 <b>전 인스턴스가 한꺼번에 사라진다.</b> 뒷단은 같은 NTP 를 보므로
     * 어긋나면 다 같이 어긋난다 — 그 순간 크레딧이 하한으로 떨어진다.
     */
    @Test
    @DisplayName("조금_앞선_보고도_신선하다")
    void 조금_앞선_보고도_신선하다() {
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 100);

        // 뒷단 시계가 1초 앞선다. 관측 자체는 방금 것이다.
        long credit = collector.collect(List.of(report("a", 100, warmed + 1)), warmed, 1);

        assertThat(credit).isEqualTo(100);
    }

    /**
     * 그렇다고 아무리 앞서도 받지는 않는다. 창보다 멀리 앞선 값은 시계가 어긋난
     * 것이 아니라 보고가 깨진 것이다 — 받으면 죽은 인스턴스가 영영 신선해진다.
     */
    @Test
    @DisplayName("허용치보다_앞선_보고는_안_받는다")
    void 허용치보다_앞선_보고는_안_받는다() {
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 100);

        // **경계를 한 칸 넘긴다.** 창 전체를 앞쪽으로 열면 죽은 인스턴스의
        // 마지막 보고가 창의 두 배 동안 살아 있다 — 여기서 그 폭을 잠근다.
        long credit = collector.collect(List.of(report("a", 100, warmed + 2)), warmed, 1);

        // 신선한 보고가 없으니 하한이다.
        assertThat(credit).isEqualTo(FLOOR);
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

    /** 하한 0 은 하한이 없는 것과 같다. 경계를 한 칸 옮기면 그 뜻이 갈린다. */
    @Test
    @DisplayName("하한이_0이면_안_뜬다")
    void 하한이_0이면_안_뜬다() {
        assertThatThrownBy(() -> CapacityCollector.of(RAMP_UP, FRESHNESS, 0, CAP))
                .isInstanceOf(IllegalArgumentException.class);
        // 1 은 뜬다. 여기까지 막으면 하한을 못 쓰는 설정이 된다.
        assertThat(CapacityCollector.of(RAMP_UP, FRESHNESS, 1, CAP).lastKnown()).isOne();
    }

    @Test
    @DisplayName("인스턴스_상한이_0이면_안_뜬다")
    void 인스턴스_상한이_0이면_안_뜬다() {
        assertThatThrownBy(() -> CapacityCollector.of(RAMP_UP, FRESHNESS, FLOOR, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(CapacityCollector.of(RAMP_UP, FRESHNESS, FLOOR, 1).lastKnown())
                .isEqualTo(FLOOR);
    }

    /**
     * <b>같은 인스턴스가 두 번 오면 새 것을 쓴다.</b> 버전별 키를 함께 읽는
     * 롤아웃 중에 옛 보고가 이기면 그 인스턴스가 낡음으로 판정돼 사라진다.
     */
    @Test
    @DisplayName("같은_인스턴스는_새_보고가_이긴다")
    void 같은_인스턴스는_새_보고가_이긴다() {
        CapacityCollector collector = collector();

        // 순서를 바꿔 넣어도 결과가 같아야 한다 — 시각이 이기는 것이지 순서가 아니다.
        long 새것_먼저 = collector.collect(
                List.of(report("a", 100, NOW), report("a", 10, NOW - 1)), NOW, 1);
        long 옛것_먼저 = collector().collect(
                List.of(report("a", 10, NOW - 1), report("a", 100, NOW)), NOW, 1);

        assertThat(새것_먼저).isEqualTo(100);
        assertThat(옛것_먼저).isEqualTo(100);
    }

    /** 시각이 같으면 어느 쪽이든 같은 값이어야 한다. 경계에서 값이 흔들리면 안 된다. */
    @Test
    @DisplayName("보고_시각이_같으면_한_번만_센다")
    void 보고_시각이_같으면_한_번만_센다() {
        CapacityCollector collector = collector();

        long credit = collector.collect(
                List.of(report("a", 100, NOW), report("a", 100, NOW)), NOW, 1);

        // 두 번 세면 200 이다. 같은 인스턴스는 하나다.
        assertThat(credit).isEqualTo(100);
    }

    /** 하한이 답이 된 회차만 기록한다. 안 걸린 회차까지 남기면 배분이 매번 하한을 얹는다. */
    @Test
    @DisplayName("하한이_안_걸린_회차는_0을_남긴다")
    void 하한이_안_걸린_회차는_0을_남긴다() {
        CapacityCollector collector = collector();
        collector.collect(List.of(report("seed", 0, NOW)), NOW, 1);

        collector.collect(List.of(report("seed", 10_000, NOW + 1)), NOW + 1, 1);

        assertThat(collector.lastFloor()).isZero();
    }

    @Test
    @DisplayName("하한이_걸린_회차는_그_값을_남긴다")
    void 하한이_걸린_회차는_그_값을_남긴다() {
        CapacityCollector collector = collector();

        collector.collect(List.of(), NOW, 3);

        assertThat(collector.lastFloor())
                .isEqualTo(Math.max(FLOOR, 3L * CapacityCollector.IDLE_DIVISOR));
    }

    /** 램프 창과 정확히 같으면 다 데운 것이다. 경계를 한 칸 옮기면 마지막 구간이 깎인다. */
    @Test
    @DisplayName("램프_창과_같으면_전액을_쓴다")
    void 램프_창과_같으면_전액을_쓴다() {
        CapacityCollector collector = collector();
        collector.collect(List.of(report("seed", 0, NOW)), NOW, 1);
        collector.collect(List.of(report("seed", 0, NOW + 1), report("a", 600, NOW + 1)), NOW + 1, 1);

        long 창_끝 = NOW + 1 + RAMP_UP.toSeconds();
        long credit = collector.collect(
                List.of(report("seed", 0, 창_끝), report("a", 600, 창_끝)), 창_끝, 1);

        assertThat(credit).isEqualTo(600);
    }

    /** 창 한 칸 앞에서는 아직 덜 데웠다. 위 시험과 짝이라야 경계가 잠긴다. */
    @Test
    @DisplayName("램프_창_한_칸_앞은_덜_데웠다")
    void 램프_창_한_칸_앞은_덜_데웠다() {
        CapacityCollector collector = collector();
        collector.collect(List.of(report("seed", 0, NOW)), NOW, 1);
        collector.collect(List.of(report("seed", 0, NOW + 1), report("a", 600, NOW + 1)), NOW + 1, 1);

        long 창_직전 = NOW + RAMP_UP.toSeconds();
        long credit = collector.collect(
                List.of(report("seed", 0, 창_직전), report("a", 600, 창_직전)), 창_직전, 1);

        // 창의 59/60 만큼 데웠다. 범위로 두면 0 이나 하한도 통과한다.
        assertThat(credit).isEqualTo(590);
    }

    private static CapacityReport 주소_있는_보고(String id, String addr, long credits, long at) {
        return new CapacityReport(id, credits, at,
                InstanceAddress.parse(addr).orElseThrow());
    }

    /**
     * <b>라우팅 목록을 합산과 같은 회차에서 만든다.</b> 따로 돌면 합산에 든
     * 인스턴스와 보낼 인스턴스가 갈리고, 그 갈림은 램프 구간에만 나타난다.
     */
    @Test
    @DisplayName("주소가_있는_신선한_보고만_라우팅에_실린다")
    void 주소가_있는_신선한_보고만_라우팅에_실린다() {
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 100);

        collector.collect(List.of(
                주소_있는_보고("a", "10.0.1.7:8080", 100, warmed),
                // 주소를 안 실었다 — 크레딧에는 들고 라우팅에서만 빠진다.
                report("b", 200, warmed),
                // 낡았다 — 어느 쪽에도 안 든다.
                주소_있는_보고("c", "10.0.1.9:8080", 300, warmed - 100)), warmed, 1);

        assertThat(collector.routable()).extracting(InstanceRouting::instanceId)
                .containsExactly("a");
    }

    /**
     * <b>램프가 깎은 몫이 라우팅에도 실린다</b> (F6 · G9.12). 갓 뜬 인스턴스로
     * 정상 비율만큼 보내면 그 대가 그대로 무너진다.
     */
    @Test
    @DisplayName("갓_뜬_인스턴스는_적은_몫으로_실린다")
    void 갓_뜬_인스턴스는_적은_몫으로_실린다() {
        CapacityCollector collector = collector();
        long warmed = warm(collector, "old", 100);

        collector.collect(List.of(
                주소_있는_보고("old", "10.0.1.7:8080", 100, warmed),
                주소_있는_보고("new", "10.0.1.8:8080", 100, warmed)), warmed, 1);

        assertThat(collector.routable())
                .filteredOn(i -> i.instanceId().equals("new"))
                .singleElement()
                .extracting(InstanceRouting::credits)
                .satisfies(credits -> assertThat((long) credits).isLessThan(100));
        assertThat(collector.routable())
                .filteredOn(i -> i.instanceId().equals("old"))
                .singleElement()
                .extracting(InstanceRouting::credits)
                .isEqualTo(100L);
    }

    /**
     * <b>하한을 만들었으면 라우팅 몫에도 싣는다.</b>
     *
     * <p>램프가 전부를 0 으로 깎으면 판정은 하한으로 통과시키는데, 라우팅 몫이
     * 전부 0 이면 고르개가 후보로 안 본다 — 통과시켜 놓고 갈 곳이 없다.
     */
    @Test
    @DisplayName("램프가_전부_깎아도_보낼_곳이_남는다")
    void 램프가_전부_깎아도_보낼_곳이_남는다() {
        CapacityCollector collector = collector();
        // 첫 회차를 지나야 새 인스턴스에 램프가 걸린다 — 첫 회차의 무리는
        // 이미 돌던 것으로 보기 때문이다.
        long 지금 = warm(collector, "old", 100);

        long 크레딧 = collector.collect(List.of(
                주소_있는_보고("a", "10.0.1.7:8080", 100, 지금),
                주소_있는_보고("b", "10.0.1.8:8080", 100, 지금)), 지금, 1);

        assertThat(크레딧).as("전제 — 하한이 걸렸다").isPositive();
        assertThat(collector.routable())
                .as("판정이 통과시키는데 보낼 곳이 없으면 안 된다")
                .isNotEmpty()
                .allSatisfy(i -> assertThat(i.credits()).isPositive());
        assertThat(collector.routable().stream().mapToLong(InstanceRouting::credits).sum())
                .as("실린 몫의 합이 발행한 크레딧과 같다")
                .isEqualTo(크레딧);
    }

    /** 한 회차도 안 돌았으면 비어 있다. 없는 목록으로 라우팅하면 보낼 곳이 없다. */
    @Test
    @DisplayName("아직_안_돌았으면_비어_있다")
    void 아직_안_돌았으면_비어_있다() {
        assertThat(collector().routable()).isEmpty();
    }

    /** 신선한 보고가 하나도 없으면 목록도 빈다 — 낡은 주소로 보내면 안 된다. */
    @Test
    @DisplayName("전부_낡으면_목록도_빈다")
    void 전부_낡으면_목록도_빈다() {
        CapacityCollector collector = collector();
        long warmed = warm(collector, "a", 100);
        collector.collect(List.of(주소_있는_보고("a", "10.0.1.7:8080", 100, warmed)), warmed, 1);

        collector.collect(List.of(), warmed + 100, 1);

        assertThat(collector.routable()).isEmpty();
    }
}

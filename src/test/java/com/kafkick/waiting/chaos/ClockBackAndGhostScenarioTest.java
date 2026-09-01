package com.kafkick.waiting.chaos;

import com.kafkick.waiting.adapter.redis.ServerClock;
import com.kafkick.waiting.control.CapacityCollector;
import com.kafkick.waiting.control.CapacityReport;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * C17 — 복제본이 뒤진 시계로 승격하고, 시계가 앞선 뒷단이 죽는다 (8.3.5 · 5절).
 *
 * <p>둘 다 신선도 판정이 기준 시각에 기대기 때문에 생긴다. 앞엣것은 크레딧을 하한에
 * 박고, 뒤엣것은 <b>없는 인스턴스 몫을 회복 첫 구간에 싣는다</b> — 뒷단이 가장
 * 차가울 때 유입이 가장 높아진다 (F6·RC4).
 */
@Tag("chaos")
class ClockBackAndGhostScenarioTest {

    private static final long NOW = 1_800_000_000L;

    private static final Duration 램프 = Duration.ofSeconds(60);

    private static final Duration 신선도 = Duration.ofSeconds(3);

    private static final long 하한 = 10;

    private static final int 노드 = 1;

    /** 신선한 보고가 하나도 없을 때 답이 되는 값. */
    private static final long 바닥 =
            Math.max(하한, (long) 노드 * CapacityCollector.IDLE_DIVISOR);

    /** 승격한 복제본이 뒤로 가는 폭. */
    private static final long 역행초 = 30;

    /**
     * 뒷단 보고가 기준 시각보다 앞서도 받아 주는 폭.
     *
     * <p><b>수집기의 private 상수를 여기 다시 적는다.</b> 값이 아니라 <b>정책</b>을
     * 재기 때문이다 — 앞섬 허용치는 신선도 창과 따로여야 하고, 그 "따로" 가 유령의
     * 수명을 정한다. 수집기에서 이 값을 늘리면 아래 판정이 바로 문다.
     */
    private static final long 앞섬_허용 = 1;

    private static final long 여유 = 10_000;

    private CapacityCollector 수집기() {
        return CapacityCollector.of(램프, 신선도, 하한, 100_000);
    }

    /**
     * 세 구간을 한 판정으로 잇는다.
     *
     * <p>유령은 <b>회복 구간에 넣는다</b>. 죽은 인스턴스의 몫이 실리는 순간이
     * 회복 첫 구간이고, 별도 구간으로 떼면 그 겹침이 안 재진다.
     */
    @Test
    @DisplayName("C17_시계가_뒤로_가도_크레딧이_유지되고_유령은_창_안에서만_세어진다")
    void C17_시계가_뒤로_가도_크레딧이_유지되고_유령은_창_안에서만_세어진다() {
        ServerClock 시계 = ServerClock.create();
        CapacityCollector 본 = 수집기();
        long[] 정상 = new long[1];
        long[] 승격_직후 = new long[1];
        long[] 보정_횟수 = new long[2];
        long[] 역행_폭 = new long[1];
        long[] 유지 = new long[2];
        long[] 회복 = new long[2];
        long[] 유령 = new long[3];

        ChaosScenario.named("C17 시계 역행과 유령 인스턴스")
                .baseline(() -> {
                    보정_횟수[0] = 시계.skew().appliedCount();
                    long 기준 = 시계.observe(NOW);
                    정상[0] = 본.collect(보고("i1", NOW), 기준, 노드);
                })
                .inject(() -> {
                    // 승격한 복제본이 뒤진 시각을 준다. 뒷단은 계속 정상 시각을 쓴다.
                    승격_직후[0] = 시계.observe(NOW - 역행초);
                    보정_횟수[1] = 시계.skew().appliedCount();
                    역행_폭[1 - 1] = 시계.skew().maxSkewMicros();
                })
                .duringFault(() -> {
                    // 보정한 시각으로 걷는다 — 뒷단 보고는 이제 기준보다 앞선다.
                    유지[0] = 본.collect(보고("i1", NOW + 1), 승격_직후[0], 노드);
                    // **대조군.** 보정을 안 하면 어떻게 되는지를 값으로 남긴다.
                    // 이 판이 하한을 안 내면 이 시나리오는 차이가 없는 것을 재는 것이다.
                    유지[1] = 수집기().collect(보고("i2", NOW + 1), NOW - 역행초, 노드);
                })
                .recover(() -> {
                    // 시계가 따라잡는다. 바닥값을 놓아야 영영 옛 시각에 안 머문다.
                    회복[0] = 시계.observe(NOW + 1);
                })
                .afterRecovery(() -> {
                    회복[1] = 본.collect(보고("i1", NOW + 1), 회복[0], 노드);
                    // 시계가 허용치만큼 앞선 뒷단이 마지막 보고를 남기고 죽는다.
                    long 마지막_보고 = NOW + 앞섬_허용;
                    CapacityCollector 유령_판 = 수집기();
                    유령_판.collect(보고("유령", 마지막_보고), NOW, 노드);
                    유령[0] = 유령_판.collect(보고("유령", 마지막_보고),
                            마지막_보고 + 신선도.toSeconds(), 노드);
                    유령[1] = 유령_판.collect(보고("유령", 마지막_보고),
                            마지막_보고 + 신선도.toSeconds() + 1, 노드);
                    // **허용치를 넘게 앞선 보고는 처음부터 안 받는다.** 받으면 그
                    // 인스턴스가 창의 두 배 가까이 살아 유령 몫이 그만큼 길어진다.
                    유령[2] = 수집기().collect(보고("먼_미래", NOW + 앞섬_허용 + 1), NOW, 노드);
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 전제 — 평시에 관측이 실렸는가. 안 실렸으면 뒤 구간이
                        // 하한끼리 비교하는 판이 된다.
                        평시에_관측이_실렸다(정상[0]),
                        // 단조 가드가 걸렸는가. 안 걸면 뒷단 보고가 전부 미래가
                        // 되어 한꺼번에 낡음이 된다 (A-9).
                        바닥값이_걸렸다(승격_직후[0]),
                        // **한 번만이다.** 양수만 보면 중복 기록도 통과한다.
                        보정이_한_번_기록됐다(보정_횟수[0], 보정_횟수[1]),
                        // 역행 폭이 지표에 남는가. 조용히 보정하면 왜 그랬는지를
                        // 영영 못 밝힌다.
                        역행_폭이_남았다(역행_폭[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 보정한 시각으로 걷으면 크레딧이 유지된다.
                        크레딧이_유지됐다(정상[0], 유지[0]),
                        // 전제 — 보정 없이는 하한이다. 이 대조가 안 서면 위 판정이
                        // 아무 차이도 안 재는 것이다.
                        보정_없이는_하한이다(유지[1])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        // 시계가 따라잡으면 바닥값을 놓는다.
                        바닥값을_놓았다(회복[0]),
                        // RC6 — 크레딧이 승격 전 값으로 수렴한다.
                        RecoveryCriteria.notConverged("가용량 크레딧", 정상[0], 회복[1]),
                        // 유령은 창 안에서만 세어진다.
                        유령이_창_안에서_세어진다(유령[0]),
                        유령이_창_밖에서_사라진다(유령[1]),
                        // 앞섬 허용치가 신선도 창과 따로여야 이 판정이 선다.
                        허용치를_넘는_앞섬은_안_받는다(유령[2])))
                .run();
    }

    private List<CapacityReport> 보고(String instanceId, long at) {
        return List.of(new CapacityReport(instanceId, 여유, at));
    }

    private Optional<String> 평시에_관측이_실렸다(long 정상) {
        return 정상 == 여유 ? Optional.empty()
                : Optional.of("전제 — 평시 크레딧이 %d 가 아니라 %d 다. 관측이 안 실렸다"
                        .formatted(여유, 정상));
    }

    private Optional<String> 바닥값이_걸렸다(long 승격_직후) {
        return 승격_직후 == NOW ? Optional.empty()
                : Optional.of("시계가 뒤로 간 값을 그대로 받았다 — %d (바닥 %d)"
                        .formatted(승격_직후, NOW));
    }

    private Optional<String> 보정이_한_번_기록됐다(long 전, long 후) {
        return 후 - 전 == 1 ? Optional.empty()
                : Optional.of("보정 기록이 %d 건이다 — 역행은 한 번이었다".formatted(후 - 전));
    }

    private Optional<String> 역행_폭이_남았다(long 마이크로초) {
        long 기대 = 역행초 * 1_000_000L;
        return 마이크로초 == 기대 ? Optional.empty()
                : Optional.of("역행 폭이 %d 마이크로초로 남았다 — %d 여야 한다"
                        .formatted(마이크로초, 기대));
    }

    private Optional<String> 크레딧이_유지됐다(long 정상, long 유지) {
        return 정상 == 유지 ? Optional.empty()
                : Optional.of("승격 뒤 크레딧이 %d 에서 %d 로 갈렸다".formatted(정상, 유지));
    }

    private Optional<String> 보정_없이는_하한이다(long 대조) {
        return 대조 == 바닥 ? Optional.empty()
                : Optional.of(("전제 — 보정 없는 판이 %d 다. 바닥 %d 이 아니면 이 "
                        + "시나리오는 차이가 없는 것을 재고 있다").formatted(대조, 바닥));
    }

    private Optional<String> 바닥값을_놓았다(long 회복) {
        return 회복 == NOW + 1 ? Optional.empty()
                : Optional.of("시계가 따라잡았는데 바닥값을 안 놓았다 — %d".formatted(회복));
    }

    private Optional<String> 유령이_창_안에서_세어진다(long 창_안) {
        return 창_안 == 여유 ? Optional.empty()
                : Optional.of(("전제 — 유령이 창 안에서 안 세어졌다 (%d). 사라지는 "
                        + "판정이 정의상 통과한다").formatted(창_안));
    }

    private Optional<String> 유령이_창_밖에서_사라진다(long 창_밖) {
        return 창_밖 == 바닥 ? Optional.empty()
                : Optional.of("유령이 창 %d초 + 허용치 %d초를 넘겨 %d 만큼 세어진다"
                        .formatted(신선도.toSeconds(), 앞섬_허용, 창_밖));
    }

    private Optional<String> 허용치를_넘는_앞섬은_안_받는다(long 크레딧) {
        return 크레딧 == 바닥 ? Optional.empty()
                : Optional.of("허용치 %d초를 넘게 앞선 보고를 받았다 — 크레딧 %d"
                        .formatted(앞섬_허용, 크레딧));
    }
}

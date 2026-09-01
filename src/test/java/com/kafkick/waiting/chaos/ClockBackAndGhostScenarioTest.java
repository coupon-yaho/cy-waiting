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
 * C17 — 복제본이 뒤진 시계로 승격하고, 시계가 앞선 뒷단이 죽는다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C17 절이 든다. 여기는
 * 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class ClockBackAndGhostScenarioTest {

    private static final long NOW = 1_800_000_000L;

    private static final Duration 램프 = Duration.ofSeconds(60);

    private static final Duration 신선도 = Duration.ofSeconds(3);

    private static final long 하한 = 10;

    private static final int 노드 = 1;

    /**
     * 신선한 보고가 하나도 없을 때 답이 되는 값. <b>리터럴로 둔다.</b>
     *
     * <p>프로덕션 식을 베끼면 그 식의 상수를 바꿔도 기대값이 같이 움직여 아무것도
     * 안 잰다. 노드 하나에 하한 10 이면 노드 몫(2)이 못 이긴다.
     */
    private static final long 바닥 = 10;

    /**
     * 램프 4초째의 몫. 손으로 잰 값이다 — 10,000 을 60초에 걸쳐 여는 곡선 위의 점이다.
     */
    private static final long 램프_4초 = 666;

    /** 승격한 복제본이 뒤로 가는 폭. */
    private static final long 역행초 = 30;

    /** 수집기의 private 상수를 옮겨 적었다. 늘리거나 줄이면 아래 판정이 문다. */
    private static final long 앞섬_허용 = 1;

    private static final long 여유 = 10_000;

    private CapacityCollector 수집기() {
        return CapacityCollector.of(램프, 신선도, 하한, 100_000);
    }

    /**
     * 세 구간을 한 판정으로 잇는다.
     *
     * <p>유령은 회복 구간에서 잰다. 죽은 인스턴스의 몫이 <b>갓 뜬 뒷단의 콜드
     * 램프값과 같은 판에 실리는 것</b>이 이 시나리오가 재려는 것이라, 그 판을
     * 회복 안에서 실제로 만든다.
     */
    @Test
    @DisplayName("C17_시계가_뒤로_가도_크레딧이_유지되고_유령은_창_안에서만_세어진다")
    void C17_시계가_뒤로_가도_크레딧이_유지되고_유령은_창_안에서만_세어진다() {
        ServerClock 시계 = ServerClock.create();
        CapacityCollector 주_수집기 = 수집기();
        long[] 정상 = new long[1];
        long[] 승격_직후 = new long[1];
        long[] 보정_횟수 = new long[2];
        long[] 역행_폭 = new long[1];
        long[] 유지 = new long[1];
        long[] 회복 = new long[2];
        long[] 유령 = new long[3];
        long[] 겹친_판 = new long[3];

        ChaosScenario.named("C17 시계 역행과 유령 인스턴스")
                .baseline(() -> {
                    보정_횟수[0] = 시계.skew().appliedCount();
                    long 기준 = 시계.observe(NOW);
                    정상[0] = 주_수집기.collect(보고("i1", NOW), 기준, 노드);
                })
                .inject(() -> {
                    승격_직후[0] = 시계.observe(NOW - 역행초);
                    보정_횟수[1] = 시계.skew().appliedCount();
                    역행_폭[0] = 시계.skew().maxSkewMicros();
                })
                .duringFault(() ->
                        유지[0] = 주_수집기.collect(보고("i1", NOW + 1), 승격_직후[0], 노드))
                .recover(() -> 회복[0] = 시계.observe(NOW + 1))
                .afterRecovery(() -> {
                    회복[1] = 주_수집기.collect(보고("i1", NOW + 1), 회복[0], 노드);

                    long 마지막_보고 = NOW + 앞섬_허용;
                    long 창_끝 = 마지막_보고 + 신선도.toSeconds();
                    CapacityCollector 유령_판 = 수집기();
                    유령_판.collect(보고("유령", 마지막_보고), NOW, 노드);
                    유령[0] = 유령_판.collect(보고("유령", 마지막_보고), 창_끝, 노드);
                    유령[1] = 유령_판.collect(보고("유령", 마지막_보고), 창_끝 + 1, 노드);
                    유령[2] = 수집기().collect(보고("먼_미래", NOW + 앞섬_허용 + 1), NOW, 노드);

                    // 죽은 유령과 갓 뜬 뒷단이 한 판에 같이 실린다.
                    CapacityCollector 겹침 = 수집기();
                    겹침.collect(보고("유령", 마지막_보고), NOW, 노드);
                    겹친_판[0] = 겹침.collect(
                            List.of(하나("유령", 마지막_보고), 하나("갓_뜬", NOW + 1)),
                            NOW + 1, 노드);
                    // 같은 판에서 유령만 뺀다. 차이가 곧 죽은 몫이다.
                    CapacityCollector 유령_없는_판 = 수집기();
                    유령_없는_판.collect(보고("유령", 마지막_보고), NOW, 노드);
                    겹친_판[1] = 유령_없는_판.collect(보고("갓_뜬", NOW + 1), NOW + 1, 노드);
                    // 창을 넘기면 유령의 몫이 빠진다. **갓 뜬 쪽은 계속 보고한다** —
                    // 그 보고까지 낡히면 재는 것이 "유령이 만료됐다" 가 아니라
                    // "전부 낡았다" 가 되고, 그 상태는 프로덕션에 없다.
                    겹친_판[2] = 겹침.collect(
                            List.of(하나("유령", 마지막_보고), 하나("갓_뜬", 창_끝 + 1)),
                            창_끝 + 1, 노드);
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        평시에_관측이_실렸다(정상[0]),
                        바닥값이_걸렸다(승격_직후[0]),
                        // 양수만 보면 중복 기록도 통과한다. 정확히 한 번이어야 한다.
                        보정이_한_번_기록됐다(보정_횟수[0], 보정_횟수[1]),
                        역행_폭이_남았다(역행_폭[0])))
                // **이 구간은 도메인 계약만 잰다.** 배선은 아직 이 값을 안 쓴다 —
                // 계획서 C17 절 참조. 프로덕션이 이 갈래를 걷게 되면 그때 배선까지
                // 덮는 판정으로 올린다.
                .assertDuring(() -> RecoveryCriteria.violations(
                        보정한_시각으로_걷으면_유지된다(정상[0], 유지[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        바닥값을_놓았다(회복[0]),
                        // **RC6 를 밴드로 안 잰다.** 이 시나리오는 보정이 먹히면
                        // 크레딧이 안 떨어져 저하되는 대상이 없다 — 밴드로 재면
                        // X == X 다. 정확 일치가 그 기준을 포함한다.
                        실측으로_정확히_돌아왔다(정상[0], 회복[1]),
                        유령이_창_안에서_세어진다(유령[0]),
                        유령이_창_밖에서_사라진다(유령[1]),
                        허용치를_넘는_앞섬은_안_받는다(유령[2]),
                        유령이_콜드_램프에_겹쳐_실린다(겹친_판[0], 겹친_판[1]),
                        겹침이_창_안에서_끝난다(겹친_판[2])))
                // **RC1~RC5 는 여기서 안 잰다.** 줄도 뒷단 유입도 안 만든다. 수렴(RC6)만
                // 회복 구간에 걸어 둔다.
                .run();
    }

    private CapacityReport 하나(String instanceId, long at) {
        return new CapacityReport(instanceId, 여유, at);
    }

    private List<CapacityReport> 보고(String instanceId, long at) {
        return List.of(하나(instanceId, at));
    }

    private Optional<String> 평시에_관측이_실렸다(long 정상) {
        return 정상 == 여유 ? Optional.empty()
                : Optional.of("전제 — 평시 크레딧이 %d 가 아니라 %d 다".formatted(여유, 정상));
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

    /**
     * 단조 가드가 준 시각으로 걷으면 크레딧이 안 떨어진다 (A-9).
     *
     * <p><b>도메인 계약이지 배선의 증명이 아니다.</b> 가용량 경로는 이 값을 안 쓴다.
     */
    private Optional<String> 보정한_시각으로_걷으면_유지된다(long 정상, long 유지) {
        return 정상 == 유지 ? Optional.empty()
                : Optional.of("보정한 시각으로 걷었는데 크레딧이 %d 에서 %d 로 갈렸다"
                        .formatted(정상, 유지));
    }

    private Optional<String> 바닥값을_놓았다(long 회복) {
        return 회복 == NOW + 1 ? Optional.empty()
                : Optional.of("시계가 따라잡았는데 바닥값을 안 놓았다 — %d".formatted(회복));
    }

    private Optional<String> 실측으로_정확히_돌아왔다(long 정상, long 회복) {
        return 회복 == 정상 ? Optional.empty()
                : Optional.of("회복이 실측이 아니다 — %d 여야 하는데 %d 다".formatted(정상, 회복));
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

    /** 유령의 몫이 콜드 램프값과 같은 판에 실린다. 그 차이가 곧 없는 인스턴스의 몫이다. */
    private Optional<String> 유령이_콜드_램프에_겹쳐_실린다(long 겹친_판, long 유령_없는_판) {
        if (유령_없는_판 != 바닥) {
            return Optional.of(("전제 — 갓 뜬 뒷단만 있는 판이 %d 다. 바닥 %d 이 아니면 "
                    + "콜드 램프가 안 걸린 것이라 겹침이 안 만들어진다")
                    .formatted(유령_없는_판, 바닥));
        }
        return 겹친_판 == 여유 ? Optional.empty()
                : Optional.of("겹친 판이 %d 다 — 죽은 유령의 몫 %d 가 그대로 실려야 한다"
                        .formatted(겹친_판, 여유));
    }

    /**
     * 부풀림이 무기한이면 유령 몫이 램프 내내 실린다. 창 + 허용치에서 끝나야 한다.
     *
     * <p>범위가 아니라 값으로 본다. "줄었다" 만 보면 하한까지 떨어진 판도 통과한다.
     */
    private Optional<String> 겹침이_창_안에서_끝난다(long 창_밖) {
        return 창_밖 == 램프_4초 ? Optional.empty()
                : Optional.of("창을 넘긴 판이 %d 다 — 갓 뜬 뒷단의 램프 몫 %d 여야 한다"
                        .formatted(창_밖, 램프_4초));
    }
}

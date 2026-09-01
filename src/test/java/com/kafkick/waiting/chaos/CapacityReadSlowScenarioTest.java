package com.kafkick.waiting.chaos;

import com.kafkick.waiting.control.CapacityCollector;
import com.kafkick.waiting.control.CapacityRefresh;
import com.kafkick.waiting.control.CapacityReport;
import com.kafkick.waiting.control.CapacitySample;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * C16 — 가용량 읽기만 예산을 넘는다 (8.3.5 · 5절).
 *
 * <p>수요 읽기와 발행은 멀쩡하다. 읽기 예산이 틱의 1/4 이라 이것이 가장 흔한 부분
 * 장애이고, <b>감쇠가 실제로 도는 구간은 여기뿐이다</b> — 레디스가 통째로 죽으면
 * 판 자체가 실패해 감쇠값이 배분에 닿지도 않는다.
 */
@Tag("chaos")
class CapacityReadSlowScenarioTest {

    private static final long NOW = 1_800_000_000L;

    private static final Duration 램프 = Duration.ofSeconds(60);

    private static final Duration 신선도 = Duration.ofSeconds(3);

    /**
     * 설정 하한과 노드 수.
     *
     * <p><b>둘을 일부러 갈라 놓는다.</b> 노드 몫(8 × 2 = 16)이 설정 하한(10)보다
     * 커야, 바닥에서 노드 수를 빼먹은 구현이 판정에 걸린다. 같게 잡으면 두 식이
     * 같은 값을 내어 그 갈래가 안 재진다.
     */
    private static final long 하한 = 10;

    private static final int 노드 = 8;

    /** 바닥. 살아 있는 분모에 맞춘 값이지 설정값이 아니다. */
    private static final long 바닥 = (long) 노드 * CapacityCollector.IDLE_DIVISOR;

    /** 감쇠가 바닥까지 닿고도 남는 판 수. 10,000 에서 절반씩이면 열 판이면 닿는다. */
    private static final int 유지_판 = 100;

    /** 뒷단이 보고하는 여유. 바닥보다 훨씬 커야 감쇠가 내려온 것이 보인다. */
    private static final long 여유 = 10_000;

    /** 가용량 읽기가 예산을 넘는가. 이 스위치로 장애를 넣고 걷는다. */
    private final AtomicBoolean 느리다 = new AtomicBoolean();

    /** 수집기·재료 읽기·지표를 한 묶음으로 든다. 지표를 따로 들면 짝이 어긋난다. */
    private record Rig(CapacityCollector 수집기, CapacityRefresh 읽기, MeterRegistry 지표) {

        void 한_판() {
            읽기.refresh().block();
        }

        void 여러_판(int 수) {
            for (int i = 0; i < 수; i++) {
                한_판();
            }
        }

        long 크레딧() {
            return 수집기.lastKnown();
        }

        /** 배분이 쓰는 값과 게이지가 같은가. 갈리면 회복 판정이 저 혼자 초록이다. */
        double 게이지() {
            return 지표.get("waiting.capacity.credit").gauge().value();
        }

        double 못_읽은_판() {
            return 지표.get("waiting.capacity.read.failed").counter().count();
        }
    }

    private Rig 판을_짠다(long 보고할_여유) {
        CapacityCollector 수집기 = CapacityCollector.of(램프, 신선도, 하한, 100_000);
        MeterRegistry 지표 = new SimpleMeterRegistry();
        CapacityRefresh 읽기 = CapacityRefresh.of(
                () -> 느리다.get()
                        // 예산을 넘긴다. 수요 읽기는 이 경로와 무관하다.
                        ? Mono.<CapacitySample>never()
                        : Mono.just(new CapacitySample(
                                List.of(new CapacityReport("i1", 보고할_여유, NOW)), NOW)),
                수집기, () -> 노드, Duration.ofMillis(50),
                Schedulers.immediate(), 지표);
        return new Rig(수집기, 읽기, 지표);
    }

    /**
     * 세 구간을 한 판정으로 잇는다.
     *
     * <p>여유를 가진 뒷단과 <b>스스로 0 이라고 말한</b> 뒷단을 같은 장애에 함께
     * 태운다. 하나만 태우면 "감쇠가 내려온다" 와 "0 은 안 올린다" 중 한쪽만 재고,
     * 둘은 같은 코드의 반대 갈래다.
     */
    @Test
    @DisplayName("C16_가용량_읽기만_느릴_때_바닥이_받치고_회복에_유예가_다시_찬다")
    void C16_가용량_읽기만_느릴_때_바닥이_받치고_회복에_유예가_다시_찬다() {
        Rig 여유있는 = 판을_짠다(여유);
        Rig 여유없는 = 판을_짠다(0);
        long[] 정상 = new long[2];
        long[] 유예중 = new long[2];
        double[] 못_읽은_판 = new double[2];
        long[] 유지 = new long[2];
        long[] 유예_직후 = new long[1];
        double[] 유지_게이지 = new double[1];
        long[] 회복 = new long[2];

        try {
            ChaosScenario.named("C16 가용량 읽기만 느리다")
                    .baseline(() -> {
                        여유있는.한_판();
                        여유없는.한_판();
                        정상[0] = 여유있는.크레딧();
                        정상[1] = 여유없는.크레딧();
                        못_읽은_판[0] = 여유있는.못_읽은_판();
                    })
                    .inject(() -> {
                        느리다.set(true);
                        // **유예만큼만 돈다.** 진입 판정이 보는 것은 "한 판
                        // 느렸다고 조이지 않는가" 다 — 더 돌면 감쇠가 섞인다.
                        여유있는.여러_판(CapacityCollector.HOLD_ROUNDS);
                        여유없는.여러_판(CapacityCollector.HOLD_ROUNDS);
                        유예중[0] = 여유있는.크레딧();
                        유예중[1] = 여유없는.크레딧();
                        못_읽은_판[1] = 여유있는.못_읽은_판();
                    })
                    .duringFault(() -> {
                        // **유예를 한 판 넘긴 자리를 먼저 본다.** 여기를 안 보면
                        // 유예를 3 에서 13 으로 늘려도 아무 판정이 안 문다 —
                        // 진입은 3판만 보고 유지는 100판 뒤를 보기 때문이다.
                        여유있는.한_판();
                        유예_직후[0] = 여유있는.크레딧();
                        여유있는.여러_판(유지_판);
                        여유없는.여러_판(유지_판);
                        유지[0] = 여유있는.크레딧();
                        유지[1] = 여유없는.크레딧();
                        유지_게이지[0] = 여유있는.게이지();
                    })
                    .recover(() -> 느리다.set(false))
                    .afterRecovery(() -> {
                        여유있는.한_판();
                        회복[0] = 여유있는.크레딧();
                        // 유예가 다시 찼는지는 **다시 실패시켜야** 보인다.
                        느리다.set(true);
                        여유있는.여러_판(CapacityCollector.HOLD_ROUNDS);
                        회복[1] = 여유있는.크레딧();
                    })
                    .assertEntry(() -> RecoveryCriteria.violations(
                            // 전제 — 평시에 관측이 실제로 실렸는가. 안 실렸으면
                            // 뒤의 모든 구간이 하한끼리 비교하는 판이 된다.
                            평시에_관측이_실렸다(정상[0]),
                            // **장애가 정말 들어갔는가.** 안 들어갔으면 유예 판정이
                            // "안 깎였다" 로 자동 통과한다 — 아무것도 안 재는 자리다.
                            읽기가_실제로_실패했다(못_읽은_판[0], 못_읽은_판[1]),
                            // 유예 안에서는 직전 값 그대로다. 한 판 느렸다고 조이면
                            // 순단마다 흔들린다.
                            유예_안에서_안_깎였다(정상[0], 유예중[0]),
                            유예_안에서_안_깎였다(정상[1], 유예중[1])))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            // **유예가 끝나는 자리가 정확한가.** 한 판 더 돌면
                            // 딱 한 번 반토막이어야 한다. 늘어난 유예는 사고 이전
                            // 관측치로 그만큼 더 배분한다는 뜻이다 (불변식 2).
                            유예가_한_판_뒤에_끝났다(정상[0], 유예_직후[0]),
                            // **감쇠가 실제로 돌았는가.** 안 돌면 아래 바닥 판정이
                            // 정상값을 바닥으로 착각해 통과한다.
                            감쇠가_돌았다(정상[0], 유지[0]),
                            // 바닥이 노드를 받친다 (R1). 노드당 몫이 유휴 역수
                            // 아래면 한산한 쿠폰이 전 노드에서 막힌다.
                            바닥이_노드를_받친다(유지[0]),
                            // **뒷단이 스스로 말한 0 은 안 올린다.** 죽었다고 말한
                            // 뒷단에 바닥만큼 다시 밀어넣으면, 서킷이 half-open
                            // 으로 갈 때 시험 트래픽이 아니라 상시 유입이 닿는다.
                            보고한_0은_안_올라간다(유지[1]),
                            // **게이지가 배분값을 따라가는가.** 성공 판에서만
                            // 갱신하면 지표는 장애 직전 값에 얼어 있고 배분은 다른
                            // 값으로 돈다 — RC6 이 "아무 일도 없었다" 로 통과한다.
                            게이지가_배분값을_따라간다(유지[0], 유지_게이지[0])))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            // RC6 — 첫 성공 판에 실측으로 돌아온다.
                            RecoveryCriteria.notConverged("가용량 크레딧", 정상[0], 회복[0]),
                            // 유예가 다시 차서 바로 다음 실패에 안 깎인다.
                            유예가_다시_찼다(회복[0], 회복[1])))
                    .run();
        } finally {
            느리다.set(false);
        }
    }

    /**
     * RC1·RC2·RC4·RC5 는 여기서 안 잰다. 이 시나리오는 제어 평면의 재료 읽기만
     * 흔들고 줄도 뒷단 유입도 만들지 않는다 — 재는 척하면 그 게이트가 이름만 남는다.
     *
     * <p>수집기에서 끝나므로 <b>평활화·하한 재적용·게이팅은 밖이다</b>. 리더십도
     * 안 갈린다. 그 둘이 무엇을 가리는지는 계획서 C16 절에 적어 뒀다.
     */
    private Optional<String> 평시에_관측이_실렸다(long 정상) {
        return 정상 > 바닥 ? Optional.empty()
                : Optional.of("전제 — 평시 크레딧 %d 가 바닥 %d 이하다. 관측이 안 실렸다"
                        .formatted(정상, 바닥));
    }

    private Optional<String> 읽기가_실제로_실패했다(double 전, double 후) {
        long 늘어난_판 = Math.round(후 - 전);
        return 늘어난_판 >= CapacityCollector.HOLD_ROUNDS ? Optional.empty()
                : Optional.of("전제 — 못 읽은 판이 %d 판만 늘었다 (%d 판을 돌렸다)"
                        .formatted(늘어난_판, CapacityCollector.HOLD_ROUNDS));
    }

    private Optional<String> 유예_안에서_안_깎였다(long 정상, long 유예중) {
        return 정상 == 유예중 ? Optional.empty()
                : Optional.of("유예 안에서 깎였다 — %d 에서 %d 로".formatted(정상, 유예중));
    }

    private Optional<String> 유예가_한_판_뒤에_끝났다(long 정상, long 유예_직후) {
        long 기대 = Math.max(Math.max(하한, 바닥), 정상 / 2);
        return 유예_직후 == 기대 ? Optional.empty()
                : Optional.of("유예 %d 판을 한 판 넘겼는데 크레딧이 %d 다 — %d 여야 한다"
                        .formatted(CapacityCollector.HOLD_ROUNDS, 유예_직후, 기대));
    }

    private Optional<String> 감쇠가_돌았다(long 정상, long 유지) {
        return 유지 < 정상 ? Optional.empty()
                : Optional.of("감쇠가 안 돌았다 — %d 판을 못 읽었는데 크레딧이 %d 그대로다"
                        .formatted(유지_판, 유지));
    }

    private Optional<String> 바닥이_노드를_받친다(long 유지) {
        if (유지 != Math.max(하한, 바닥)) {
            return Optional.of("바닥이 %d 가 아니다 — %d 까지 내려갔다"
                    .formatted(Math.max(하한, 바닥), 유지));
        }
        long 노드당 = 유지 / 노드;
        return 노드당 >= CapacityCollector.IDLE_DIVISOR ? Optional.empty()
                : Optional.of("노드당 몫 %d 가 유휴 역수 %d 아래다 (R1)"
                        .formatted(노드당, CapacityCollector.IDLE_DIVISOR));
    }

    private Optional<String> 보고한_0은_안_올라간다(long 유지) {
        return 유지 == 0 ? Optional.empty()
                : Optional.of("뒷단이 여유 0 이라고 말했는데 크레딧이 %d 로 올라갔다"
                        .formatted(유지));
    }

    private Optional<String> 게이지가_배분값을_따라간다(long 배분값, double 게이지) {
        return Math.round(게이지) == 배분값 ? Optional.empty()
                : Optional.of("게이지가 배분값을 안 따라간다 — 배분 %d, 게이지 %.0f"
                        .formatted(배분값, 게이지));
    }

    private Optional<String> 유예가_다시_찼다(long 회복, long 다시_실패한_뒤) {
        return 회복 == 다시_실패한_뒤 ? Optional.empty()
                : Optional.of("유예가 안 찼다 — 회복 %d 인데 %d 판 만에 %d 로 깎였다"
                        .formatted(회복, CapacityCollector.HOLD_ROUNDS, 다시_실패한_뒤));
    }
}

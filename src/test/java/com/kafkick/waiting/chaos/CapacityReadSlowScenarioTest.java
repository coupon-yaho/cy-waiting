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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * C16 — 가용량 읽기만 예산을 넘는다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C16 절이 든다. 여기는
 * 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class CapacityReadSlowScenarioTest {

    private static final long 시작_시각 = 1_800_000_000L;

    /** 회차 하나가 지나는 시간. <b>시계가 멎어 있으면 신선도도 램프도 안 밟힌다.</b> */
    private static final Duration 틱 = Duration.ofSeconds(1);

    private static final Duration 램프 = Duration.ofSeconds(60);

    private static final Duration 신선도 = Duration.ofSeconds(3);

    /**
     * 설정 하한과 노드 수. <b>둘을 일부러 갈라 놓는다.</b>
     *
     * <p>노드 몫(16)이 설정 하한(10)보다 커야 바닥에서 노드 수를 빼먹은 구현이
     * 걸린다. 이 쌍은 기동 설정으로는 안 서고 — 프로퍼티 생성자가 던진다 — 노드
     * 다섯을 기대하고 뜬 클러스터가 여덟 대로 늘어난 회차가 정확히 이 상태다.
     */
    private static final long 하한 = 10;

    private static final int 노드 = 8;

    /** 바닥. 살아 있는 분모에 맞춘 값이지 설정값이 아니다. */
    private static final long 바닥 = Math.max(하한, (long) 노드 * CapacityCollector.IDLE_DIVISOR);

    /** 가용량 읽기의 예산. 배분 틱의 1/4 을 흉내 낸다. */
    private static final Duration 예산 = Duration.ofMillis(50);

    /** 감쇠가 바닥까지 닿고도 남는 회차 수. 10,000 에서 절반씩이면 열 회차면 닿는다. */
    private static final int 유지_회차 = 20;

    /**
     * 램프 창을 넘기는 장애의 길이.
     *
     * <p>수집기는 램프 창만큼 안 보인 인스턴스를 지우는데, 그 청소가 <b>걷는 루프
     * 뒤에</b> 돈다. 그래서 장애가 창보다 길어도 회복 첫 회차는 옛 램프 기록을 그대로
     * 써 실측으로 돌아온다. 청소를 앞으로 옮기면 그 회차가 바닥이 되고 창만큼 재램프한다.
     */
    private static final int 긴_유지_회차 = (int) 램프.dividedBy(틱) + 10;

    /** 뒷단이 보고하는 여유. 바닥보다 훨씬 커야 감쇠가 내려온 것이 보인다. */
    private static final long 여유 = 10_000;

    /** 회차 하나가 실제로 안 끝나면 시험이 멎는다. 가상 시간이라 이 값에 안 닿는다. */
    private static final long 안전_상한_초 = 10;

    /** 수집기·재료 읽기·지표·시계를 한 묶음으로 든다. 따로 들면 짝이 어긋난다. */
    private record Rig(CapacityCollector 수집기, CapacityRefresh 읽기, MeterRegistry 지표,
            AtomicLong 시각, AtomicReference<Duration> 지연, VirtualTimeScheduler 가상시계) {

        /**
         * 한 회차. <b>기다리지 않고 시간을 민다.</b>
         *
         * <p>실제 지연으로 예산을 재면 러너가 밀릴 때 예산 안의 읽기가 밖으로 나간다.
         * 지연도 시한도 같은 가상 시계에 걸어 판정이 기계 속도와 무관하게 선다.
         */
        void 한_회차() {
            CompletableFuture<Void> 끝 = 읽기.refresh().toFuture();
            // 지연과 예산 중 먼저 오는 쪽이 이긴다. 둘을 합친 만큼 밀면 어느 쪽이든 끝난다.
            가상시계.advanceTimeBy(지연.get().plus(예산));
            try {
                끝.get(안전_상한_초, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("회차가 안 끝났다", e);
            }
            시각.addAndGet(틱.toSeconds());
        }

        void 여러_회차(int 수) {
            for (int i = 0; i < 수; i++) {
                한_회차();
            }
        }

        void 느리게(Duration d) {
            지연.set(d);
        }

        long 크레딧() {
            return 수집기.lastKnown();
        }

        /** 배분이 쓰는 값과 게이지가 같은가. 갈리면 회복 판정이 저 혼자 초록이다. */
        double 게이지() {
            return 지표.get("waiting.capacity.credit").gauge().value();
        }

        double 못_읽은_회차() {
            return 지표.get("waiting.capacity.read.failed").counter().count();
        }
    }

    private Rig 회차를_짠다(long 보고할_여유) {
        CapacityCollector 수집기 = CapacityCollector.of(램프, 신선도, 하한, 100_000);
        MeterRegistry 지표 = new SimpleMeterRegistry();
        AtomicLong 시각 = new AtomicLong(시작_시각);
        AtomicReference<Duration> 지연 = new AtomicReference<>(Duration.ZERO);
        VirtualTimeScheduler 가상시계 = VirtualTimeScheduler.create();
        CapacityRefresh 읽기 = CapacityRefresh.of(
                () -> {
                    long 지금 = 시각.get();
                    Mono<CapacitySample> 표본 = Mono.fromSupplier(() -> new CapacitySample(
                            List.of(new CapacityReport("i1", 보고할_여유, 지금)), 지금));
                    Duration d = 지연.get();
                    // **끊는 것이 아니라 늦춘다.** 즉시 오류로 끝내면 이 시나리오가
                    // 재는 것이 C1 과 같아지고, 예산 값이 어떤 판정에도 안 걸린다.
                    return d.isZero() ? 표본 : 표본.delayElement(d, 가상시계);
                },
                수집기, () -> 노드, 예산, 가상시계, 지표);
        return new Rig(수집기, 읽기, 지표, 시각, 지연, 가상시계);
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
        Rig 여유있는 = 회차를_짠다(여유);
        Rig 여유없는 = 회차를_짠다(0);
        Rig 긴장애 = 회차를_짠다(여유);
        Rig 예산_회차 = 회차를_짠다(여유);
        long[] 정상 = new long[2];
        double[] 정상_게이지 = new double[1];
        long[] 예산_경계 = new long[2];
        double[] 예산_실패 = new double[2];
        long[] 유예중 = new long[1];
        double[] 못_읽은_회차 = new double[2];
        long[] 유예_직후 = new long[1];
        long[] 유지 = new long[2];
        double[] 유지_게이지 = new double[1];
        long[] 회복 = new long[2];
        double[] 회복_게이지 = new double[1];
        long[] 긴_회복 = new long[2];

        ChaosScenario.named("C16 가용량 읽기만 느리다")
                .baseline(() -> {
                    여유있는.한_회차();
                    여유없는.한_회차();
                    긴장애.한_회차();
                    정상[0] = 여유있는.크레딧();
                    정상[1] = 여유없는.크레딧();
                    정상_게이지[0] = 여유있는.게이지();
                    못_읽은_회차[0] = 여유있는.못_읽은_회차();
                })
                .inject(() -> {
                    // **예산 경계를 따로 잰다.** 이 회차만 흔들어야 아래 유예 세기가
                    // 안 밀린다. 예산 안의 지연은 성공해야 하고, 밖은 실패해야 한다.
                    예산_실패[0] = 예산_회차.못_읽은_회차();
                    예산_회차.느리게(예산.dividedBy(2));
                    예산_회차.한_회차();
                    예산_경계[0] = 예산_회차.크레딧();
                    예산_회차.느리게(예산.multipliedBy(4));
                    예산_회차.한_회차();
                    예산_경계[1] = 예산_회차.크레딧();
                    예산_실패[1] = 예산_회차.못_읽은_회차();

                    여유있는.느리게(예산.multipliedBy(4));
                    여유없는.느리게(예산.multipliedBy(4));
                    긴장애.느리게(예산.multipliedBy(4));
                    // **유예만큼만 돈다.** 진입 판정이 보는 것은 "한 회차 느렸다고
                    // 조이지 않는가" 다 — 더 돌면 감쇠가 섞인다.
                    여유있는.여러_회차(CapacityCollector.HOLD_ROUNDS);
                    유예중[0] = 여유있는.크레딧();
                    못_읽은_회차[1] = 여유있는.못_읽은_회차();
                })
                .duringFault(() -> {
                    // **유예를 한 회차 넘긴 자리를 먼저 본다.** 여기를 안 보면 유예를
                    // 3 에서 13 으로 늘려도 아무 판정이 안 문다 — 진입은 3회차만 보고
                    // 유지는 그 한참 뒤를 보기 때문이다.
                    여유있는.한_회차();
                    유예_직후[0] = 여유있는.크레딧();
                    여유있는.여러_회차(유지_회차);
                    여유없는.여러_회차(CapacityCollector.HOLD_ROUNDS + 유지_회차);
                    긴장애.여러_회차(긴_유지_회차);
                    유지[0] = 여유있는.크레딧();
                    유지[1] = 여유없는.크레딧();
                    유지_게이지[0] = 여유있는.게이지();
                })
                .recover(() -> {
                    여유있는.느리게(Duration.ZERO);
                    여유없는.느리게(Duration.ZERO);
                    긴장애.느리게(Duration.ZERO);
                    예산_회차.느리게(Duration.ZERO);
                })
                .afterRecovery(() -> {
                    여유있는.한_회차();
                    회복[0] = 여유있는.크레딧();
                    회복_게이지[0] = 여유있는.게이지();
                    긴장애.한_회차();
                    긴_회복[0] = 긴장애.크레딧();
                    긴장애.한_회차();
                    긴_회복[1] = 긴장애.크레딧();
                    // 유예가 다시 찼는지는 **다시 실패시켜야** 보인다.
                    여유있는.느리게(예산.multipliedBy(4));
                    여유있는.여러_회차(CapacityCollector.HOLD_ROUNDS);
                    회복[1] = 여유있는.크레딧();
                    여유있는.느리게(Duration.ZERO);
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 전제 — 평시에 관측이 실제로 실렸는가. 안 실렸으면 뒤의
                        // 모든 구간이 하한끼리 비교하는 회차가 된다.
                        평시에_관측이_실렸다(정상[0]),
                        // 전제 — 여유 0 도 평시에 0 으로 실렸는가. 이걸 안 걸면
                        // 유지 구간의 0 판정이 다른 결함을 제 것으로 보고한다.
                        평시에_0이_실렸다(정상[1]),
                        // 성공 회차에서 게이지를 안 맞추면 여기가 문다.
                        게이지가_배분값을_따라간다("정상", 정상[0], 정상_게이지[0]),
                        // **예산이 어디서 끊는가.** 안 재면 예산 값을 60배로 틀리게
                        // 줘도 통과한다 — 제어 틱이 회차마다 멎는 설정이 그냥 나간다.
                        예산_안에서는_성공한다(예산_경계[0]),
                        예산_밖에서는_실패한다(예산_경계[1], 예산_실패[0], 예산_실패[1]),
                        // **장애가 정말 들어갔는가.** 안 들어갔으면 유예 판정이
                        // "안 깎였다" 로 자동 통과한다.
                        읽기가_실제로_실패했다(못_읽은_회차[0], 못_읽은_회차[1]),
                        // 유예 안에서는 직전 값 그대로다. 한 회차 느렸다고 조이면
                        // 순단마다 흔들린다.
                        유예_안에서_안_깎였다(정상[0], 유예중[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // **유예가 끝나는 자리가 정확한가.** 한 회차 더 돌면 딱 한 번
                        // 반토막이어야 한다. 늘어난 유예는 사고 이전 관측치로 그만큼
                        // 더 배분한다는 뜻이다 (불변식 2).
                        유예가_한_회차_뒤에_끝났다(정상[0], 유예_직후[0]),
                        // 바닥이 노드를 받친다 (R1). 노드당 몫이 유휴 역수 아래면
                        // 한산한 쿠폰이 전 노드에서 막힌다.
                        바닥이_노드를_받친다(유지[0]),
                        // 감쇠의 0 갈래를 지우면 여기가 문다. 왜 안 올리는지는
                        // observationFailed 의 주석이 든다.
                        보고한_0은_안_올라간다(유지[1]),
                        // 실패 회차에서 게이지를 안 맞추면 여기가 문다.
                        게이지가_배분값을_따라간다("유지", 유지[0], 유지_게이지[0])))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        // **정확히 돌아온다.** 인메모리라 결정적이다 — 밴드로만 보면
                        // 10% 어긋난 회복이 통과한다.
                        실측으로_정확히_돌아왔다(정상[0], 회복[0]),
                        // RC6 은 게이트가 전 시나리오를 같은 자로 재는 자리다.
                        RecoveryCriteria.notConverged("가용량 크레딧", 정상[0], 회복[0]),
                        게이지가_배분값을_따라간다("회복", 회복[0], 회복_게이지[0]),
                        // 유예가 다시 차서 바로 다음 실패에 안 깎인다.
                        유예가_다시_찼다(회복[0], 회복[1]),
                        // **램프 창을 넘긴 장애도 첫 회차에 실측이다.** 청소가 걷는
                        // 루프 뒤에 돌아 옛 램프 기록이 남기 때문이다. 앞으로 옮기면
                        // 여기가 바닥이 되고 창만큼 재램프한다 — 그 회복이 훨씬 느리다.
                        램프_창을_넘겨도_실측이다(긴_회복[0], 긴_회복[1])))
                // **RC1~RC6 은 여기서 안 잰다.** 이 시나리오는 제어 평면의 재료
                // 읽기만 흔들고 줄도 뒷단 유입도 만들지 않는다 — 재는 척하면 그
                // 게이트가 이름만 남는다. 수렴(RC6)만 회복 구간에 걸어 둔다.
                //
                // 수집기에서 끝나므로 평활화·하한 재적용·게이팅도 밖이고, 리더십도
                // 안 갈린다. 그 둘이 무엇을 가리는지는 계획서 C16 절에 있다.
                .run();
    }

    private Optional<String> 평시에_관측이_실렸다(long 정상) {
        return 정상 == 여유 ? Optional.empty()
                : Optional.of("전제 — 평시 크레딧이 %d 가 아니라 %d 다. 관측이 안 실렸다"
                        .formatted(여유, 정상));
    }

    private Optional<String> 평시에_0이_실렸다(long 정상) {
        return 정상 == 0 ? Optional.empty()
                : Optional.of("전제 — 여유 0 을 보고했는데 평시 크레딧이 %d 다".formatted(정상));
    }

    private Optional<String> 예산_안에서는_성공한다(long 크레딧) {
        return 크레딧 == 여유 ? Optional.empty()
                : Optional.of("예산 %s 의 절반만 늦췄는데 못 읽었다 — 크레딧 %d"
                        .formatted(예산, 크레딧));
    }

    private Optional<String> 예산_밖에서는_실패한다(long 크레딧, double 전, double 후) {
        if (Math.round(후 - 전) != 1) {
            return Optional.of("예산 밖 지연에서 못 읽은 회차가 %d 건 늘었다 — 1 이어야 한다"
                    .formatted(Math.round(후 - 전)));
        }
        return 크레딧 == 여유 ? Optional.empty()
                : Optional.of("예산 밖인데 크레딧이 %d 로 바뀌었다 — 유예 안이라 그대로여야 한다"
                        .formatted(크레딧));
    }

    private Optional<String> 읽기가_실제로_실패했다(double 전, double 후) {
        long 늘어난_회차 = Math.round(후 - 전);
        return 늘어난_회차 >= CapacityCollector.HOLD_ROUNDS ? Optional.empty()
                : Optional.of("전제 — 못 읽은 회차가 %d 회차만 늘었다 (%d 회차를 돌렸다)"
                        .formatted(늘어난_회차, CapacityCollector.HOLD_ROUNDS));
    }

    private Optional<String> 유예_안에서_안_깎였다(long 정상, long 유예중) {
        return 정상 == 유예중 ? Optional.empty()
                : Optional.of("유예 안에서 깎였다 — %d 에서 %d 로".formatted(정상, 유예중));
    }

    private Optional<String> 유예가_한_회차_뒤에_끝났다(long 정상, long 유예_직후) {
        long 기대 = Math.max(바닥, 정상 / 2);
        return 유예_직후 == 기대 ? Optional.empty()
                : Optional.of("유예 %d 회차를 한 회차 넘겼는데 크레딧이 %d 다 — %d 여야 한다"
                        .formatted(CapacityCollector.HOLD_ROUNDS, 유예_직후, 기대));
    }

    private Optional<String> 바닥이_노드를_받친다(long 유지) {
        if (유지 != 바닥) {
            return Optional.of("바닥이 %d 가 아니다 — %d 까지 내려갔다".formatted(바닥, 유지));
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

    private Optional<String> 게이지가_배분값을_따라간다(String 구간, long 배분값, double 게이지) {
        return Math.round(게이지) == 배분값 ? Optional.empty()
                : Optional.of("%s 구간 게이지가 배분값을 안 따라간다 — 배분 %d, 게이지 %.0f"
                        .formatted(구간, 배분값, 게이지));
    }

    private Optional<String> 실측으로_정확히_돌아왔다(long 정상, long 회복) {
        return 회복 == 정상 ? Optional.empty()
                : Optional.of("첫 성공 회차가 실측이 아니다 — %d 여야 하는데 %d 다"
                        .formatted(정상, 회복));
    }

    private Optional<String> 유예가_다시_찼다(long 회복, long 다시_실패한_뒤) {
        return 회복 == 다시_실패한_뒤 ? Optional.empty()
                : Optional.of("유예가 안 찼다 — 회복 %d 인데 %d 회차 만에 %d 로 깎였다"
                        .formatted(회복, CapacityCollector.HOLD_ROUNDS, 다시_실패한_뒤));
    }

    private Optional<String> 램프_창을_넘겨도_실측이다(long 첫_회차, long 둘째_회차) {
        if (첫_회차 != 여유) {
            return Optional.of("램프 창(%s)을 넘긴 장애의 첫 회복 회차가 %d 다 — %d 여야 한다"
                    .formatted(램프, 첫_회차, 여유));
        }
        return 둘째_회차 == 여유 ? Optional.empty()
                : Optional.of("둘째 회차가 %d 로 떨어졌다 — 재램프가 돌고 있다".formatted(둘째_회차));
    }
}

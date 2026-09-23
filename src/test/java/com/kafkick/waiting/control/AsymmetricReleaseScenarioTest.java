package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.chaos.ChaosScenario;
import com.kafkick.waiting.chaos.RecoveryCriteria;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.CircuitState;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * C6e — 레디스 상한이 전원에 걸렸다가 비대칭으로 풀린다. 적게 센 관측이 분모를 무너뜨리는가.
 *
 * <p>등록부 안에서는 감소 지연이 유일한 방어선이다. 흡수판은 그 폭 안에서 버티는 것을, 붕괴판은 폭을
 * 넘으면 무너지는 틱을, 대조판은 지연을 1 로 두면 반드시 무너지는 것을 못 박는다. 정리 규칙과 발행 전달은
 * 시험이 모형으로 다시 쓴 것이라 그쪽의 방어선은 이 시험이 못 본다.
 */
class AsymmetricReleaseScenarioTest {

    /** 같은 틱 안에서 발행자가 다른 노드보다 먼저 치는가. */
    enum Order { 리더_먼저, 리더_나중 }

    /** 설계가 정한 감소 지연. 제품 상수에서 읽으면 그 상수를 바꾼 뮤턴트가 일정도 같이 줄인다. */
    private static final int 설계_감소_지연 = 3;

    /** 1·2·3 으로 나누어떨어져 나머지가 판정을 흐리지 않는다. */
    private static final long 예산 = 12_000;

    private static final long 개방_합_상한 = 예산 / 2;

    private static final int 회복_틱 = 5;

    private static final String 발행자 = "A";

    private static final List<String> 동료 = List.of("B", "C");

    private static final ControlPlaneProperties 설정 = ControlPlaneProperties.defaults();

    /** 정리 임계는 판정 기준이 아니라 모형의 입력이라 제품 값을 읽는다. */
    private static final long 정리_틱 =
            설정.capacity().freshness().toMillis() / 설정.scheduler().tick().toMillis();

    /** 상한 계산만 쓴다. 리미터 상태는 안 건드린다. */
    private static final AdmissionDecider 판정 =
            AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(10), 0.7);

    @ParameterizedTest
    @EnumSource(Order.class)
    @DisplayName("C6e_한_틱_늦게_풀린_노드는_감소_지연이_흡수한다")
    void C6e_한_틱_늦게_풀린_노드는_감소_지연이_흡수한다(Order 순서) {
        흡수판(제품_분모(), 순서).run();
    }

    /** 등록부나 상한 계산에 두 번째 방어선이 생기면 여기가 빨개진다. 그때 계획서 C6e 를 고친다. */
    @ParameterizedTest
    @EnumSource(Order.class)
    @DisplayName("C6e_감소_지연이_1_이면_같은_일정에서_무너진다")
    void C6e_감소_지연이_1_이면_같은_일정에서_무너진다(Order 순서) {
        assertThatThrownBy(() -> 흡수판(GatewayRegistry.of(1, 1), 순서).run())
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("개방 상한 합");
    }

    /** 돌아온 틱의 배율은 순서에 달렸다. 리더가 먼저 치면 돌아온 노드를 못 세고 무너진 분모를 넘긴다. */
    @ParameterizedTest
    @EnumSource(Order.class)
    @DisplayName("C6e_감소_지연보다_늦게_풀린_노드는_무너진_분모를_받는다")
    void C6e_감소_지연보다_늦게_풀린_노드는_무너진_분모를_받는다(Order 순서) {
        Cluster 판 = new Cluster(제품_분모(), 순서);
        int[] 해제 = new int[1];
        double 돌아온_배율 = 순서 == Order.리더_먼저 ? 1.5 : 1.0;

        시나리오("C6e 붕괴 " + 순서, 판, 설계_감소_지연 + 1, 해제)
                .assertDuring(() -> RecoveryCriteria.violations(
                        첫_관측이_전제대로다(판, 해제[0], 순서),
                        같다("첫 위반 틱", 첫_위반(판), 해제[0] + 설계_감소_지연 - 1)))
                .assertRecovery(() -> {
                    List<Tick> 회복 = 판.해제부터(판.c_해제);
                    return RecoveryCriteria.violations(
                            회복.get(0).배율() == 돌아온_배율 ? Optional.empty()
                                    : Optional.of(("돌아온 틱의 배율이 %.2f — %.1f 였다. 바뀌었으면 방어선이 "
                                            + "하나 더 생긴 것이니 C6e 의 문장을 고친다")
                                            .formatted(회복.get(0).배율(), 돌아온_배율)),
                            배율을_지킨다(회복.subList(1, 회복.size()), 1.0),
                            모두_셋을_든다(회복.get(1)));
                })
                .run();
    }

    private ChaosScenario 흡수판(GatewayRegistry 분모, Order 순서) {
        Cluster 판 = new Cluster(분모, 순서);
        int[] 해제 = new int[1];
        return 시나리오("C6e 흡수 " + 순서, 판, 1, 해제)
                .assertDuring(() -> RecoveryCriteria.violations(
                        첫_관측이_전제대로다(판, 해제[0], 순서),
                        개방_합을_지킨다(판.해제부터(해제[0]).subList(0, 1)),
                        분모가_셋이다(판.해제부터(해제[0]).subList(0, 1))))
                .assertRecovery(() -> {
                    List<Tick> 회복 = 판.해제부터(판.c_해제);
                    return RecoveryCriteria.violations(
                            배율을_지킨다(회복, 1.0),
                            개방_합을_지킨다(회복),
                            모두_셋을_든다(회복.get(1)));
                });
    }

    /** 전원을 정리 임계 너머로 막고, 발행자와 B 를 먼저 푼 뒤 C 를 {@code c_지연} 틱 늦게 푼다. */
    private ChaosScenario 시나리오(String 이름, Cluster 판, int c_지연, int[] 해제) {
        return ChaosScenario.named(이름)
                .baseline(판::한_틱)
                .inject(() -> {
                    판.막힘.addAll(List.of(발행자, "B", "C"));
                    IntStream.rangeClosed(0, (int) 정리_틱).forEach(i -> 판.한_틱());
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        판.기록.stream().skip(1).allMatch(t -> t.관측() < 0 && t.분모() == 3)
                                ? Optional.empty()
                                : Optional.of("막힌 동안 분모를 지키지 않았다 — " + 판.기록)))
                .duringFault(() -> {
                    판.막힘.removeAll(List.of(발행자, "B"));
                    해제[0] = 판.지금 + 1;
                    IntStream.range(0, c_지연).forEach(i -> 판.한_틱());
                })
                .recover(() -> {
                    판.막힘.remove("C");
                    판.c_해제 = 판.지금 + 1;
                })
                .afterRecovery(() -> IntStream.range(0, 회복_틱).forEach(i -> 판.한_틱()));
    }

    private GatewayRegistry 제품_분모() {
        return new GatewayPresenceConfig().gatewayRegistry(설정);
    }

    /** 틱 하나의 기록. 관측이 음수면 발행자가 하트비트를 놓쳤다. */
    record Tick(int 번호, int 관측, int 분모, long 개방_합, double 배율, Map<String, Integer> 든_분모) {
    }

    /** 틱 하나 = 하트비트 한 번 = 배분 한 회차. 막힌 노드는 치지도 받지도 못한다. */
    private static final class Cluster {

        private final GatewayRegistry 분모;

        private final Runnable 놓침;

        private final Order 순서;

        private final Set<String> 막힘 = new HashSet<>();

        private final Map<String, Integer> 마지막 = new LinkedHashMap<>();

        private final Map<String, SnapshotMeta> 든_것 = new LinkedHashMap<>();

        private final List<Tick> 기록 = new ArrayList<>();

        private int 지금;

        private int c_해제 = -1;

        Cluster(GatewayRegistry 분모, Order 순서) {
            this.분모 = 분모;
            this.놓침 = new GatewayPresenceConfig().missStep(() -> CircuitState.CLOSED, 분모);
            this.순서 = 순서;
            동료.forEach(id -> 마지막.put(id, 0));
        }

        void 한_틱() {
            지금++;
            if (순서 == Order.리더_나중) {
                친다();
            }
            int 관측 = -1;
            SnapshotMeta 발행 = null;
            if (막힘.contains(발행자)) {
                놓침.run();
            } else {
                관측 = 1 + (int) 동료.stream().filter(id -> 지금 - 마지막.get(id) <= 정리_틱).count();
                분모.observed(관측);
                발행 = new SnapshotMeta(예산, 분모.count());
                든_것.put(발행자, 발행);
            }
            if (순서 == Order.리더_먼저) {
                친다();
            }
            for (String id : 동료) {
                if (발행 != null && !막힘.contains(id)) {
                    든_것.put(id, 발행);
                }
            }
            // 상한 합은 풀린 노드까지 더한다. 보수적인 쪽이라 초록은 의미가 있다.
            기록.add(new Tick(지금, 관측, 분모.count(),
                    든_것.values().stream().mapToLong(판정::failOpenCap).sum(),
                    든_것.values().stream().mapToLong(판정::globalCap).sum() / (double) 예산,
                    든_분모()));
        }

        private void 친다() {
            동료.stream().filter(id -> !막힘.contains(id)).forEach(id -> 마지막.put(id, 지금));
        }

        private Map<String, Integer> 든_분모() {
            Map<String, Integer> 분모들 = new LinkedHashMap<>();
            든_것.forEach((id, meta) -> 분모들.put(id, meta.gatewayCount()));
            return 분모들;
        }

        List<Tick> 해제부터(int 번호) {
            return 기록.stream().filter(t -> t.번호() >= 번호).toList();
        }
    }

    private static int 첫_위반(Cluster 판) {
        return 판.기록.stream().filter(t -> t.개방_합() > 개방_합_상한)
                .mapToInt(Tick::번호).findFirst().orElse(-1);
    }

    /** 전원이 정리됐다는 전제. 이것이 안 서면 이 시나리오는 아무것도 안 잰다. */
    private static Optional<String> 첫_관측이_전제대로다(Cluster 판, int 해제, Order 순서) {
        // 리더가 나중에 치면 같은 틱에 풀린 B 가 먼저 세어진다. 그 판은 지연 1 만 가른다.
        int 기대 = 순서 == Order.리더_먼저 ? 1 : 2;
        return 같다("해제 첫 틱의 관측", 판.해제부터(해제).get(0).관측(), 기대)
                .map(사유 -> "전제 — " + 사유);
    }

    private static Optional<String> 개방_합을_지킨다(List<Tick> 구간) {
        return 구간.stream().filter(t -> t.개방_합() > 개방_합_상한).findFirst()
                .map(t -> "%d 틱의 개방 상한 합 %d — %d 이하여야 한다. 든 분모 %s"
                        .formatted(t.번호(), t.개방_합(), 개방_합_상한, t.든_분모()));
    }

    private static Optional<String> 분모가_셋이다(List<Tick> 구간) {
        return 구간.stream().filter(t -> t.분모() != 3).findFirst()
                .map(t -> "%d 틱의 분모 %d — 3 을 지켜야 한다".formatted(t.번호(), t.분모()));
    }

    private static Optional<String> 배율을_지킨다(List<Tick> 구간, double 상한) {
        return 구간.stream().filter(t -> t.배율() > 상한).findFirst()
                .map(t -> "%d 틱의 유입 배율 %.2f — %.1f 이하여야 한다. 든 분모 %s"
                        .formatted(t.번호(), t.배율(), 상한, t.든_분모()));
    }

    private static Optional<String> 모두_셋을_든다(Tick t) {
        return t.든_분모().values().stream().allMatch(n -> n == 3) ? Optional.empty()
                : Optional.of("%d 틱에 든 분모 %s — 전원 3 이어야 한다".formatted(t.번호(), t.든_분모()));
    }

    private static Optional<String> 같다(String 이름, long 실제, long 기대) {
        return 실제 == 기대 ? Optional.empty()
                : Optional.of("%s 가 %d — %d 여야 한다".formatted(이름, 실제, 기대));
    }
}

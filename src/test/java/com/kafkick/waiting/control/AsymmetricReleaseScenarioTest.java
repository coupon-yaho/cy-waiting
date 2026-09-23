package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.chaos.ChaosScenario;
import com.kafkick.waiting.chaos.RecoveryCriteria;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.CircuitState;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.MutableClock;
import com.kafkick.waiting.domain.allocation.ReleaseRamp;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * C6e — 레디스 상한이 전원에 걸렸다가 비대칭으로 풀린다. 적게 센 관측이 분모를 무너뜨리는가.
 *
 * <p>등록부 안에서는 감소 지연이 유일한 방어선이다. 흡수판은 그 폭 안에서 버티는 것을, 붕괴판은 폭을
 * 넘으면 무너지는 틱을, 대조판은 지연을 1 로 두면 반드시 무너지는 것을 못 박는다. 정리 규칙과 발행 전달, 받는
 * 경로는 시험이 모형으로 다시 쓴 것이라 그쪽의 방어선은 이 시험이 못 본다. 받는 경로는 갱신 루프 시험이 잰다.
 */
class AsymmetricReleaseScenarioTest {

    /** 같은 틱 안에서 발행자가 다른 노드보다 먼저 치는가. */
    enum Order { 리더_먼저, 리더_나중 }

    /** 설계가 정한 감소 지연. 제품 상수에서 읽으면 그 상수를 바꾼 뮤턴트가 일정도 같이 줄인다. */
    private static final int 설계_감소_지연 = 3;

    /** 1·2·3 으로 나누어떨어져 나머지가 판정을 흐리지 않는다. */
    private static final long 예산 = 12_000;

    /** 재료가 낡았다고 판정되는 나이(초). 옮겨 적은 설계값이다 — 이보다 오래 막히면 승계가 몫을 깎는다. */
    private static final int 설계_낡음_초 = 5;

    private static final long 개방_합_상한 = 예산 / 2;

    private static final int 회복_틱 = 5;

    private static final String 발행자 = "A";

    private static final List<String> 노드 = List.of(발행자, "B", "C");

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
        흡수판(this::제품_분모, 순서).run();
    }

    /** 등록부나 상한 계산에 두 번째 방어선이 생기면 여기가 빨개진다. 그때 계획서 C6e 를 고친다. */
    @ParameterizedTest
    @EnumSource(Order.class)
    @DisplayName("C6e_감소_지연이_1_이면_같은_일정에서_무너진다")
    void C6e_감소_지연이_1_이면_같은_일정에서_무너진다(Order 순서) {
        assertThatThrownBy(() -> 흡수판(() -> GatewayRegistry.of(1, 1), 순서).run())
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("개방 상한 합");
    }

    /**
     * 돌아온 틱의 유입은 순서에 달렸다. 리더가 먼저 치면 돌아온 노드를 못 센 분모를 발행하지만, 돌아온 노드는
     * 제 관측으로 분모를 올려 든다. 남는 것은 합류와 같은 한 틱의 겹침이다. 리더가 나중에 치면 발행값이 이미
     * 전원이라 바닥이 할 일이 없고, 그 판은 회귀 대조다.
     */
    @ParameterizedTest
    @EnumSource(Order.class)
    @DisplayName("C6e_감소_지연보다_늦게_풀린_노드는_제_관측으로_분모를_든다")
    void C6e_감소_지연보다_늦게_풀린_노드는_제_관측으로_분모를_든다(Order 순서) {
        붕괴판(List.of(발행자, "B"), List.of("C"), 순서, 순서 == Order.리더_먼저 ? 16_000 : 예산);
    }

    /**
     * 발행자 홀로 먼저 풀리면 분모가 1 까지 무너진다. 돌아온 둘이 그 1 을 그대로 들면 유입이 세 배다. 리더가 먼저
     * 치면 B 는 C 가 치기 전에 세어 2 를 보고, C 는 3 을 본다.
     */
    @ParameterizedTest
    @EnumSource(Order.class)
    @DisplayName("C6e_발행자_홀로_먼저_풀려도_돌아온_노드는_무너진_분모를_안_든다")
    void C6e_발행자_홀로_먼저_풀려도_돌아온_노드는_무너진_분모를_안_든다(Order 순서) {
        붕괴판(List.of(발행자), List.of("B", "C"), 순서, 순서 == Order.리더_먼저 ? 22_000 : 예산);
    }

    /**
     * 리스보다 오래 막혀 리더십을 잃는 것은 같지만, 재료가 낡을 만큼 막히면 되찾은 리더가 몫을 한산 최소 몫까지
     * 깎고 네 배씩 오른다. 그 사이 무너진 분모의 배율은 절대 유입으로 흡수된다.
     */
    @ParameterizedTest
    @EnumSource(Order.class)
    @DisplayName("C6e_낡을_만큼_막히면_되찾은_리더의_램프가_무너진_분모를_흡수한다")
    void C6e_낡을_만큼_막히면_되찾은_리더의_램프가_무너진_분모를_흡수한다(Order 순서) {
        Cluster 판 = new Cluster(this::제품_분모, 순서);
        int[] 해제 = new int[1];

        시나리오("C6e 낡은 승계 " + 순서, 판, 설계_낡음_초, List.of(발행자, "B"), List.of("C"),
                설계_감소_지연 + 1, 해제)
                .assertDuring(() -> RecoveryCriteria.violations(
                        같다("되찾은 첫 발행 몫", 판.해제부터(해제[0]).get(0).크레딧(), 24),
                        같다("첫 위반 틱", 첫_위반(판), -1)))
                .assertRecovery(() -> {
                    List<Tick> 회복 = 판.해제부터(판.c_해제);
                    return RecoveryCriteria.violations(
                            같다("돌아온 틱의 유입 합", 회복.get(0).유입(), 순서 == Order.리더_먼저 ? 8_192 : 6_144)
                                    .map(사유 -> 사유 + ". 크레딧 " + 회복.get(0).크레딧() + " 든 분모 "
                                            + 회복.get(0).든_분모()),
                            배율을_지킨다(회복, 1.0));
                })
                .run();
    }

    private void 붕괴판(List<String> 먼저, List<String> 늦게, Order 순서, long 돌아온_유입) {
        Cluster 판 = new Cluster(this::제품_분모, 순서);
        int[] 해제 = new int[1];

        시나리오("C6e 붕괴 " + 먼저 + " " + 순서, 판, 먼저, 늦게, 설계_감소_지연 + 1, 해제)
                .assertDuring(() -> RecoveryCriteria.violations(
                        첫_관측이_전제대로다(판, 해제[0], 순서 == Order.리더_먼저 ? 1 : 먼저.size()),
                        같다("첫 위반 틱", 첫_위반(판), 해제[0] + 설계_감소_지연 - 1)))
                .assertRecovery(() -> {
                    List<Tick> 회복 = 판.해제부터(판.c_해제);
                    return RecoveryCriteria.violations(
                            같다("돌아온 틱의 유입 합", 회복.get(0).유입(), 돌아온_유입)
                                    .map(사유 -> 사유 + ". 든 분모 " + 회복.get(0).든_분모()),
                            배율을_지킨다(회복.subList(1, 회복.size()), 1.0),
                            모두_셋을_든다(회복.get(1)));
                })
                .run();
    }

    private ChaosScenario 흡수판(Supplier<GatewayRegistry> 등록부, Order 순서) {
        Cluster 판 = new Cluster(등록부, 순서);
        int[] 해제 = new int[1];
        return 시나리오("C6e 흡수 " + 순서, 판, List.of(발행자, "B"), List.of("C"), 1, 해제)
                .assertDuring(() -> RecoveryCriteria.violations(
                        첫_관측이_전제대로다(판, 해제[0], 순서 == Order.리더_먼저 ? 1 : 2),
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

    /** 전원을 정리 임계 너머로 막고, {@code 먼저} 를 푼 뒤 {@code 늦게} 를 {@code c_지연} 틱 늦게 푼다. */
    private ChaosScenario 시나리오(String 이름, Cluster 판, List<String> 먼저, List<String> 늦게,
            int c_지연, int[] 해제) {
        return 시나리오(이름, 판, (int) 정리_틱 + 1, 먼저, 늦게, c_지연, 해제);
    }

    /** 막는 길이를 따로 준다. 재료가 낡았다고 판정되는 길이를 넘기면 승계가 몫을 깎고 램프로 오른다. */
    private ChaosScenario 시나리오(String 이름, Cluster 판, int 막는_틱, List<String> 먼저,
            List<String> 늦게, int c_지연, int[] 해제) {
        return ChaosScenario.named(이름)
                .baseline(판::한_틱)
                .inject(() -> {
                    판.막힘.addAll(노드);
                    IntStream.range(0, 막는_틱).forEach(i -> 판.한_틱());
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        판.기록.stream().skip(1).allMatch(t -> t.관측() < 0 && t.분모() == 3)
                                ? Optional.empty()
                                : Optional.of("막힌 동안 분모를 지키지 않았다 — " + 판.기록),
                        // 전제 — 리스보다 오래 막혔으니 리더십을 잃는다. 안 잃으면 승계를 안 잰다.
                        판.리더 ? Optional.of("전제 — 막힌 동안 리더십을 안 잃었다") : Optional.empty()))
                .duringFault(() -> {
                    판.막힘.removeAll(먼저);
                    해제[0] = 판.지금 + 1;
                    IntStream.range(0, c_지연).forEach(i -> 판.한_틱());
                })
                .recover(() -> {
                    판.막힘.removeAll(늦게);
                    판.c_해제 = 판.지금 + 1;
                })
                .afterRecovery(() -> IntStream.range(0, 회복_틱).forEach(i -> 판.한_틱()));
    }

    private GatewayRegistry 제품_분모() {
        return new GatewayPresenceConfig().gatewayRegistry(설정);
    }

    /** 틱 하나의 기록. 관측이 음수면 발행자가 하트비트를 놓쳤다. 유입은 노드들이 든 초당 예산의 합이다. */
    record Tick(int 번호, int 관측, int 분모, long 크레딧, long 개방_합, long 유입,
            Map<String, Integer> 든_분모) {

        double 배율() {
            return 유입 / (double) 예산;
        }
    }

    /**
     * 틱 하나 = 하트비트 한 번 = 배분 한 회차. 노드마다 제 등록부가 있고, 스냅샷은 제 관측 아래로 안 든다.
     * 막힌 노드는 치지도 받지도 못한다.
     */
    private static final class Cluster {

        private final Map<String, GatewayRegistry> 등록부 = new LinkedHashMap<>();

        private final Map<String, Runnable> 놓침 = new LinkedHashMap<>();

        private final Order 순서;

        private final Set<String> 막힘 = new HashSet<>();

        private final Map<String, Integer> 마지막 = new LinkedHashMap<>();

        private final Map<String, SnapshotMeta> 든_것 = new LinkedHashMap<>();

        private final List<Tick> 기록 = new ArrayList<>();

        private int 지금;

        private int c_해제 = -1;

        /** 리스가 틱 몇 개를 버티는가. 발행자가 이보다 오래 막히면 리더십을 잃는다. */
        private final long 리스_틱 = 설정.leader().lease().toMillis() / 설정.scheduler().tick().toMillis();

        private final MutableClock 시계 = MutableClock.at(Instant.ofEpochSecond(1_700_000_000L));

        /** 발행자가 든 재료. 되찾을 때 이 나이로 출발점을 정한다. 임계는 제품 배선의 것이다. */
        private final SnapshotHolder 발행자_홀더 =
                new HealthConfig().snapshotHolder(시계, new SimpleMeterRegistry());

        private final ReleaseRamp 램프 = ReleaseRamp.of(ReleaseRamp.DEFAULT_STEP);

        private boolean 리더 = true;

        private int 막힌_연속;

        Cluster(Supplier<GatewayRegistry> 새_등록부, Order 순서) {
            this.순서 = 순서;
            for (String id : 노드) {
                GatewayRegistry 그_노드 = 새_등록부.get();
                등록부.put(id, 그_노드);
                놓침.put(id, new GatewayPresenceConfig().missStep(() -> CircuitState.CLOSED, 그_노드));
                마지막.put(id, 0);
            }
        }

        void 한_틱() {
            지금++;
            시계.앞으로(Duration.ofSeconds(1));
            막힌_연속 = 막힘.contains(발행자) ? 막힌_연속 + 1 : 0;
            if (막힌_연속 >= 리스_틱) {
                리더 = false;
            }
            List<String> 차례 = 순서 == Order.리더_먼저 ? 노드 : List.of("B", "C", 발행자);
            int 관측 = -1;
            SnapshotMeta 발행 = null;
            for (String id : 차례) {
                if (막힘.contains(id)) {
                    놓침.get(id).run();
                    continue;
                }
                // 스크립트는 자기를 먼저 쓰고 남을 센다. 같은 틱에 먼저 친 노드는 세어진다.
                int 본_수 = 1 + (int) 노드.stream().filter(other -> !other.equals(id))
                        .filter(other -> 지금 - 마지막.get(other) <= 정리_틱).count();
                마지막.put(id, 지금);
                등록부.get(id).observed(본_수);
                if (id.equals(발행자)) {
                    관측 = 본_수;
                    발행 = 발행한다();
                }
            }
            // 제품은 하트비트와 갱신이 따로 돈다. 순서가 뒤집혀도 막힌 동안 놓침이 등록부를 지켜 바닥은 같다.
            for (String id : 노드) {
                if (발행 != null && !막힘.contains(id)) {
                    든_것.put(id, 발행.withGatewayCountAtLeast(등록부.get(id).seenNow()));
                }
            }
            // 상한 합은 풀린 노드까지 더한다. 보수적인 쪽이라 초록은 의미가 있다.
            기록.add(new Tick(지금, 관측, 분모().count(), 발행 == null ? -1 : 발행.globalCredit(),
                    든_것.values().stream().mapToLong(판정::failOpenCap).sum(),
                    든_것.values().stream().mapToLong(판정::globalCap).sum(),
                    든_분모()));
        }

        private GatewayRegistry 분모() {
            return 등록부.get(발행자);
        }

        /**
         * 발행자의 한 회차. 리더십을 잃었으면 제품 배선의 출발점으로 되찾고 램프를 다시 세운다. 목표는 예산으로
         * 고정한다 — 평활 이월은 이 모형이 안 다룬다.
         */
        private SnapshotMeta 발행한다() {
            if (!리더) {
                long 출발점 = new ControlPlaneConfig().startingCredit(발행자_홀더.view(), 발행자_홀더, 분모());
                if (출발점 >= 0) {
                    램프.resumeFrom(출발점);
                }
                리더 = true;
            }
            long 크레딧 = 램프.next(예산, CapacityCollector.idleMinimum(분모().count()), false);
            SnapshotMeta 발행 = new SnapshotMeta(크레딧, 분모().count());
            발행자_홀더.replace(new GatewaySnapshot(Map.of(), 발행, 시계.instant()));
            return 발행;
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

    /**
     * 전원이 정리됐다는 전제. 리더가 나중에 치면 같은 틱에 풀린 노드가 먼저 세어진다. 이것이 안 서면 이 시나리오는
     * 아무것도 안 잰다.
     */
    private static Optional<String> 첫_관측이_전제대로다(Cluster 판, int 해제, int 기대) {
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

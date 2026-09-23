package com.kafkick.waiting.control;

import com.kafkick.waiting.chaos.ChaosScenario;
import com.kafkick.waiting.chaos.RecoveryCriteria;
import com.kafkick.waiting.domain.allocation.CouponDemand;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.coupon.CouponState;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import com.kafkick.waiting.domain.admission.CircuitState;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * C4e — 되찾은 직후 클러스터 서킷이 떨린다. 사용자가 받는 몫이 출렁이면 안 된다.
 *
 * <p>방향 전환 0 은 서킷 해제의 연속 관측을, 최대 몫 상한은 램프 바닥을 문다. 등록부는 제품 배선으로
 * 만든다 — 연속 관측 수가 거기 상수로 있다.
 */
class ReacquireTrembleScenarioTest {

    /** 설계가 정한 해제 지연. 제품 상수에서 읽으면 그 상수를 바꾼 뮤턴트가 일정도 같이 줄인다. */
    private static final int 설계_감소_지연 = 3;

    private static final double 설계_배수 = 4.0;

    private static final long 목표 = 7_300;

    private static final int 노드 = 3;

    private static final int 떨림_주기 = 4;

    private static final int 도착_뒤_유지 = 5;

    private static final Instant 시각 = Instant.ofEpochSecond(1_700_000_000L);

    private final GatewayRegistry 분모 =
            new GatewayPresenceConfig().gatewayRegistry(ControlPlaneProperties.defaults());

    private final SnapshotCodec 코덱 = SnapshotCodec.create();

    private final Map<String, String> 발행 = new HashMap<>();

    private final AllocationRound 회차 = AllocationRound.of(
            () -> true,
            () -> Mono.just(new TimedDemands(
                    List.of(new CouponDemand("c1", 20_000, 1_000_000)), 시각.getEpochSecond())),
            () -> 목표, 분모::count,
            grant -> Mono.just(grant.credit()),
            hash -> {
                발행.clear();
                발행.putAll(hash);
                return Mono.empty();
            },
            () -> 시각,
            () -> Mono.just(CreditSmoother.of(1.0)),
            코덱, () -> 0L, Optional::empty,
            SoldOutCleanup.of(Integer.MAX_VALUE, new SimpleMeterRegistry()),
            ids -> Mono.just(List.of()), ids -> Mono.just(List.of()),
            QueueSweeper.of(SweepGates.warmed(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl()),
                    (ids, limit, removeFront) -> Mono.just(QueueSweeper.SweepResult.NOTHING)),
            () -> false, 분모::circuit);

    @Test
    @DisplayName("C4e_되찾은_직후_서킷이_떨려도_몫이_출렁이지_않는다")
    void C4e_되찾은_직후_서킷이_떨려도_몫이_출렁이지_않는다() {
        long[] 평시 = new long[1];
        List<Long> 임기 = new ArrayList<>();
        List<CircuitState> 유지_서킷 = new ArrayList<>();
        CircuitState[] 진입_서킷 = new CircuitState[1];
        int[] 유지_끝 = new int[1];

        ChaosScenario.named("C4e 되찾은 직후 서킷 떨림")
                .baseline(() -> {
                    분모.observed(노드);
                    틱(0, 0);
                    평시[0] = 틱(0, 0);
                })
                .inject(() -> {
                    회차.leadershipLost();
                    회차.leadershipAcquired(목표);
                    // 첫 회차 전에 반쯤 연다. 되찾은 바로 그 회차가 조여야 한다.
                    임기.add(틱(0, 1));
                    진입_서킷[0] = 분모.circuit();
                })
                .duringFault(() -> {
                    for (int 주기 = 0; 주기 < 떨림_주기; 주기++) {
                        임기.add(틱(0, 1));
                        유지_서킷.add(분모.circuit());
                        for (int i = 0; i < 설계_감소_지연 - 1; i++) {
                            임기.add(틱(0, 0));
                            유지_서킷.add(분모.circuit());
                        }
                    }
                    유지_끝[0] = 임기.size();
                })
                // 떨림이 멎는다. 마지막 주기의 닫힘 둘에 이어 셋째 관측에서 풀린다.
                .recover(() -> 분모.circuitObserved(노드, 0, 0))
                .afterRecovery(() -> IntStream.range(0, 6 + 도착_뒤_유지)
                        .forEach(i -> 임기.add(틱(0, 0))))
                .assertEntry(() -> RecoveryCriteria.violations(
                        같다("평시 몫", 평시[0], 목표),
                        같다("되찾은 첫 발행", 임기.get(0), 3),
                        진입_서킷[0] == CircuitState.HALF_OPEN ? Optional.empty()
                                : Optional.of("되찾은 직후 서킷이 " + 진입_서킷[0])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        유지_서킷.contains(CircuitState.CLOSED)
                                ? Optional.of("떨리는 동안 서킷이 풀렸다 — " + 유지_서킷)
                                : Optional.empty(),
                        같다("유지 구간 최대 몫", 최대(임기.subList(0, 유지_끝[0])), 3)))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        같다("방향 전환", 방향_전환(임기), 0),
                        최대(임기) <= 목표 ? Optional.empty()
                                : Optional.of("목표 %d 를 넘었다 — %s".formatted(목표, 임기)),
                        한_틱_배수를_넘은_자리(임기),
                        같다("첫 회복 몫", 임기.get(유지_끝[0]), 12),
                        회복열이_계단이다(임기.subList(유지_끝[0], 임기.size()))))
                .run();
    }

    /** 하트비트 한 번, 회차 한 번. 발행된 c1 의 몫을 돌려준다. */
    private long 틱(int 열림, int 반쯤열림) {
        분모.circuitObserved(노드, 열림, 반쯤열림);
        회차.run().block(Duration.ofSeconds(5));
        CouponState 상태 = 코덱.decode(발행).coupons().get("c1");
        return 상태 == null ? -1 : 상태.credit();
    }

    private static long 최대(List<Long> 열) {
        return 열.stream().mapToLong(Long::longValue).max().orElse(-1);
    }

    /** 0 이 아닌 차분의 부호가 바뀐 횟수. */
    private static int 방향_전환(List<Long> 열) {
        int 전환 = 0;
        long 앞_부호 = 0;
        for (int i = 1; i < 열.size(); i++) {
            long 부호 = Long.signum(열.get(i) - 열.get(i - 1));
            if (부호 != 0 && 앞_부호 != 0 && 부호 != 앞_부호) {
                전환++;
            }
            if (부호 != 0) {
                앞_부호 = 부호;
            }
        }
        return 전환;
    }

    private static Optional<String> 한_틱_배수를_넘은_자리(List<Long> 열) {
        long 바닥 = CapacityCollector.idleMinimum(노드);
        for (int i = 1; i < 열.size(); i++) {
            long 상한 = Math.max(바닥, (long) Math.floor(설계_배수 * 열.get(i - 1)));
            if (열.get(i) > 상한) {
                return Optional.of("%d 번째 틱이 %d → %d — 한 틱 상한 %d 를 넘었다. 열 %s"
                        .formatted(i, 열.get(i - 1), 열.get(i), 상한, 열));
            }
        }
        return Optional.empty();
    }

    private static Optional<String> 회복열이_계단이다(List<Long> 회복) {
        List<Long> 기대 = new ArrayList<>(List.of(12L, 48L, 192L, 768L, 3072L, 목표));
        기대.addAll(Collections.nCopies(도착_뒤_유지, 목표));
        return 회복.equals(기대) ? Optional.empty()
                : Optional.of("회복열이 %s — %s 여야 한다".formatted(회복, 기대));
    }

    private static Optional<String> 같다(String 이름, long 실제, long 기대) {
        return 실제 == 기대 ? Optional.empty()
                : Optional.of("%s 가 %d — %d 여야 한다".formatted(이름, 실제, 기대));
    }
}

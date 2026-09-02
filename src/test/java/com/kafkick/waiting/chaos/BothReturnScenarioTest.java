package com.kafkick.waiting.chaos;

import com.kafkick.waiting.control.CapacityCollector;
import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.control.CapacityReport;
import com.kafkick.waiting.control.GatewayRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * X2 — 뒷단 인스턴스와 게이트웨이 노드가 <b>같은 순간에</b> 돌아온다.
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 조합 시나리오 절이 든다.
 * 여기는 그것을 어떻게 판정하는가만 든다.
 */
@Tag("chaos")
class BothReturnScenarioTest {

    private static final long NOW = 1_800_000_000L;

    /**
     * 배선이 쓰는 값 그대로 읽는다.
     *
     * <p><b>손으로 적으면 배포 기본값이 바뀌어도 이 시험만 옛 눈금으로 초록이다</b> —
     * 그러면 계획서가 "닫혔다" 고 적은 것이 다른 배포의 얘기가 된다.
     */
    private static final ControlPlaneProperties.Capacity 배선 =
            ControlPlaneProperties.defaults().capacity();

    private static final Duration 램프 = 배선.rampUp();

    private static final Duration 신선도 = 배선.freshness();

    private static final long 하한 = 배선.floor();

    private static final int 남은_노드 = 4;

    private static final int 복귀_뒤_노드 = 5;

    /** 뒷단 하나가 보고하는 여유. 둘이 서면 전역 크레딧이 그 두 배다. */
    private static final long 인스턴스_여유 = 5_000;

    /** 하강 지연. 노드가 줄어드는 쪽은 늦게 반영된다 (F5). */
    private static final int 하강_지연_틱 = 배선.rampDownTicks();

    /** 소실 첫 관측 뒤의 분모. 지연이 있으면 아직 안 내려간다. */
    private final int[] 첫_관측_뒤_분모 = new int[1];

    private final CapacityCollector 수집기 =
            CapacityCollector.of(램프, 신선도, 하한, 배선.perInstanceCap());

    /**
     * 발행 전에 걸리는 평활화. <b>배선이 쓰는 계수와 같다.</b>
     *
     * <p>수집기의 날값을 그대로 나누면 프로덕션이 실제로 발행하는 값과 다른 것을
     * 재게 된다 — 평활화나 이월이 깨져도 이 시나리오가 초록이다.
     */
    /** 계수를 배선과 나눠 쓴다. 따로 적으면 기본값이 바뀌는 날 갈린다. */
    private final CreditSmoother 평활 = CreditSmoother.of(CreditSmoother.DEFAULT_ALPHA);

    /** 가라앉았다고 볼 남은 오차. 램프 끝 판정의 허용치와 같은 값이다. */
    private static final double 허용_오차 = 0.01;

    /**
     * 평활화가 가라앉는 데 드는 틱.
     *
     * <p>남는 오차가 {@code (1 - alpha)^n} 이므로 계수에서 역산한다 — 상수로
     * 두면 계수를 바꾸는 날 이 시나리오가 덜 가라앉은 값을 정상값으로 읽는다.
     */
    private static final int 수렴_틱 = (int) Math.ceil(
            Math.log(허용_오차) / Math.log(1 - CreditSmoother.DEFAULT_ALPHA));

    private final GatewayRegistry 분모 = GatewayRegistry.of(하강_지연_틱, 복귀_뒤_노드);

    /**
     * 살아 있던 하나와, 있으면 둘째.
     *
     * <p><b>돌아오는 인스턴스는 새 식별자다</b> (R-3). 재기동은 새 이름으로 오므로
     * 옛 기록이 램프를 건너뛰게 하지 않는다 — 같은 이름으로 두면 램프가 안 걸려
     * 이 시나리오가 재려던 것이 통째로 사라진다.
     */
    private List<CapacityReport> 보고(long 지금, String 둘째) {
        return 둘째 == null
                ? List.of(new CapacityReport("be-1", 인스턴스_여유, 지금))
                : List.of(new CapacityReport("be-1", 인스턴스_여유, 지금),
                        new CapacityReport(둘째, 인스턴스_여유, 지금));
    }

    /**
     * 이 수준이 <b>가라앉았을 때</b>의 노드당 몫.
     *
     * <p>수집기의 날값을 그대로 나누면 프로덕션이 발행하는 값과 다른 것을 재게
     * 된다 — 발행 전에 평활화가 걸린다. 그렇다고 한 틱만 넣으면 평활화가 아직
     * 따라오는 중이라 값이 흔들리므로, <b>같은 수준을 충분히 넣어 가라앉힌 뒤</b>
     * 나눈다. 이 시나리오가 견주는 것은 구간마다의 정상값이지 전이의 모양이 아니다.
     */
    private long 노드당(long 크레딧) {
        double 가라앉은 = 크레딧;
        for (int i = 0; i < 수렴_틱; i++) {
            가라앉은 = 평활.observe(크레딧);
        }
        return (long) 가라앉은 / 분모.count();
    }

    /**
     * 세 구간을 한 판정으로 잇는다.
     *
     * <p><b>둘이 같은 순간에 돌아오는 것이 이 시나리오다.</b> 하나씩 오면 C6·C7 이
     * 각자 재는데, 같이 오면 분모와 분자가 동시에 움직여 노드당 몫이 어디로 갈지
     * 두 시나리오 어느 쪽도 안 본다.
     */
    @Test
    @DisplayName("X2_뒷단과_노드가_같이_돌아와도_노드당_몫이_안_뛴다")
    void X2_뒷단과_노드가_같이_돌아와도_노드당_몫이_안_뛴다() {
        long[] 평시 = new long[2];
        long[] 소실중 = new long[2];
        long[] 복귀_직후 = new long[2];
        long[] 램프_중간 = new long[2];
        long[] 램프_끝 = new long[2];
        int[] 복귀_분모 = new int[1];

        ChaosScenario.named("X2 뒷단과 노드가 같이 돌아온다")
                .baseline(() -> {
                    분모.observed(복귀_뒤_노드);
                    평시[0] = 수집기.collect(보고(NOW, "be-2"), NOW, 분모.count());
                    평시[1] = 노드당(평시[0]);
                })
                .inject(() -> {
                    // 뒷단 하나와 노드 하나가 같이 빠진다. 분모는 늦게 준다 (F5).
                    분모.observed(남은_노드);
                    첫_관측_뒤_분모[0] = 분모.count();
                    for (int i = 1; i < 하강_지연_틱; i++) {
                        분모.observed(남은_노드);
                    }
                    소실중[0] = 수집기.collect(보고(NOW + 1, null), NOW + 1, 분모.count());
                    소실중[1] = 노드당(소실중[0]);
                })
                .duringFault(() -> {
                    // 소실이 이어진다. 램프 기록이 살아 있어야 회복이 계단이 안 된다.
                    for (int t = 2; t < 10; t++) {
                        수집기.collect(보고(NOW + t, null), NOW + t, 분모.count());
                    }
                })
                .recover(() -> {
                    // **둘이 같이 돌아온다.** 분모는 증가라 즉시, 뒷단은 램프를 탄다.
                    분모.observed(복귀_뒤_노드);
                    복귀_분모[0] = 분모.count();
                    복귀_직후[0] = 수집기.collect(보고(NOW + 10, "be-3"), NOW + 10, 분모.count());
                    복귀_직후[1] = 노드당(복귀_직후[0]);
                })
                .afterRecovery(() -> {
                    // **중간을 하나 집는다.** 두 끝점(0초·창 끝)만 보면 램프의
                    // 기울기와 창 길이를 아무도 안 잰다 — 창을 열두 배로 줄이는
                    // 뮤턴트도, 첫 순간에 20% 를 주는 뮤턴트도 그대로 지나간다.
                    long 중간 = NOW + 10 + 램프.toSeconds() / 2;
                    램프_중간[0] = 수집기.collect(보고(중간, "be-3"), 중간, 분모.count());
                    램프_중간[1] = 노드당(램프_중간[0]);
                    long 끝 = NOW + 10 + 램프.toSeconds();
                    램프_끝[0] = 수집기.collect(보고(끝, "be-3"), 끝, 분모.count());
                    램프_끝[1] = 노드당(램프_끝[0]);
                })
                .assertEntry(() -> RecoveryCriteria.violations(
                        // 전제 — 평시에 둘이 다 실려야 아래 값이 뜻을 갖는다.
                        평시에_둘이_실렸다(평시[0]),
                        // **첫 관측에 안 내려간다.** 즉시 내려가면 남은 노드가
                        // 죽은 노드의 몫까지 쓴다 — 초과 발급 방향이다 (F5).
                        분모가_첫_관측에_안_내려간다(첫_관측_뒤_분모[0]),
                        // 소실 구간은 과소 통과다. 분모가 아직 크고 분자만 줄었다.
                        소실이_과소_방향이다(평시[1], 소실중[1])))
                .assertDuring(() -> RecoveryCriteria.violations(
                        // 분모는 지연 뒤에 내려간다 (F5). 여기서 즉시 내려가면
                        // 남은 노드가 죽은 노드의 몫까지 쓴다.
                        분모가_지연_뒤에_내려간다()))
                .assertRecovery(() -> RecoveryCriteria.violations(
                        // 분모는 즉시 올라간다. 늦으면 기존 노드가 작은 분모로 나눈다.
                        분모가_즉시_올라간다(복귀_분모[0]),
                        // **여기가 이 조합의 전부다.** 분모가 즉시 늘고 분자는 램프를
                        // 타므로 노드당 몫이 복귀 순간에 오히려 줄어야 한다.
                        복귀가_노드당_몫을_안_올린다(소실중[1], 복귀_직후[1]),
                        // **갓 뜬 인스턴스는 첫 순간에 아무것도 안 낸다** (F6).
                        // 노드당 몫만 보면 분모가 커진 몫에 가려 20% 를 흘려도
                        // 통과한다 — 콜드 몫을 따로 못 박는다.
                        복귀_직후에_콜드_몫이_없다(복귀_직후[0]),
                        // 램프의 절반 지점에서 콜드 인스턴스가 대략 절반을 낸다.
                        램프_중간이_절반쯤이다(램프_중간[0]),
                        // 램프가 끝나면 평시로 돌아온다.
                        램프_끝에_평시로_돌아온다(평시[1], 램프_끝[1])))
                // **RC4 는 여기서 안 잰다.** 평시와 복귀 직후가 같은 두 인스턴스를
                // 같은 분모로 나눈 값이라 복귀 직후가 평시를 넘을 수 없다 — 비율이
                // 1 을 못 넘으니 한계 1.2 에 닿을 방법이 없다. 램프를 통째로 꺼도
                // 그 판정만은 침묵한다. 재려면 인스턴스별 유입이 필요한데 이 판에는
                // 그 값이 없다 (계획서 8.2.3 의 완료 조건이 정확히 그것이다).
                // **RC1·RC2·RC5 는 여기서 안 잰다.** 줄을 안 세운다. 이 시나리오가 재는
                // 것은 분모와 분자가 같은 순간에 움직일 때의 노드당 몫이다.
                .run();
    }

    private Optional<String> 평시에_둘이_실렸다(long 크레딧) {
        long 기대 = 인스턴스_여유 * 2;
        return 크레딧 == 기대 ? Optional.empty()
                : Optional.of("전제 — 평시 크레딧이 %d 다. %d 여야 둘이 실린 것이다"
                        .formatted(크레딧, 기대));
    }

    private Optional<String> 소실이_과소_방향이다(long 평시, long 소실중) {
        return 소실중 < 평시 ? Optional.empty()
                : Optional.of("뒷단이 빠졌는데 노드당 몫이 %d 에서 %d 로 안 줄었다"
                        .formatted(평시, 소실중));
    }

    private Optional<String> 분모가_첫_관측에_안_내려간다(int 분모값) {
        return 분모값 == 복귀_뒤_노드 ? Optional.empty()
                : Optional.of("첫 관측에 분모가 %d 로 내려갔다 — %d 여야 한다 (F5)"
                        .formatted(분모값, 복귀_뒤_노드));
    }

    private Optional<String> 분모가_지연_뒤에_내려간다() {
        return 분모.count() == 남은_노드 ? Optional.empty()
                : Optional.of("지연이 지났는데 분모가 %d 다 — %d 여야 한다"
                        .formatted(분모.count(), 남은_노드));
    }

    private Optional<String> 분모가_즉시_올라간다(int 분모값) {
        return 분모값 == 복귀_뒤_노드 ? Optional.empty()
                : Optional.of("노드가 돌아왔는데 분모가 %d 다 — 증가는 즉시여야 한다"
                        .formatted(분모값));
    }

    /**
     * 둘이 같이 돌아오면 노드당 몫이 <b>줄어든다</b>.
     *
     * <p>분모는 증가라 즉시 반영되는데 분자는 램프를 탄다. 램프를 끄면 이 자리가
     * 뛰고, 하필 뒷단이 가장 차가울 때 유입이 가장 높아진다 (F6).
     */
    /**
     * <b>갓 뜬 인스턴스는 첫 순간에 아무것도 안 낸다</b> (F6).
     *
     * <p>노드당 몫만 보면 분모가 커진 만큼에 가려 콜드가 자기 여유의 25% 까지
     * 흘려도 통과한다 — 그게 F6 이 막으려는 사고 그 자체다.
     */
    private Optional<String> 복귀_직후에_콜드_몫이_없다(long 크레딧) {
        long 콜드_몫 = 크레딧 - 인스턴스_여유;
        return 콜드_몫 == 0 ? Optional.empty()
                : Optional.of("복귀 첫 순간에 콜드 인스턴스가 %d 를 냈다 — 0 이어야 한다"
                        .formatted(콜드_몫));
    }

    /**
     * 램프 절반 지점에서 콜드 인스턴스가 대략 절반을 낸다.
     *
     * <p><b>두 끝점만 보면 기울기를 안 잰다.</b> {@code warmed=0} 에서는 곡선의
     * 모양과 창 길이가 무엇이든 0 이라, 첫 순간에 20% 를 주는 뮤턴트도 창을
     * 열두 배로 줄이는 뮤턴트도 그대로 지나간다.
     */
    private Optional<String> 램프_중간이_절반쯤이다(long 크레딧) {
        long 콜드_몫 = 크레딧 - 인스턴스_여유;
        long 아래 = 인스턴스_여유 * 3 / 10;
        long 위 = 인스턴스_여유 * 7 / 10;
        return 콜드_몫 >= 아래 && 콜드_몫 <= 위 ? Optional.empty()
                : Optional.of("램프 절반에서 콜드 인스턴스 몫이 %d 다 — %d~%d 여야 한다"
                        .formatted(콜드_몫, 아래, 위));
    }

    private Optional<String> 복귀가_노드당_몫을_안_올린다(long 소실중, long 복귀_직후) {
        return 복귀_직후 <= 소실중 ? Optional.empty()
                : Optional.of("복귀 순간에 노드당 몫이 %d 에서 %d 로 뛰었다 — 램프가 안 걸렸다"
                        .formatted(소실중, 복귀_직후));
    }

    /**
     * 램프가 끝나면 평시로 돌아온다.
     *
     * <p><b>정확히 같기를 요구하지 않는다.</b> 발행 전 평활화가 지수 이동평균이라
     * 몇 틱을 넣어도 마지막 한 자리가 남는다 — 그 한 자리를 요구하면 평활화
     * 계수를 만지는 날 이 시나리오가 뜻 없이 빨개진다.
     */
    private Optional<String> 램프_끝에_평시로_돌아온다(long 평시, long 램프_끝) {
        long 오차 = Math.abs(램프_끝 - 평시);
        return 오차 <= (long) (평시 * 허용_오차) ? Optional.empty()
                : Optional.of("램프가 끝났는데 노드당 몫이 %d 다 — 평시 %d 의 %.0f%% 안이어야 한다"
                        .formatted(램프_끝, 평시, 허용_오차 * 100));
    }
}

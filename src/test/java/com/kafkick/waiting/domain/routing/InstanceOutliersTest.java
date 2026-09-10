package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 빨리 실패하는 인스턴스를 후보에서 빼는 규칙.
 *
 * <p>물린 표는 응답이 끝날 때 놓는다. 그래서 500 을 즉시 뱉는 대는 물린 건수가
 * 안 쌓여 <b>가장 한가해 보이고</b>, 부하율로 고르는 이상 그쪽으로 더 간다.
 */
@Tag("unit")
class InstanceOutliersTest {

    private static final Duration 배제_시간 = Duration.ofSeconds(10);

    private static final Duration 램프 = Duration.ofSeconds(60);

    private InstanceOutliers 배제기() {
        return InstanceOutliers.of(3, 배제_시간, 램프);
    }

    @Test
    @DisplayName("연속으로_실패하면_후보에서_뺀다")
    void 연속으로_실패하면_후보에서_뺀다() {
        InstanceOutliers outliers = 배제기();
        Set<String> 산_대 = Set.of("가", "나");

        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        assertThat(outliers.ejected(산_대, 1_000)).containsExactly("가");
    }

    @Test
    @DisplayName("임계에_못_미치면_안_뺀다")
    void 임계에_못_미치면_안_뺀다() {
        InstanceOutliers outliers = 배제기();

        outliers.failed("가", 1_000);
        outliers.failed("가", 1_000);

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).isEmpty();
    }

    /**
     * <b>연속이어야 한다.</b> 누적으로 세면 오래 산 인스턴스가 실패 몇 건만으로
     * 결국 배제된다 — 부하가 큰 대일수록 먼저 걸린다.
     */
    @Test
    @DisplayName("성공_하나가_연속을_끊는다")
    void 성공_하나가_연속을_끊는다() {
        InstanceOutliers outliers = 배제기();

        outliers.failed("가", 1_000);
        outliers.failed("가", 1_000);
        outliers.succeeded("가", 1_000);
        outliers.failed("가", 1_000);

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).isEmpty();
    }

    /**
     * <b>배제는 영구가 아니다.</b> 안 풀면 회복한 인스턴스가 영영 안 돌아오고,
     * 재기동 없이는 용량이 못 회복한다.
     */
    @Test
    @DisplayName("배제_시간이_지나면_다시_후보다")
    void 배제_시간이_지나면_다시_후보다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000 + 배제_시간.toMillis() - 1))
                .as("아직 배제 중")
                .containsExactly("가");
        assertThat(outliers.ejected(Set.of("가", "나"), 1_000 + 배제_시간.toMillis()))
                .as("풀린 뒤")
                .isEmpty();
    }

    /**
     * <b>풀리는 그 순간이 램프의 시작이다.</b> 임계에 못 미치는 실패가 램프를 취소해
     * 버리면 그 대가 몫을 깎지 않은 채 전량을 받는다 — 배제가 노린 것과 반대다.
     */
    @Test
    @DisplayName("램프를_취소하지_않고_이어_간다")
    void 램프를_취소하지_않고_이어_간다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 풀린_뒤 = 1_000 + 배제_시간.toMillis();
        assertThat(outliers.ejected(Set.of("가", "나"), 풀린_뒤)).isEmpty();

        outliers.failed("가", 풀린_뒤);

        assertThat(outliers.ejected(Set.of("가", "나"), 풀린_뒤))
                .as("임계에 못 미치면 안 뺀다").isEmpty();
        assertThat(outliers.recoveryRemaining("가", 풀린_뒤))
                .as("램프는 그대로 이어진다").isEqualTo(1.0);
    }

    /**
     * <b>램프 구간이 밖에서 보여야 한다.</b> 배제 게이지는 배제 창만 세므로 되돌리는
     * 중인 대는 어디에도 안 잡힌다 — 그 대가 회복을 마쳤는지를 물을 수단이 없다.
     */
    @Test
    @DisplayName("되돌리는_중인_대와_안_준_몫이_보인다")
    void 되돌리는_중인_대와_안_준_몫이_보인다() {
        InstanceOutliers outliers = 배제기();
        long 늦게 = 1_000 + 램프.toMillis() / 4;
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        for (int i = 0; i < 3; i++) {
            outliers.failed("나", 늦게);
        }
        // 안 앓은 대는 기록만 있고 되돌릴 것이 없다.
        outliers.succeeded("다", 늦게);
        outliers.retain(Set.of("가", "나", "다"), 늦게);

        // 가는 이미 램프에 들었고 나는 아직 배제 창 안이다.
        long 나의_배제_중 = 늦게 + 배제_시간.toMillis() / 2;
        assertThat(outliers.rampingCount(나의_배제_중))
                .as("배제 중은 되돌리는 중이 아니다").isOne();

        long 램프_절반 = 1_000 + 배제_시간.toMillis() + 램프.toMillis() / 2;
        assertThat(outliers.rampingCount(램프_절반)).as("둘이 서로 다른 지점에 있다").isEqualTo(2);
        // 가는 절반, 나는 사분의 일 늦게 들어가 4분의 3 이 남았다.
        assertThat(outliers.rampSuppressed(램프_절반))
                .as("안 준 몫은 대마다의 합이다").isCloseTo(0.5 + 0.75, within(0.01));

        long 가라앉은_뒤 = 늦게 + 배제_시간.toMillis() + 램프.toMillis();
        assertThat(outliers.rampingCount(가라앉은_뒤)).isZero();
        assertThat(outliers.rampSuppressed(가라앉은_뒤)).isZero();
    }

    /**
     * <b>걷히길 기다리는 죽은 기록은 안 센다.</b> 앓다 빠진 대의 기록은 램프가 끝날
     * 때까지 남는데, 그것까지 세면 롤링 배포마다 게이지가 75초씩 부풀어 회복이 안
     * 끝난 것처럼 보인다.
     */
    @Test
    @DisplayName("사라진_대는_되돌리는_중으로_안_센다")
    void 사라진_대는_되돌리는_중으로_안_센다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 램프_절반 = 1_000 + 배제_시간.toMillis() + 램프.toMillis() / 2;
        outliers.retain(Set.of("나"), 램프_절반);

        assertThat(outliers.tracked()).as("기록 자체는 남는다").containsExactly("가");
        assertThat(outliers.rampingCount(램프_절반)).isZero();
        assertThat(outliers.rampSuppressed(램프_절반)).isZero();
    }

    /**
     * <b>그 대에게 요청이 다시 안 와도 완주로 센다.</b> 그 대의 성공·실패로만 세면
     * 그 전에 빠진 대의 완주가 영영 안 세어져, 연 배제에서 재배제와 완주를 뺀 값이
     * 배포마다 벌어진다. 걷는 자리는 라우팅 한 건마다 돈다.
     */
    @Test
    @DisplayName("그_대에_요청이_안_와도_완주로_센다")
    void 그_대에_요청이_안_와도_완주로_센다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        long 램프_절반 = 1_000 + 배제_시간.toMillis() + 램프.toMillis() / 2;
        outliers.retain(Set.of("가"), 램프_절반);
        assertThat(outliers.rampsCompleted()).as("아직 도는 중이다").isZero();

        long 가라앉은_뒤 = 1_000 + 배제_시간.toMillis() + 램프.toMillis();
        outliers.retain(Set.of("가"), 가라앉은_뒤);
        assertThat(outliers.rampsCompleted()).isOne();

        outliers.retain(Set.of("가"), 가라앉은_뒤 + 1);
        assertThat(outliers.rampsCompleted()).as("한 번만 센다").isOne();
    }

    /**
     * <b>되돌리다 다시 빠진 것과 끝까지 마친 것을 가른다.</b> 앞엣것만 늘고 뒤엣것이
     * 안 늘면 회복이 안 끝나는 것인데, 합쳐 세면 그 상태가 안 보인다.
     */
    @Test
    @DisplayName("정상_구간의_배제와_재배제와_완주를_따로_센다")
    void 정상_구간의_배제와_재배제와_완주를_따로_센다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        assertThat(outliers.ejectionsStarted()).isOne();
        assertThat(outliers.reEjections()).isZero();

        long 램프_중 = 1_000 + 배제_시간.toMillis() + 1;
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 램프_중);
        }
        assertThat(outliers.reEjections()).as("되돌리다 다시 빠졌다").isOne();
        assertThat(outliers.ejectionsStarted()).as("국면을 연 배제는 안 는다").isOne();

        long 두_번째_램프_뒤 = 램프_중 + 배제_시간.toMillis() + 램프.toMillis();
        outliers.succeeded("가", 두_번째_램프_뒤);

        assertThat(outliers.rampsCompleted()).as("끝까지 마쳤다").isOne();
        assertThat(outliers.ejectionsStarted()).as("완주가 다른 계수를 안 건드린다").isOne();
        assertThat(outliers.reEjections()).as("완주가 다른 계수를 안 건드린다").isOne();
    }

    /**
     * <b>램프에서 쌓은 연속은 램프와 함께 버린다.</b> 넘기면 평상시 첫 실패 한 건이
     * 그 대를 다시 뺀다 — 하필 트래픽이 끊겼다 돌아오는 순간이 그 자리다.
     */
    @Test
    @DisplayName("램프에서_쌓은_연속은_안_넘어간다")
    void 램프에서_쌓은_연속은_안_넘어간다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 램프_중 = 1_000 + 배제_시간.toMillis() + 1;
        outliers.failed("가", 램프_중);
        outliers.failed("가", 램프_중);

        long 가라앉은_뒤 = 1_000 + 배제_시간.toMillis() + 램프.toMillis();
        outliers.failed("가", 가라앉은_뒤);

        assertThat(outliers.ejected(Set.of("가", "나"), 가라앉은_뒤))
                .as("앓은 적 없는 대와 같이 임계만큼 준다").isEmpty();
        assertThat(outliers.rampsCompleted()).as("그 실패가 램프를 닫는다").isOne();
    }

    /**
     * <b>걷는 자리가 램프를 끝낼 때도 연속을 버린다.</b> 실패가 끝내는 갈래만 버리면,
     * 다른 대의 요청이 먼저 걷은 뒤에 오는 평상시 첫 실패 한 건이 그 대를 다시 뺀다.
     */
    @Test
    @DisplayName("걷힌_뒤에도_연속은_안_넘어간다")
    void 걷힌_뒤에도_연속은_안_넘어간다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 램프_중 = 1_000 + 배제_시간.toMillis() + 1;
        outliers.failed("가", 램프_중);
        outliers.failed("가", 램프_중);

        long 가라앉은_뒤 = 1_000 + 배제_시간.toMillis() + 램프.toMillis();
        outliers.retain(Set.of("가", "나"), 가라앉은_뒤);
        outliers.failed("가", 가라앉은_뒤);

        assertThat(outliers.ejected(Set.of("가", "나"), 가라앉은_뒤))
                .as("앓은 적 없는 대와 같이 임계만큼 준다").isEmpty();
    }

    /**
     * <b>램프까지 다 지나야 평상시다.</b> 그 전에는 되돌리는 중이라 미덥지 않고,
     * 다 지나면 한 번도 앓은 적 없는 대와 같이 다룬다.
     */
    @Test
    @DisplayName("램프까지_지나면_다시_임계만큼_준다")
    void 램프까지_지나면_다시_임계만큼_준다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 다_지난_뒤 = 1_000 + 배제_시간.toMillis() + 램프.toMillis();

        outliers.failed("가", 다_지난_뒤);

        assertThat(outliers.ejected(Set.of("가", "나"), 다_지난_뒤)).isEmpty();
    }

    /**
     * <b>배제 중의 실패는 시계를 되감는다.</b> 그 구간은 트래픽이 0 이라 여기 오는
     * 것은 배제 전에 나갔다 늦게 돌아온 결과다 — 아직 안 나은 대가 원래 시각에
     * 그대로 풀리면 안 된다.
     */
    @Test
    @DisplayName("배제_중_한_건이_시계를_되감는다")
    void 배제_중_한_건이_시계를_되감는다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 절반 = 1_000 + 배제_시간.toMillis() / 2;

        outliers.failed("가", 절반);

        long 원래_풀릴_때 = 1_000 + 배제_시간.toMillis();
        assertThat(outliers.ejected(Set.of("가", "나"), 원래_풀릴_때))
                .as("되감겼으므로 아직 배제 중이다").containsExactly("가");
    }

    /**
     * <b>램프 중에도 뺄 근거는 처음과 같다.</b> 한 건으로 되감으면 배경 오류만으로
     * 램프가 안 끝난다 — 대당 50rps 에서 1% 면 완주 확률이 사실상 0 이고, 그 대는
     * 영구히 제 몫에서 빠진다. 램프가 이미 그 대의 몫을 선형으로 깎고 있다.
     */
    @Test
    @DisplayName("램프_중_한_건은_안_되감는다")
    void 램프_중_한_건은_안_되감는다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 램프_중 = 1_000 + 배제_시간.toMillis() + 1;
        assertThat(outliers.ejected(Set.of("가", "나"), 램프_중)).isEmpty();

        outliers.failed("가", 램프_중);

        assertThat(outliers.ejected(Set.of("가", "나"), 램프_중))
                .as("배경 잡음 한 건으로 되감으면 램프가 영영 안 끝난다").isEmpty();
    }

    /** 진짜 고장은 램프 중에도 임계만큼이면 다시 빠진다. 근거가 서면 그 자리에서 뺀다. */
    @Test
    @DisplayName("램프_중_연속_임계면_다시_뺀다")
    void 램프_중_연속_임계면_다시_뺀다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 램프_중 = 1_000 + 배제_시간.toMillis() + 1;

        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 램프_중);
        }

        assertThat(outliers.ejected(Set.of("가", "나"), 램프_중)).containsExactly("가");
    }

    /** 성공 하나가 램프 중의 연속도 끊는다. 안 끊으면 흩어진 잡음이 쌓여 되감는다. */
    @Test
    @DisplayName("램프_중_성공이_연속을_끊는다")
    void 램프_중_성공이_연속을_끊는다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 램프_중 = 1_000 + 배제_시간.toMillis() + 1;

        outliers.failed("가", 램프_중);
        outliers.failed("가", 램프_중);
        outliers.succeeded("가", 램프_중);
        outliers.failed("가", 램프_중);
        outliers.failed("가", 램프_중);

        assertThat(outliers.ejected(Set.of("가", "나"), 램프_중)).isEmpty();
    }

    /**
     * <b>돌아오는 순간이 절벽이 아니어야 한다.</b> 배제 동안 트래픽이 0 이라
     * 물린 건수도 0 이고, 그대로 두면 부하율이 가장 낮아 전량이 그리로 간다.
     */
    @Test
    @DisplayName("되돌아올_때_제_몫이_천천히_는다")
    void 되돌아올_때_제_몫이_천천히_는다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 풀린_때 = 1_000 + 배제_시간.toMillis();

        assertThat(outliers.recoveryRemaining("가", 1_000)).as("배제 중").isZero();
        assertThat(outliers.recoveryRemaining("가", 풀린_때)).as("막 풀렸다").isEqualTo(1);
        assertThat(outliers.recoveryRemaining("가", 풀린_때 + 램프.toMillis() / 2))
                .as("절반").isEqualTo(0.5);
        assertThat(outliers.recoveryRemaining("가", 풀린_때 + 램프.toMillis()))
                .as("다 돌아왔다").isZero();
        assertThat(outliers.recoveryRemaining("나", 풀린_때)).as("기록조차 없는 대").isZero();

        outliers.succeeded("나", 풀린_때);
        assertThat(outliers.recoveryRemaining("나", 풀린_때))
                .as("기록은 있으나 앓은 적 없는 대").isZero();
    }

    /**
     * <b>배제가 전면 차단이 되면 안 된다.</b> 뒷단 전체가 앓을 때 전부 빼면
     * 보낼 곳이 0 이 되고, 그건 열화된 대로라도 보내는 것보다 나쁘다. 계획서가
     * 인스턴스별 서킷을 막았던 이유가 바로 이것이다.
     */
    @Test
    @DisplayName("전부_대상이면_하나도_안_뺀다")
    void 전부_대상이면_하나도_안_뺀다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
            outliers.failed("나", 1_000);
        }

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).isEmpty();
    }

    /** 한 대만 남아 있으면 그 한 대가 앓아도 뺄 수 없다. */
    @Test
    @DisplayName("한_대뿐이면_안_뺀다")
    void 한_대뿐이면_안_뺀다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        assertThat(outliers.ejected(Set.of("가"), 1_000)).isEmpty();
    }

    /** 목록에 없는 인스턴스는 뺄 것도 없다. 산 것만 돌려준다. */
    @Test
    @DisplayName("목록에_없는_대는_안_돌려준다")
    void 목록에_없는_대는_안_돌려준다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        assertThat(outliers.ejected(Set.of("나", "다"), 1_000)).isEmpty();
    }

    /**
     * 식별자는 재기동마다 새로 온다. 안 걷으면 죽은 이름이 무한히 쌓인다.
     * 물린 건수와 달리 <b>산 요청을 볼 필요가 없다</b> — 실패 기록은 지워도
     * 다음 실패부터 다시 세면 된다.
     */
    @Test
    @DisplayName("목록에서_사라진_대의_기록을_버린다")
    void 목록에서_사라진_대의_기록을_버린다() {
        InstanceOutliers outliers = 배제기();
        outliers.failed("가", 1_000);

        outliers.retain(Set.of("나"), 1_000);
        outliers.succeeded("나", 1_000);

        assertThat(outliers.tracked()).containsExactly("나");
    }

    /**
     * <b>앓는 대는 목록을 들락거린다.</b> readiness 가 흔들리기 때문이다. 그때
     * 기록을 지우면 돌아올 때마다 임계만큼을 새로 먹여야 해서 배제가 영영 안
     * 걸린다 — 이 기능을 통째로 무력화하는 자리다.
     */
    @Test
    @DisplayName("배제_중이면_목록에서_빠져도_기록을_지킨다")
    void 배제_중이면_목록에서_빠져도_기록을_지킨다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        outliers.retain(Set.of("나"), 1_000);

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).containsExactly("가");
    }

    /** 남겨도 시간으로 유계다. 배제와 램프가 끝나면 다음 호출에서 걷힌다. */
    @Test
    @DisplayName("가라앉은_뒤에는_걷힌다")
    void 가라앉은_뒤에는_걷힌다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        outliers.retain(Set.of("나"), 1_000 + 배제_시간.toMillis() + 램프.toMillis());

        assertThat(outliers.tracked()).doesNotContain("가");
        // **빠진 대의 완주도 세어야 한다.** 산 대만 세면 롤링 배포마다 연 배제에서
        // 재배제와 완주를 뺀 값이 벌어져, 계수 셋을 견주는 것 자체가 뜻을 잃는다.
        assertThat(outliers.rampsCompleted()).as("걷으면서 완주로 센다").isOne();
    }

    /**
     * <b>목록에 남은 대의 배제는 지킨다.</b> 목록은 판정 재료가 새로 올 때마다
     * 다시 오므로, 걷는 김에 산 대까지 지우면 배제가 1초마다 풀린다.
     */
    @Test
    @DisplayName("목록에_남은_대의_배제는_지킨다")
    void 목록에_남은_대의_배제는_지킨다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        outliers.retain(Set.of("가", "나"), 1_000);

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).containsExactly("가");
    }

    /** 걷힌 대가 돌아오면 기록 없이 시작한다 — 나가기 전 실패가 안 따라붙는다. */
    @Test
    @DisplayName("걷힌_뒤_돌아오면_임계만큼_다시_준다")
    void 걷힌_뒤_돌아오면_임계만큼_다시_준다() {
        InstanceOutliers outliers = 배제기();
        outliers.failed("가", 1_000);
        outliers.failed("가", 1_000);

        outliers.retain(Set.of("나"), 1_000);
        outliers.failed("가", 1_000);

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).isEmpty();
    }

    @Test
    @DisplayName("임계와_배제_시간은_양수여야_한다")
    void 임계와_배제_시간은_양수여야_한다() {
        assertThatThrownBy(() -> InstanceOutliers.of(0, 배제_시간, 램프))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> InstanceOutliers.of(3, Duration.ZERO, 램프))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> InstanceOutliers.of(3, Duration.ofMillis(-1), 램프))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> InstanceOutliers.of(3, 배제_시간, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        // 밀리초 미만은 0 으로 잘려 배제가 걸리자마자 풀린다.
        assertThatThrownBy(() -> InstanceOutliers.of(3, Duration.ofNanos(500_000), 램프))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> InstanceOutliers.of(3, 배제_시간, Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 지표가 읽는 값이다. <b>전부가 대상이면 걸러진 수는 0 인데 이 값은 전체
     * 대수다</b> — 그 어긋남이 뒷단 전체가 앓는다는 신호라 이쪽을 낸다.
     */
    @Test
    @DisplayName("표시된_수는_걸러진_수와_따로_센다")
    void 표시된_수는_걸러진_수와_따로_센다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
            outliers.failed("나", 1_000);
        }
        outliers.succeeded("다", 1_000);
        outliers.retain(Set.of("가", "나", "다"), 1_000);

        assertThat(outliers.markedCount(1_000)).as("표시된 것").isEqualTo(2);
        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).as("걸러진 것").isEmpty();
        assertThat(outliers.seenCount()).as("견줄 대상").isEqualTo(3);
    }

    /**
     * <b>걷히길 기다리는 죽은 기록을 세면 안 된다.</b> 롤링 배포에서 앓던 대가
     * 목록에서 빠지면 기록은 남는데, 그것까지 세면 멀쩡한 뒷단에 대고 전부
     * 앓는다고 말하게 된다 — 배포 때마다 헛울리는 경보가 된다.
     */
    @Test
    @DisplayName("목록에서_빠진_대는_표시된_수에_안_든다")
    void 목록에서_빠진_대는_표시된_수에_안_든다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("옛것", 1_000);
        }

        outliers.retain(Set.of("새것"), 1_000);

        assertThat(outliers.tracked()).as("기록은 남긴다").contains("옛것");
        assertThat(outliers.markedCount(1_000)).as("세지는 않는다").isZero();
    }

    /**
     * 시각은 벽시계라 보정이나 재개로 뒤로 간다. 흐른 시간을 음수로 보면 배제가
     * 안 풀리는데, <b>빠진 대는 트래픽이 0 이라 시간 말고는 나갈 문이 없다.</b>
     */
    @Test
    @DisplayName("시각이_뒤로_가도_배제가_안_굳는다")
    void 시각이_뒤로_가도_배제가_안_굳는다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 10_000);
        }

        assertThat(outliers.ejected(Set.of("가", "나"), 5_000))
                .as("뒤로 간 시각에는 갓 뺀 것으로 본다")
                .containsExactly("가");
        assertThat(outliers.recoveryRemaining("가", 5_000)).isZero();
    }

    @Test
    @DisplayName("배제_시간이_지나면_표시된_수에서도_빠진다")
    void 배제_시간이_지나면_표시된_수에서도_빠진다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        outliers.retain(Set.of("가"), 1_000);

        assertThat(outliers.markedCount(1_000)).isEqualTo(1);
        assertThat(outliers.markedCount(1_000 + 배제_시간.toMillis())).isZero();
    }

    /** 산 목록이 비어 있다. 디스커버리가 빈 목록을 줄 때 실제로 온다. */
    @Test
    @DisplayName("산_목록이_비면_뺄_것도_없다")
    void 산_목록이_비면_뺄_것도_없다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        assertThat(outliers.ejected(Set.of(), 1_000)).isEmpty();
    }

    /** 설정이 받는 값이다. 하나면 실패 한 건에 바로 빠진다. */
    @Test
    @DisplayName("임계가_하나면_실패_한_건에_뺀다")
    void 임계가_하나면_실패_한_건에_뺀다() {
        InstanceOutliers outliers = InstanceOutliers.of(1, 배제_시간, 램프);

        outliers.failed("가", 1_000);

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).containsExactly("가");
    }

    /**
     * <b>배제 창을 여는 데 실패 셋이 필요하면 닫는 데도 성공 셋이 필요하다.</b> 그
     * 창은 응답 상한을 덮으라고 잡은 값인데, 그 안에 늦게 돌아온 성공 하나가 창을
     * 지우면 반쯤 고장 난 대가 스스로 배제를 취소한다.
     */
    @Test
    @DisplayName("배제_중_성공_하나로는_창이_안_지워진다")
    void 배제_중_성공_하나로는_창이_안_지워진다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        outliers.succeeded("가", 1_000);

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000))
                .as("늦게 돌아온 결과 하나는 근거가 얕다").containsExactly("가");
        assertThat(outliers.recoveryRemaining("가", 1_000))
                .as("아직 배제 중이라 되돌릴 것이 없다").isZero();
    }

    /** 임계만큼 이어지면 창을 닫되 램프로 넘긴다. 전량을 되돌리지는 않는다. */
    @Test
    @DisplayName("배제_중_성공이_임계만큼_이어지면_램프로_넘긴다")
    void 배제_중_성공이_임계만큼_이어지면_램프로_넘긴다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        for (int i = 0; i < 3; i++) {
            outliers.succeeded("가", 1_000);
        }

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).isEmpty();
        assertThat(outliers.recoveryRemaining("가", 1_000))
                .as("배제를 끝내되 램프로 넘긴다").isEqualTo(1);
    }

    /** 실패 한 건이 그 사이의 성공을 되돌린다. 흩어진 성공이 쌓여 창을 지우면 안 된다. */
    @Test
    @DisplayName("배제_중_실패가_쌓인_성공을_지운다")
    void 배제_중_실패가_쌓인_성공을_지운다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        outliers.succeeded("가", 1_000);
        outliers.succeeded("가", 1_000);
        outliers.failed("가", 1_000);
        outliers.succeeded("가", 1_000);
        outliers.succeeded("가", 1_000);

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000))
                .as("셋을 새로 채워야 한다").containsExactly("가");
    }

    /**
     * 응답 완료마다 이벤트루프 스레드에서 불린다. <b>연속을 세는 자리가 안
     * 막히면 증분을 잃어</b> 임계에 못 닿고, 앓는 대가 안 빠진다.
     */
    @Test
    @DisplayName("동시에_세도_증분을_안_잃는다")
    void 동시에_세도_증분을_안_잃는다() throws InterruptedException {
        InstanceOutliers outliers = InstanceOutliers.of(200, 배제_시간, 램프);
        전부_돌린다(8, () -> {
            for (int i = 0; i < 25; i++) {
                outliers.failed("가", 1_000);
            }
        });

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).containsExactly("가");
    }

    /**
     * 걷는 쪽은 고르는 경로에서, 세는 쪽은 응답 완료 경로에서 온다.
     * <b>둘이 실제로 동시에 돈다.</b>
     */
    @Test
    @DisplayName("걷는_동안_세도_안_깨진다")
    void 걷는_동안_세도_안_깨진다() throws InterruptedException {
        InstanceOutliers outliers = 배제기();
        Set<String> 산것 = Set.of("가", "나", "다");
        AtomicBoolean 끝 = new AtomicBoolean();

        Thread 청소 = new Thread(() -> {
            while (!끝.get()) {
                outliers.retain(산것, 1_000);
            }
        });
        청소.start();
        전부_돌린다(6, () -> {
            for (int i = 0; i < 50; i++) {
                outliers.failed("가", 1_000);
            }
        });
        끝.set(true);
        청소.join();

        assertThat(산것).containsAll(outliers.tracked());
        assertThat(outliers.ejected(산것, 1_000)).containsExactly("가");
    }

    /** 같은 순간에 출발시킨다. 순차로 돌면 재려던 경합이 안 난다. */
    private void 전부_돌린다(int 스레드, Runnable 일) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(스레드);
        CountDownLatch 출발 = new CountDownLatch(1);
        CountDownLatch 도착 = new CountDownLatch(스레드);
        for (int i = 0; i < 스레드; i++) {
            pool.execute(() -> {
                try {
                    출발.await();
                    일.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    도착.countDown();
                }
            });
        }
        출발.countDown();
        assertThat(도착.await(10, TimeUnit.SECONDS)).as("제때 끝났다").isTrue();
        pool.shutdownNow();
    }

    /**
     * <b>램프 중의 성공이 램프를 건너뛰지 않는다.</b> 되돌리는 중에 몇 건
     * 성공했다고 전량을 주면, 그 순간이 다시 절벽이 된다.
     */
    @Test
    @DisplayName("램프_중_성공해도_램프는_그대로_간다")
    void 램프_중_성공해도_램프는_그대로_간다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 램프_절반 = 1_000 + 배제_시간.toMillis() + 램프.toMillis() / 2;

        outliers.succeeded("가", 램프_절반);

        assertThat(outliers.recoveryRemaining("가", 램프_절반)).isEqualTo(0.5);
    }

    /** 램프까지 지난 뒤의 성공은 기록을 지운다. 안 지우면 죽은 이름이 쌓인다. */
    @Test
    @DisplayName("램프까지_지난_뒤_성공하면_기록이_풀린다")
    void 램프까지_지난_뒤_성공하면_기록이_풀린다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 다_지난_뒤 = 1_000 + 배제_시간.toMillis() + 램프.toMillis();

        outliers.succeeded("가", 다_지난_뒤);
        outliers.retain(Set.of("나"), 다_지난_뒤);

        assertThat(outliers.tracked()).doesNotContain("가");
    }

    /** 램프를 0 으로 두면 되돌릴 것이 없다. 복귀가 절벽이라는 뜻이다. */
    @Test
    @DisplayName("램프가_0_이면_되돌릴_몫이_없다")
    void 램프가_0_이면_되돌릴_몫이_없다() {
        InstanceOutliers outliers = InstanceOutliers.of(3, 배제_시간, Duration.ZERO);
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        assertThat(outliers.recoveryRemaining("가", 1_000 + 배제_시간.toMillis())).isZero();
    }
}

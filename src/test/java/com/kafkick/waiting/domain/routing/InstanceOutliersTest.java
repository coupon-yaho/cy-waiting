package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 빨리 실패하는 인스턴스를 후보에서 빼는 규칙.
 *
 * <p>물린 표는 응답이 끝날 때 놓는다. 그래서 500 을 즉시 뱉는 대는 물린 건수가
 * 안 쌓여 <b>가장 한가해 보이고</b>, 부하율로 고르는 이상 그쪽으로 더 간다.
 */
class InstanceOutliersTest {

    private static final Duration 배제_시간 = Duration.ofSeconds(10);

    private InstanceOutliers 배제기() {
        return InstanceOutliers.of(3, 배제_시간);
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
        outliers.succeeded("가");
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
     * <b>풀린 뒤 한 건만 실패해도 다시 뺀다.</b> 처음부터 세면 아직 고장 난 대가
     * 임계만큼 더 받고, 그동안 그 대는 여전히 가장 한가해 보인다.
     */
    @Test
    @DisplayName("풀린_뒤_한_번_더_실패하면_바로_뺀다")
    void 풀린_뒤_한_번_더_실패하면_바로_뺀다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 풀린_뒤 = 1_000 + 배제_시간.toMillis();
        assertThat(outliers.ejected(Set.of("가", "나"), 풀린_뒤)).isEmpty();

        outliers.failed("가", 풀린_뒤);

        assertThat(outliers.ejected(Set.of("가", "나"), 풀린_뒤)).containsExactly("가");
    }

    /** 풀린 뒤 성공하면 평상시로 돌아간다 — 다음 실패 하나에 다시 걸리지 않는다. */
    @Test
    @DisplayName("풀린_뒤_성공하면_다시_임계만큼_준다")
    void 풀린_뒤_성공하면_다시_임계만큼_준다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 풀린_뒤 = 1_000 + 배제_시간.toMillis();
        assertThat(outliers.ejected(Set.of("가", "나"), 풀린_뒤)).isEmpty();

        outliers.succeeded("가");
        outliers.failed("가", 풀린_뒤);

        assertThat(outliers.ejected(Set.of("가", "나"), 풀린_뒤)).isEmpty();
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
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        outliers.retain(Set.of("나"));
        outliers.succeeded("나");

        assertThat(outliers.tracked()).containsExactly("나");
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

        outliers.retain(Set.of("가", "나"));

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).containsExactly("가");
    }

    /** 돌아온 대는 기록 없이 시작한다 — 나가기 전 실패가 따라붙지 않는다. */
    @Test
    @DisplayName("걷힌_뒤_돌아오면_임계만큼_다시_준다")
    void 걷힌_뒤_돌아오면_임계만큼_다시_준다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        outliers.retain(Set.of("나"));
        outliers.failed("가", 1_000);

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).isEmpty();
    }

    @Test
    @DisplayName("임계와_배제_시간은_양수여야_한다")
    void 임계와_배제_시간은_양수여야_한다() {
        assertThatThrownBy(() -> InstanceOutliers.of(0, 배제_시간))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> InstanceOutliers.of(3, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> InstanceOutliers.of(3, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

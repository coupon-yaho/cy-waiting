package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;
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
     * <b>되돌리는 중의 실패는 그 자리에서 다시 뺀다.</b> 임계만큼을 다시 주면
     * 아직 고장 난 대가 그동안 여전히 가장 한가해 보인다.
     */
    @Test
    @DisplayName("램프_중_한_번_실패하면_바로_다시_뺀다")
    void 램프_중_한_번_실패하면_바로_다시_뺀다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        long 램프_중 = 1_000 + 배제_시간.toMillis() + 1;
        assertThat(outliers.ejected(Set.of("가", "나"), 램프_중)).isEmpty();

        outliers.failed("가", 램프_중);

        assertThat(outliers.ejected(Set.of("가", "나"), 램프_중)).containsExactly("가");
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

        assertThat(outliers.ejectedCount(1_000)).as("표시된 것").isEqualTo(2);
        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).as("걸러진 것").isEmpty();
    }

    @Test
    @DisplayName("배제_시간이_지나면_표시된_수에서도_빠진다")
    void 배제_시간이_지나면_표시된_수에서도_빠진다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }

        assertThat(outliers.ejectedCount(1_000)).isEqualTo(1);
        assertThat(outliers.ejectedCount(1_000 + 배제_시간.toMillis())).isZero();
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
     * <b>배제 전에 나간 요청이 나중에 성공으로 끝나면 배제가 일찍 풀린다.</b>
     * 의도한 동작이다 — 지우는 것은 성공뿐이라는 규칙이 여기까지 온다. 반쯤
     * 고장 난 대가 스스로 배제를 취소하는 진동이 이 자리에서 난다.
     */
    @Test
    @DisplayName("배제_중_늦게_온_성공이_배제를_푼다")
    void 배제_중_늦게_온_성공이_배제를_푼다() {
        InstanceOutliers outliers = 배제기();
        for (int i = 0; i < 3; i++) {
            outliers.failed("가", 1_000);
        }
        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).containsExactly("가");

        outliers.succeeded("가", 1_000);

        assertThat(outliers.ejected(Set.of("가", "나"), 1_000)).isEmpty();
        assertThat(outliers.recoveryRemaining("가", 1_000))
                .as("배제를 끝내되 램프로 넘긴다").isEqualTo(1);
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

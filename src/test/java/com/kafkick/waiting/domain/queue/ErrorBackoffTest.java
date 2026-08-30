package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 오류 경로의 재시도 안내 (F7 · 8.2.5).
 *
 * <p>장애 중 503 을 받은 대기자는 <b>전원이 같은 초에 오류를 받는다.</b> 같은
 * 값을 주면 전원이 같은 초에 돌아오고, 그 파도가 회복을 2차 장애로 만든다.
 * 지터가 가장 필요한 순간이 정확히 여기인데, 정상 경로에만 있었다.
 */
class ErrorBackoffTest {

    /** 기본 간격과 상한. 정책이 바뀌면 이 시험도 같이 움직인다. */
    private static final ErrorBackoff 정책 = ErrorBackoff.defaults();

    /** 흔들림을 뺀 바닥값. 위로만 흔들므로 난수 0 이 그 계단의 시작이다. */
    private long 계단(int streak) {
        return 정책.retryAfterSec(streak, () -> 0.0);
    }

    /**
     * <b>오류 경로의 흔들림이 정상 경로보다 넓다.</b>
     *
     * <p>정상 경로는 사람마다 폴링 시점이 이미 흩어져 있다. 오류 시점은 전원이
     * 같으므로 같은 폭으로는 안 흩어진다.
     */
    @Test
    @DisplayName("오류_경로의_지터가_정상_경로보다_넓다")
    void 오류_경로의_지터가_정상_경로보다_넓다() {
        assertThat(ErrorBackoff.JITTER_RATIO)
                .isGreaterThan(PollIntervalPolicy.NORMAL_JITTER_RATIO);
    }

    /** 난수가 양 끝일 때 실제로 그 폭만큼 벌어져야 한다. 상수만 크면 소용없다. */
    @Test
    @DisplayName("난수가_양_끝이면_폭만큼_벌어진다")
    void 난수가_양_끝이면_폭만큼_벌어진다() {
        long 아래 = 정책.retryAfterSec(4, () -> 0.0);
        long 위 = 정책.retryAfterSec(4, () -> 1.0);

        assertThat(위 - 아래).as("한 칸에 갇히면 흩어지지 않는다").isGreaterThanOrEqualTo(2);
    }

    /** 아래로는 안 흔든다. 흔들면 바닥보다 빨리 불러 폴링 예산이 깨진다. */
    @Test
    @DisplayName("바닥_아래로는_안_내려간다")
    void 바닥_아래로는_안_내려간다() {
        for (double r = 0; r <= 1; r += 0.05) {
            double 난수 = r;
            assertThat(정책.retryAfterSec(1, 30, () -> 난수))
                    .as("난수 %.2f".formatted(난수)).isGreaterThanOrEqualTo(30);
        }
    }

    /**
     * <b>연속 실패에 백오프가 붙는다.</b>
     *
     * <p>장애가 이어지는 동안 같은 간격으로 계속 두드리면, 그 요청이 회복하려는
     * 뒷단의 자리를 계속 차지한다.
     */
    @Test
    @DisplayName("연속_실패에_백오프가_붙는다")
    void 연속_실패에_백오프가_붙는다() {
        assertThat(계단(2)).isGreaterThan(계단(1));
        assertThat(계단(3)).isGreaterThan(계단(2));
        assertThat(계단(4)).isGreaterThan(계단(3));
    }

    /**
     * <b>상한이 있다.</b>
     *
     * <p>없으면 장애가 길어질수록 안내가 무한히 멀어지고, 회복한 뒤에도 한참
     * 아무도 안 돌아온다. 그건 장애가 끝난 뒤의 장애다.
     */
    @Test
    @DisplayName("백오프에_상한이_있다")
    void 백오프에_상한이_있다() {
        assertThat(정책.retryAfterSec(50, () -> 1.0)).isEqualTo(ErrorBackoff.MAX_SEC);
        assertThat(정책.retryAfterSec(Integer.MAX_VALUE, () -> 1.0))
                .isEqualTo(ErrorBackoff.MAX_SEC);
        assertThat(계단(50)).as("계단의 시작은 상한 아래다")
                .isLessThan(ErrorBackoff.MAX_SEC);
    }

    /** 첫 실패도 기본 간격을 받는다. 0 이나 음수가 와도 마찬가지다. */
    @Test
    @DisplayName("첫_실패는_기본_간격이다")
    void 첫_실패는_기본_간격이다() {
        assertThat(계단(1)).isEqualTo(ErrorBackoff.BASE_SEC);
        assertThat(계단(0)).as("아직 안 센 것도 첫 실패로 본다")
                .isEqualTo(ErrorBackoff.BASE_SEC);
        assertThat(계단(-5)).isEqualTo(ErrorBackoff.BASE_SEC);
    }

    /** 0 초는 안 준다. 즉시 재시도는 흩어짐이 없다. */
    @Test
    @DisplayName("영_초는_안_준다")
    void 영_초는_안_준다() {
        for (int streak = 0; streak < 10; streak++) {
            assertThat(정책.retryAfterSec(streak, () -> 0.0))
                    .as("%d 번째 실패".formatted(streak)).isPositive();
        }
    }

    /**
     * <b>값 1만 개의 표준편차가 0.5초 이상이다</b> (G5.7 과 같은 자).
     *
     * <p>폭이 넓어도 반올림이 한 칸으로 흡수하면 흩어지지 않는다. 실제로 흩어진
     * 값이 나오는지를 재는 것이지 상수를 재는 것이 아니다.
     */
    @Test
    @DisplayName("일만_개의_표준편차가_반초를_넘는다")
    void 일만_개의_표준편차가_반초를_넘는다() {
        RandomGenerator 난수 = RandomGenerator.of("L64X128MixRandom");
        double 합 = 0;
        double 제곱합 = 0;
        int 개수 = 10_000;

        // **배선이 실제로 넘기는 바닥으로 잰다.** 바닥 없이 재면 프로덕션이 한
        // 번도 안 내는 값의 분산을 재는 셈이다.
        for (int i = 0; i < 개수; i++) {
            long v = 정책.retryAfterSec(1, 30, 난수::nextDouble);
            합 += v;
            제곱합 += (double) v * v;
        }

        double 평균 = 합 / 개수;
        double 표준편차 = Math.sqrt(제곱합 / 개수 - 평균 * 평균);
        assertThat(표준편차).isGreaterThanOrEqualTo(0.5);
    }

    /** 난수가 범위를 벗어나면 만들 때 막는다. 조용히 통과하면 흩어짐이 없어진다. */
    @Test
    @DisplayName("잘못된_폭은_거절한다")
    void 잘못된_폭은_거절한다() {
        assertThatThrownBy(() -> ErrorBackoff.of(2, 1, 0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("상한");
        assertThatThrownBy(() -> ErrorBackoff.of(0, 30, 0.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ErrorBackoff.of(2, 30, -0.1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ErrorBackoff.of(2, 30, 1.5))
                .as("1 을 넘으면 음수가 나온다")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ErrorBackoff.of(2, 30, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>바닥을 받으면 그보다 빨리 안 부른다.</b>
     *
     * <p>장애 구간이 곧 폴링 예산이 빠듯한 구간이다. 바닥을 무시하면 하필 그때
     * 거절받은 사람만 예산 밖으로 돌아온다.
     */
    @Test
    @DisplayName("바닥보다_빨리_안_부른다")
    void 바닥보다_빨리_안_부른다() {
        assertThat(정책.retryAfterSec(1, 45, () -> 0.0)).isEqualTo(45);
        assertThat(정책.retryAfterSec(1, 0, () -> 0.0)).as("바닥이 없으면 기본 간격")
                .isEqualTo(ErrorBackoff.BASE_SEC);
    }

    /** 바닥이 있어도 상한은 넘지 않는다. 넘으면 회복 뒤에도 아무도 안 돌아온다. */
    @Test
    @DisplayName("바닥이_커도_상한을_안_넘는다")
    void 바닥이_커도_상한을_안_넘는다() {
        assertThat(정책.retryAfterSec(1, 9_999, () -> 0.0)).isEqualTo(ErrorBackoff.MAX_SEC);
    }

    /**
     * <b>상한에서 흔들림이 무너지면 안 된다.</b>
     *
     * <p>흔든 뒤에 자르면 상한 위로 흩어진 값이 전부 상한 하나로 모인다. 그
     * 지점이 바로 장애가 길어진 구간이라, 하필 F7 이 막으려던 파도가 거기서
     * 그대로 다시 생긴다. {@code PollIntervalPolicy} 가 이미 푼 문제다.
     */
    @Test
    @DisplayName("상한_구간에서도_흩어진다")
    void 상한_구간에서도_흩어진다() {
        RandomGenerator 난수 = RandomGenerator.of("L64X128MixRandom");
        Map<Long, Integer> 분포 = new HashMap<>();
        int 개수 = 100_000;

        for (int i = 0; i < 개수; i++) {
            분포.merge(정책.retryAfterSec(20, 난수::nextDouble), 1, Integer::sum);
        }

        int 최빈 = 분포.values().stream().mapToInt(Integer::intValue).max().orElseThrow();
        assertThat((double) 최빈 / 개수).as("한 값에 몰리면 흩어진 것이 아니다")
                .isLessThan(0.10);
    }

    /**
     * <b>바닥이 상한 가까이 와도 안 몰린다.</b>
     *
     * <p>폴링 배수가 커진 구간이 곧 바닥이 높아지는 구간이고, 그때가 장애와
     * 겹친다. 위 끝을 자르는 대신 폭을 줄여야 그 구간에서도 흩어진다.
     */
    @Test
    @DisplayName("바닥이_높아도_흩어진다")
    void 바닥이_높아도_흩어진다() {
        RandomGenerator 난수 = RandomGenerator.of("L64X128MixRandom");
        Map<Long, Integer> 분포 = new HashMap<>();
        int 개수 = 100_000;

        for (int i = 0; i < 개수; i++) {
            분포.merge(정책.retryAfterSec(1, 45, 난수::nextDouble), 1, Integer::sum);
        }

        int 최빈 = 분포.values().stream().mapToInt(Integer::intValue).max().orElseThrow();
        assertThat((double) 최빈 / 개수).isLessThan(0.10);
    }

    /**
     * <b>배선이 실제로 그리는 곡선을 못 박는다.</b>
     *
     * <p>조회 경로는 폴링 예산이 정한 바닥 30초를 함께 넘긴다. 그래서 앞쪽
     * 계단들은 바닥에 가려 안 보이고, 관측되는 상승은 바닥을 넘어선 뒤부터다.
     * 이걸 안 적어 두면 "기본 2초에 배로 는다" 는 문서가 프로덕션에서 한 번도
     * 안 일어나는 동작을 말하게 된다.
     */
    @Test
    @DisplayName("바닥이_있으면_앞_계단은_안_보인다")
    void 바닥이_있으면_앞_계단은_안_보인다() {
        long 바닥 = 30;

        for (int streak = 1; streak <= 4; streak++) {
            assertThat(정책.retryAfterSec(streak, 바닥, () -> 0.0))
                    .as("%d 번째 계단은 바닥에 가린다".formatted(streak)).isEqualTo(바닥);
        }
        assertThat(정책.retryAfterSec(5, 바닥, () -> 0.0))
                .as("다섯째부터 바닥을 넘어선다").isGreaterThan(바닥);
        // 천장이 있으므로 그 위로는 안 는다. 늘면 상한 위로 나가거나 상한 한
        // 점에 몰린다.
        assertThat(정책.retryAfterSec(20, 바닥, () -> 0.0))
                .isEqualTo(정책.retryAfterSec(6, 바닥, () -> 0.0));
    }

    /**
     * <b>큰 기본 간격에서도 안 넘친다.</b>
     *
     * <p>시프트로 키우면 기본 간격이 클 때 열여섯 번 미만에도 넘쳐 음수가 된다.
     * 그러면 상한을 씌우기 전에 값이 이미 뒤집혀, 장애가 길어질수록 오히려
     * 즉시 재시도를 부른다.
     */
    @Test
    @DisplayName("큰_기본_간격에서도_안_넘친다")
    void 큰_기본_간격에서도_안_넘친다() {
        ErrorBackoff 큰_정책 = ErrorBackoff.of(Long.MAX_VALUE / 4, Long.MAX_VALUE / 2, 0.5);

        // **양수인지만 보면 안 잡힌다.** 넘쳐서 음수가 되면 하한 1 로 잘려 1 초가
        // 나가는데 그것도 양수다. 장애가 길어질수록 오히려 즉시 재시도를 부르는
        // 것이 이 결함의 실제 모양이라, 단조성으로 잰다.
        long 직전 = 0;
        for (int streak = 1; streak <= 40; streak++) {
            long 지금 = 큰_정책.retryAfterSec(streak, () -> 0.0);
            assertThat(지금).as("%d 번째 계단이 뒤로 갔다".formatted(streak))
                    .isGreaterThanOrEqualTo(직전);
            직전 = 지금;
        }
    }
}

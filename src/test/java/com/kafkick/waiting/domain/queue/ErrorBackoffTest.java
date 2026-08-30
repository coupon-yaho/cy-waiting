package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    /** 흔들림을 뺀 중앙값. 난수가 0.5 면 흔들림이 0 이다. */
    private long 중앙값(int streak) {
        return 정책.retryAfterSec(streak, () -> 0.5);
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
        long 아래 = 정책.retryAfterSec(1, () -> 0.0);
        long 위 = 정책.retryAfterSec(1, () -> 1.0);

        assertThat(위 - 아래).as("한 칸에 갇히면 흩어지지 않는다").isGreaterThanOrEqualTo(2);
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
        assertThat(중앙값(2)).isGreaterThan(중앙값(1));
        assertThat(중앙값(3)).isGreaterThan(중앙값(2));
        assertThat(중앙값(4)).isGreaterThan(중앙값(3));
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
        assertThat(중앙값(50)).isEqualTo(ErrorBackoff.MAX_SEC);
        assertThat(중앙값(Integer.MAX_VALUE)).isEqualTo(ErrorBackoff.MAX_SEC);
    }

    /** 첫 실패도 기본 간격을 받는다. 0 이나 음수가 와도 마찬가지다. */
    @Test
    @DisplayName("첫_실패는_기본_간격이다")
    void 첫_실패는_기본_간격이다() {
        assertThat(중앙값(1)).isEqualTo(ErrorBackoff.BASE_SEC);
        assertThat(중앙값(0)).as("아직 안 센 것도 첫 실패로 본다")
                .isEqualTo(ErrorBackoff.BASE_SEC);
        assertThat(중앙값(-5)).isEqualTo(ErrorBackoff.BASE_SEC);
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

        for (int i = 0; i < 개수; i++) {
            long v = 정책.retryAfterSec(1, 난수::nextDouble);
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
        assertThat(정책.retryAfterSec(1, 45, () -> 0.5)).isEqualTo(45);
        assertThat(정책.retryAfterSec(1, 0, () -> 0.5)).as("바닥이 없으면 기본 간격")
                .isEqualTo(ErrorBackoff.BASE_SEC);
    }

    /** 바닥이 있어도 상한은 넘지 않는다. 넘으면 회복 뒤에도 아무도 안 돌아온다. */
    @Test
    @DisplayName("바닥이_커도_상한을_안_넘는다")
    void 바닥이_커도_상한을_안_넘는다() {
        assertThat(정책.retryAfterSec(1, 9_999, () -> 0.5)).isEqualTo(ErrorBackoff.MAX_SEC);
    }
}

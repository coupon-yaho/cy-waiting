package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 초 단위 고정 윈도우 리미터.
 *
 * <p>시계를 주입받는다(TS-4). 도메인은 {@code Instant.now()} 를 부르지 않는다 —
 * 부르는 순간 초 경계 동작을 시험할 수 없다.
 */
class SecondWindowLimiterTest {

    @Test
    @DisplayName("상한_안이면_허용하고_넘으면_거부한다")
    void 상한_안이면_허용하고_넘으면_거부한다() {
        SecondWindowLimiter limiter = new SecondWindowLimiter(1000);

        assertThat(limiter.tryAcquire("c1", 100, 10)).isTrue();
        assertThat(limiter.tryAcquire("c1", 1, 10)).isFalse();
    }

    @Test
    @DisplayName("초가_넘어가면_카운터가_리셋된다")
    void 초가_넘어가면_카운터가_리셋된다() {
        SecondWindowLimiter limiter = new SecondWindowLimiter(1000);

        assertThat(limiter.tryAcquire("c1", 100, 10)).isTrue();
        assertThat(limiter.tryAcquire("c1", 1, 10)).isFalse();
        assertThat(limiter.tryAcquire("c1", 100, 11)).isTrue();
    }

    @Test
    @DisplayName("상한이_0이하면_아무것도_통과시키지_않는다")
    void 상한이_0이하면_아무것도_통과시키지_않는다() {
        SecondWindowLimiter limiter = new SecondWindowLimiter(1000);

        assertThat(limiter.tryAcquire("c1", 0, 10)).isFalse();
        assertThat(limiter.tryAcquire("c1", -1, 10)).isFalse();
    }

    @Test
    @DisplayName("키가_다르면_예산도_따로다")
    void 키가_다르면_예산도_따로다() {
        SecondWindowLimiter limiter = new SecondWindowLimiter(1000);

        assertThat(limiter.tryAcquire("c1", 1, 10)).isTrue();
        assertThat(limiter.tryAcquire("c2", 1, 10)).isTrue();
    }

    @Test
    @DisplayName("같은_초에_경로가_바뀌어도_합산_상한을_넘지_않는다")
    void 같은_초에_경로가_바뀌어도_합산_상한을_넘지_않는다() {
        // F4 — 회복 전이 순간 정상 경로와 fail-open 경로가 각자 카운터를 들면
        // 같은 초에 두 상한이 동시에 열려 1.5× 버스트가 나간다.
        // 리미터를 경로별로 나누지 않고 상한만 인자로 받아 막는다.
        SecondWindowLimiter limiter = new SecondWindowLimiter(1000);

        for (int i = 0; i < 60; i++) {
            assertThat(limiter.tryAcquire("c1", 100, 10)).isTrue();
        }
        // 같은 키·같은 초에 더 낮은 상한으로 전환 — 이미 60 을 썼으므로 0 이어야 한다
        assertThat(limiter.tryAcquire("c1", 50, 10)).isFalse();
    }

    @Test
    @DisplayName("윈도우_맵은_상한을_넘지_않는다")
    void 윈도우_맵은_상한을_넘지_않는다() {
        // 쿠폰 ID 는 URL 경로변수라 공격자가 무한히 넣을 수 있다.
        SecondWindowLimiter limiter = new SecondWindowLimiter(1000);

        for (int i = 0; i < 100_000; i++) {
            limiter.tryAcquire("k" + i, 10, 10);
        }

        assertThat(limiter.size()).isLessThanOrEqualTo(1000);
    }

    @Test
    @DisplayName("지난_초의_윈도우는_정리된다")
    void 지난_초의_윈도우는_정리된다() {
        SecondWindowLimiter limiter = new SecondWindowLimiter(1000);

        limiter.tryAcquire("c1", 10, 10);
        limiter.tryAcquire("c2", 10, 11);

        assertThat(limiter.size()).isEqualTo(1);
    }
}

package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.SecondWindowLimiter.AcquireResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 리미터의 경계값.
 *
 * <p>여기 있는 것들은 전부 <b>뮤테이션 테스트가 짚어준 구멍</b>이다. 상한 그 자체,
 * 두 상한이 같은 값일 때, 키 자리가 정확히 찼을 때 — 예시 테스트가 늘 비껴가는
 * 지점이고, 이 리미터에서 이미 한 번 버그가 났던 자리이기도 하다.
 */
class LimiterBoundaryTest {

    private static final String COUPON = "c1";
    private static final String GLOBAL = "__node__";

    @Test
    @DisplayName("상한이_정확히_0이면_한_건도_통과시키지_않는다")
    void 상한이_정확히_0이면_한_건도_통과시키지_않는다() {
        // cap < 0 로 써도 음수 상한은 안 오므로 아무 테스트도 안 깨진다.
        // 0 은 실제로 온다 — credit 0 인 쿠폰이 그렇다.
        assertThat(SecondWindowLimiter.withMaxKeys(10).tryAcquire(COUPON, 0, 0)).isFalse();
    }

    @Test
    @DisplayName("두_예산_획득에서도_상한_0은_거절한다")
    void 두_예산_획득에서도_상한_0은_거절한다() {
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10);

        assertThat(limiter.tryAcquireAll(COUPON, 0, GLOBAL, 100, 0))
                .isEqualTo(AcquireResult.COUPON_EXHAUSTED);
        assertThat(limiter.tryAcquireAll(COUPON, 100, GLOBAL, 0, 0))
                .isEqualTo(AcquireResult.GLOBAL_EXHAUSTED);
    }

    @Test
    @DisplayName("키_자리가_다_차면_새_키는_상한이_남아도_거절한다")
    void 키_자리가_다_차면_새_키는_상한이_남아도_거절한다() {
        // 통과시키면 상한이 무의미해진다. 메모리 상한이 곧 정확성 상한이다.
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(2);

        assertThat(limiter.tryAcquire("a", 100, 0)).isTrue();
        assertThat(limiter.tryAcquire("b", 100, 0)).isTrue();
        assertThat(limiter.tryAcquire("c", 100, 0)).isFalse();

        // 이미 자리를 잡은 키는 계속 쓴다 — 자리가 없다고 기존 줄까지 막지 않는다.
        assertThat(limiter.tryAcquire("a", 100, 0)).isTrue();
        assertThat(limiter.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("두_예산_획득은_남은_자리가_하나면_새_키_둘을_넣지_않는다")
    void 두_예산_획득은_남은_자리가_하나면_새_키_둘을_넣지_않는다() {
        // 하나씩 검사하면 마지막 슬롯을 두 키가 함께 차지해 상한을 넘긴다.
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(3);
        assertThat(limiter.tryAcquire("other1", 100, 0)).isTrue();
        assertThat(limiter.tryAcquire("other2", 100, 0)).isTrue();

        assertThat(limiter.tryAcquireAll(COUPON, 100, GLOBAL, 100, 0))
                .isEqualTo(AcquireResult.COUPON_EXHAUSTED);
        assertThat(limiter.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("두_예산_획득은_남은_자리가_둘이면_새_키_둘을_함께_넣는다")
    void 두_예산_획득은_남은_자리가_둘이면_새_키_둘을_함께_넣는다() {
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(3);
        assertThat(limiter.tryAcquire("other1", 100, 0)).isTrue();

        assertThat(limiter.tryAcquireAll(COUPON, 100, GLOBAL, 100, 0))
                .isEqualTo(AcquireResult.ACQUIRED);
        assertThat(limiter.size()).isEqualTo(3);
    }

    @Test
    @DisplayName("한쪽_키만_새것이면_자리를_하나만_센다")
    void 한쪽_키만_새것이면_자리를_하나만_센다() {
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(2);
        assertThat(limiter.tryAcquire(GLOBAL, 100, 0)).isTrue();

        // 새 키는 쿠폰 하나뿐이다. 둘로 세면 여기서 잘못 거절한다.
        assertThat(limiter.tryAcquireAll(COUPON, 100, GLOBAL, 100, 0))
                .isEqualTo(AcquireResult.ACQUIRED);
    }

    @Test
    @DisplayName("두_키가_같으면_요청_하나가_예산을_하나만_쓴다")
    void 두_키가_같으면_요청_하나가_예산을_하나만_쓴다() {
        // 따로 차감하면 상한의 절반만 통과한다. 반환값만 보면 안 드러나서
        // 통과 건수를 센다.
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10);
        int passed = 0;
        for (int i = 0; i < 20; i++) {
            if (limiter.tryAcquireAll(GLOBAL, 10, GLOBAL, 10, 0) == AcquireResult.ACQUIRED) {
                passed++;
            }
        }

        assertThat(passed).isEqualTo(10);
    }

    @Test
    @DisplayName("두_키가_같고_상한이_같으면_쿠폰_사유로_고갈된다")
    void 두_키가_같고_상한이_같으면_쿠폰_사유로_고갈된다() {
        // 동점의 귀속처를 정해 둔다. 대응이 다르다 — 전역이면 노드를 늘리고,
        // 쿠폰이면 그 쿠폰의 배분을 본다. 헷갈리면 엉뚱한 데를 고친다.
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10);
        assertThat(limiter.tryAcquireAll(GLOBAL, 1, GLOBAL, 1, 0))
                .isEqualTo(AcquireResult.ACQUIRED);

        assertThat(limiter.tryAcquireAll(GLOBAL, 1, GLOBAL, 1, 0))
                .isEqualTo(AcquireResult.COUPON_EXHAUSTED);
    }

    @Test
    @DisplayName("두_키가_같고_전역_상한이_더_작으면_전역_사유로_고갈된다")
    void 두_키가_같고_전역_상한이_더_작으면_전역_사유로_고갈된다() {
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10);
        assertThat(limiter.tryAcquireAll(GLOBAL, 5, GLOBAL, 1, 0))
                .isEqualTo(AcquireResult.ACQUIRED);

        assertThat(limiter.tryAcquireAll(GLOBAL, 5, GLOBAL, 1, 0))
                .isEqualTo(AcquireResult.GLOBAL_EXHAUSTED);
    }

    @Test
    @DisplayName("두_키가_같으면_새_키_자리를_하나만_센다")
    void 두_키가_같으면_새_키_자리를_하나만_센다() {
        // 자리가 이미 다 찼으면 같은 키여도 새로 못 넣는다. 여기서 통과시키면
        // maxKeys 가 상한이 아니게 된다.
        SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(1);
        assertThat(limiter.tryAcquire("other", 100, 0)).isTrue();

        assertThat(limiter.tryAcquireAll(GLOBAL, 100, GLOBAL, 100, 0))
                .isEqualTo(AcquireResult.COUPON_EXHAUSTED);
        assertThat(limiter.size()).isEqualTo(1);
    }
}

package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 드레인이 <b>상한 안에 끝났는지</b>를 남깁니다 (6.4.2 · G6.22).
 *
 * <p>안 남기면 오케스트레이터가 진행 중인 요청째 죽인 판과 곱게 빠진 판이 로그에서
 * 같습니다. 롤링 배포마다 사용자가 오류를 보는데 아무도 그 사실을 못 봅니다.
 */
class DrainOutcomeTest {

    private final List<String> 남긴_것 = new ArrayList<>();

    /** 남은 건수를 세는 자리. 격벽이 이 값을 안다. */
    private final AtomicInteger 남은_요청 = new AtomicInteger();

    private DrainOutcome 지켜본다(Duration 상한) {
        return DrainOutcome.of(남은_요청::get, 상한, millis -> { }, 남긴_것::add);
    }

    /** 다 빠졌으면 그 사실을 남깁니다. 진입만 있고 끝이 없으면 사후에 못 답합니다. */
    @Test
    @DisplayName("다_빠지면_끝났다고_남긴다")
    void 다_빠지면_끝났다고_남긴다() {
        남은_요청.set(0);

        boolean 끝났다 = 지켜본다(Duration.ofSeconds(5)).await();

        assertThat(끝났다).isTrue();
        assertThat(남긴_것).singleElement().asString().contains("드레인 끝");
    }

    /**
     * 상한을 넘기면 <b>몇 건이 남았는지와 함께</b> 남깁니다.
     *
     * <p>그 건수가 곧 강제 종료로 끊길 요청 수입니다. 숫자가 없으면 롤링 배포의
     * 오류가 이것 때문인지 아닌지를 못 가립니다.
     */
    @Test
    @DisplayName("상한을_넘기면_남은_건수와_함께_남긴다")
    void 상한을_넘기면_남은_건수와_함께_남긴다() {
        남은_요청.set(7);

        boolean 끝났다 = 지켜본다(Duration.ofMillis(30)).await();

        assertThat(끝났다).isFalse();
        assertThat(남긴_것).singleElement().asString()
                .contains("드레인 상한 초과").contains("7");
    }

    /** 기다리는 동안 빠지면 그때 끝냅니다. 상한까지 붙들면 배포가 그만큼 느려집니다. */
    @Test
    @DisplayName("기다리는_동안_빠지면_그때_끝낸다")
    void 기다리는_동안_빠지면_그때_끝낸다() {
        남은_요청.set(3);
        DrainOutcome 지켜보기 = DrainOutcome.of(
                () -> 남은_요청.getAndUpdate(n -> Math.max(0, n - 1)),
                Duration.ofSeconds(5), millis -> { }, 남긴_것::add);

        assertThat(지켜보기.await()).isTrue();
    }

    /** 상한이 0 이하면 지켜보는 것이 아니다. 값으로 끄면 그 사실이 안 드러난다. */
    @Test
    @DisplayName("상한이_0이하면_기동을_막는다")
    void 상한이_0이하면_기동을_막는다() {
        assertThat(catchThrowable(() -> 지켜본다(Duration.ZERO)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Throwable catchThrowable(Runnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }
}

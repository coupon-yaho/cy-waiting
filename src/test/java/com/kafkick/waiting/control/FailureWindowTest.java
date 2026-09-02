package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 이어지는 실패를 한 번만 알린다.
 *
 * <p>매 회차 찍으면 정작 조사가 필요한 순간에 로그가 묻히고, 지속 시간만 남기면
 * 한 회차가 실패한 것인지 수백 회차인지 사후에 못 가린다.
 */
class FailureWindowTest {

    private final AtomicLong ticker = new AtomicLong(1_000_000_000L);

    private final FailureWindow window = FailureWindow.of(ticker::get);

    private void 시간을_흘린다(Duration 만큼) {
        ticker.addAndGet(만큼.toNanos());
    }

    @Test
    @DisplayName("구간의_시작만_참이다")
    void 구간의_시작만_참이다() {
        assertThat(window.entered()).isTrue();
        assertThat(window.entered()).isFalse();
        assertThat(window.entered()).isFalse();
    }

    @Test
    @DisplayName("걷히면_지속_시간과_삼킨_시도를_준다")
    void 걷히면_지속_시간과_삼킨_시도를_준다() {
        window.entered();
        window.entered();
        window.entered();
        시간을_흘린다(Duration.ofSeconds(7));

        assertThat(window.exited()).hasValueSatisfying(회복 -> {
            assertThat(회복.elapsedSeconds()).isEqualTo(7);
            assertThat(회복.swallowed()).isEqualTo(3);
        });
    }

    @Test
    @DisplayName("실패가_없었으면_아무것도_안_준다")
    void 실패가_없었으면_아무것도_안_준다() {
        assertThat(window.exited()).isEmpty();
    }

    @Test
    @DisplayName("걷힌_뒤에는_다시_처음부터_센다")
    void 걷힌_뒤에는_다시_처음부터_센다() {
        // 안 비우면 다음 구간이 누적치를 보고한다.
        window.entered();
        window.entered();
        window.exited();

        window.entered();
        시간을_흘린다(Duration.ofSeconds(2));

        assertThat(window.exited()).hasValueSatisfying(회복 -> {
            assertThat(회복.elapsedSeconds()).isEqualTo(2);
            assertThat(회복.swallowed()).isEqualTo(1);
        });
    }
}

package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 연장을 부르는 루프.
 *
 * <p>연장은 리스보다 훨씬 자주 돌아야 하는데, 그 주기를 정하는 자리가 여기다.
 * <b>아무도 안 부르면 리더가 조용히 사라진다</b> — 락은 리스 만료로 풀리고
 * 배분은 멎는데, 예외도 로그도 안 난다.
 */
class LeadershipLoopTest {

    private static final Duration DELAY = Duration.ofMillis(100);

    private final AtomicInteger 연장 = new AtomicInteger();

    private LeadershipLoop loop(VirtualTimeScheduler timer) {
        return LeadershipLoop.of(DELAY, () -> Mono.fromRunnable(연장::incrementAndGet), timer);
    }

    @Test
    @DisplayName("바로_한_번_부르고_주기마다_다시_부른다")
    void 바로_한_번_부르고_주기마다_다시_부른다() {
        // 첫 연장을 미루면 그동안 아무도 리더가 아니라 배분이 안 돈다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        LeadershipLoop loop = loop(timer);

        loop.start();
        assertThat(연장).hasValue(1);

        timer.advanceTimeBy(DELAY.multipliedBy(3));
        assertThat(연장).hasValue(4);

        loop.stop();
    }

    @Test
    @DisplayName("한_판이_터져도_루프는_돈다")
    void 한_판이_터져도_루프는_돈다() {
        // 여기서 멎으면 리스가 만료돼 리더가 없어지고, 그 뒤로 영영 안 돌아온다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        AtomicInteger 호출 = new AtomicInteger();
        LeadershipLoop loop = LeadershipLoop.of(DELAY, () -> 호출.incrementAndGet() == 1
                ? Mono.error(new IllegalStateException("끊겼다"))
                : Mono.fromRunnable(연장::incrementAndGet), timer);

        loop.start();
        timer.advanceTimeBy(DELAY.multipliedBy(3));

        assertThat(연장).hasValueGreaterThanOrEqualTo(1);
        loop.stop();
    }

    @Test
    @DisplayName("멈추면_다시_안_부른다")
    void 멈추면_다시_안_부른다() {
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        LeadershipLoop loop = loop(timer);
        loop.start();

        loop.stop();
        timer.advanceTimeBy(DELAY.multipliedBy(5));

        assertThat(연장).hasValue(1);
    }

    @Test
    @DisplayName("두_번_시작해도_한_줄기만_돈다")
    void 두_번_시작해도_한_줄기만_돈다() {
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        LeadershipLoop loop = loop(timer);

        loop.start();
        loop.start();
        timer.advanceTimeBy(DELAY);

        assertThat(연장).hasValue(2);
        loop.stop();
    }

    @Test
    @DisplayName("주기가_잘못되면_안_뜬다")
    void 주기가_잘못되면_안_뜬다() {
        assertThatThrownBy(() -> LeadershipLoop.of(DELAY.negated(), Mono::empty,
                VirtualTimeScheduler.create()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("renewDelay");
    }
}

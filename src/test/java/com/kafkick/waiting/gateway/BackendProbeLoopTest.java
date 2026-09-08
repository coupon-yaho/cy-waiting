package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 프로브를 주기적으로 돌리는 루프.
 *
 * <p><b>회차가 간격을 넘기는 것이 정상이다.</b> 프로브가 의미를 갖는 유일한 상황이
 * 뒷단이 느린 상황이기 때문이다. 그때 루프가 죽으면 이 장치가 통째로 사라지고,
 * 회복은 다시 폴링 간격에 묶인다.
 */
@Tag("unit")
class BackendProbeLoopTest {

    private static final Duration 간격 = Duration.ofSeconds(1);

    /** 회차마다 지금 도는 수를 세어 최대치를 남긴다. 겹치면 그 값이 2 가 된다. */
    private static final class CountingRound implements Supplier<Mono<Void>> {

        private final AtomicInteger 돈_수 = new AtomicInteger();
        private final AtomicInteger 지금_도는_수 = new AtomicInteger();
        private final AtomicInteger 겹친_최대 = new AtomicInteger();
        private final Duration 걸리는_시간;

        /** 회차 안의 지연도 같은 시계를 타야 가상 시간이 이 루프를 통째로 민다. */
        private final VirtualTimeScheduler 시계;

        private CountingRound(Duration 걸리는_시간, VirtualTimeScheduler 시계) {
            this.걸리는_시간 = 걸리는_시간;
            this.시계 = 시계;
        }

        @Override
        public Mono<Void> get() {
            return Mono.defer(() -> {
                겹친_최대.accumulateAndGet(지금_도는_수.incrementAndGet(), Math::max);
                돈_수.incrementAndGet();
                return 걸리는_시간.isZero() ? Mono.<Void>empty()
                        : Mono.delay(걸리는_시간, 시계).then();
            }).doFinally(signal -> 지금_도는_수.decrementAndGet());
        }
    }

    /**
     * <b>간격을 실제로 지킨다.</b> 지연을 빼면 회차가 더 빨리 늘어 "죽지 않는다" 만
     * 보는 판정은 오히려 더 빨리 초록이 된다 — 그래서 경계로 잰다.
     */
    @Test
    @DisplayName("간격만큼_쉬고_다음을_돈다")
    void 간격만큼_쉬고_다음을_돈다() {
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        CountingRound 회차 = new CountingRound(Duration.ZERO, 가상);
        BackendProbeLoop 루프 = BackendProbeLoop.of(회차, 간격);

        루프.start(가상);
        try {
            assertThat(회차.돈_수).as("구독하면 첫 회차가 바로 돈다").hasValue(1);

            가상.advanceTimeBy(간격.minusMillis(1));
            assertThat(회차.돈_수).as("간격 직전에는 그대로다").hasValue(1);

            가상.advanceTimeBy(Duration.ofMillis(1));
            assertThat(회차.돈_수).as("간격이 지나면 다음이 돈다").hasValue(2);
        } finally {
            루프.stop();
        }
    }

    /**
     * <b>회차가 간격을 넘겨도 계속 돌고, 겹치지 않는다.</b> {@code Flux.interval} 은
     * 이 자리에서 기다리지 않고 넘침으로 스트림을 끝내고, 동시 실행 판으로 바꾸면
     * 프로브가 반쯤 열린 자리를 한꺼번에 먹는다.
     */
    @Test
    @DisplayName("느린_회차가_겹치지도_끊기지도_않는다")
    void 느린_회차가_겹치지도_끊기지도_않는다() {
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        CountingRound 회차 = new CountingRound(간격.multipliedBy(3), 가상);
        BackendProbeLoop 루프 = BackendProbeLoop.of(회차, 간격);

        루프.start(가상);
        try {
            가상.advanceTimeBy(간격.multipliedBy(20));

            assertThat(회차.돈_수.get()).as("넘침으로 안 끊긴다").isGreaterThanOrEqualTo(4);
            assertThat(회차.겹친_최대).as("한 회차가 끝나야 다음이 돈다").hasValue(1);
        } finally {
            루프.stop();
        }
    }

    /** 회차가 터져도 다음이 나간다. 안 그러면 한 번의 실패가 장치를 끝낸다. */
    @Test
    @DisplayName("회차가_터져도_다음이_나간다")
    void 회차가_터져도_다음이_나간다() {
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        AtomicInteger 회차 = new AtomicInteger();
        BackendProbeLoop 루프 = BackendProbeLoop.of(
                () -> Mono.error(new IllegalStateException("터졌다 " + 회차.incrementAndGet())),
                간격);

        루프.start(가상);
        try {
            가상.advanceTimeBy(간격.multipliedBy(3));

            assertThat(회차).hasValue(4);
        } finally {
            루프.stop();
        }
    }

    /** 멈추면 더 안 돈다. 멈춤 실패가 이 클래스의 이름이 약속한 바로 그 사고다. */
    @Test
    @DisplayName("멈추면_더_안_돈다")
    void 멈추면_더_안_돈다() {
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        CountingRound 회차 = new CountingRound(Duration.ZERO, 가상);
        BackendProbeLoop 루프 = BackendProbeLoop.of(회차, 간격);

        루프.start(가상);
        가상.advanceTimeBy(간격.multipliedBy(2));
        int 멈추기_직전 = 회차.돈_수.get();
        루프.stop();

        가상.advanceTimeBy(간격.multipliedBy(5));

        assertThat(회차.돈_수).hasValue(멈추기_직전);
        assertThat(루프.isRunning()).isFalse();
    }

    /** 두 번 켜도 하나만 돈다. 겹치면 반쯤 열린 자리를 두 배로 먹는다. */
    @Test
    @DisplayName("두_번_켜도_하나만_돈다")
    void 두_번_켜도_하나만_돈다() {
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        CountingRound 회차 = new CountingRound(Duration.ZERO, 가상);
        BackendProbeLoop 루프 = BackendProbeLoop.of(회차, 간격);

        루프.start(가상);
        루프.start(가상);
        try {
            가상.advanceTimeBy(간격.multipliedBy(3));

            assertThat(회차.돈_수).as("구독이 둘이면 회차가 두 배다").hasValue(4);
        } finally {
            루프.stop();
        }
    }

    /**
     * <b>끝난 루프는 끝난 것으로 보인다.</b> 안 그러면 {@code isRunning()} 이 계속
     * 참을 돌려주고, 다시 켜려는 호출이 CAS 에 걸려 조용히 아무것도 안 한다.
     */
    @Test
    @DisplayName("루프가_끝나면_다시_켤_수_있다")
    void 루프가_끝나면_다시_켤_수_있다() {
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        AtomicInteger 회차 = new AtomicInteger();
        BackendProbeLoop 루프 = BackendProbeLoop.of(() -> {
            회차.incrementAndGet();
            return Mono.empty();
        }, 간격);

        루프.start(가상);
        루프.stop();

        assertThat(루프.isRunning()).isFalse();
        int 멈춘_뒤 = 회차.get();
        루프.start(가상);
        가상.advanceTimeBy(간격.multipliedBy(2));

        assertThat(회차.get()).as("다시 켜면 다시 돈다").isGreaterThan(멈춘_뒤);
        루프.stop();
    }

    /**
     * <b>첫 회차의 위상을 흩는다.</b> 간격이 결정적이면 노드 둘의 서킷이 같은 초에
     * 표본을 채우고 같은 초에 닫힌다 — 억눌린 줄 두 벌이 함께 나가 회복 봉우리가
     * 노드 수만큼 커진다 (RC4). 자극도 같고 타이머도 같아서 그렇게 된다.
     */
    @Test
    @DisplayName("첫_회차를_간격_안에서_흩는다")
    void 첫_회차를_간격_안에서_흩는다() {
        VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
        AtomicInteger 회차 = new AtomicInteger();
        // 난수 0.5 면 첫 회차가 간격의 절반 뒤다.
        BackendProbeLoop 루프 = BackendProbeLoop.of(
                () -> Mono.fromRunnable(회차::incrementAndGet),
                Duration.ofSeconds(2), () -> 0.5);

        루프.start(시계);
        시계.advanceTimeBy(Duration.ofMillis(900));
        assertThat(회차.get()).as("절반 전에는 안 돈다").isZero();

        시계.advanceTimeBy(Duration.ofMillis(200));
        assertThat(회차.get()).as("절반이 지나면 첫 회차").isEqualTo(1);

        루프.stop();
    }

    /** 난수 0 이면 곧바로 돈다. 위상을 흩는 것이지 늦추는 것이 아니다. */
    @Test
    @DisplayName("난수가_0_이면_곧바로_돈다")
    void 난수가_0_이면_곧바로_돈다() {
        VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
        AtomicInteger 회차 = new AtomicInteger();
        BackendProbeLoop 루프 = BackendProbeLoop.of(
                () -> Mono.fromRunnable(회차::incrementAndGet),
                Duration.ofSeconds(2), () -> 0.0);

        루프.start(시계);
        시계.advanceTimeBy(Duration.ofMillis(1));

        assertThat(회차.get()).isEqualTo(1);
        루프.stop();
    }
}

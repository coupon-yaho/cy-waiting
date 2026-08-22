package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 배분은 리더 한 대만 돈다. 이 루프가 그 판을 만든다.
 *
 * <p>주기를 고정 간격으로 잡으면 레디스가 느려질 때 틱이 큐에 쌓였다가 회복하는
 * 순간 한꺼번에 터진다. 크레딧이 몰려 나가면 뒷단이 그 순간 다시 넘어진다.
 */
class AllocationSchedulerTest {

    private static final Duration TICK = Duration.ofSeconds(1);
    private static final Duration FIRST = Duration.ofSeconds(3);

    private final AtomicBoolean 리더 = new AtomicBoolean(true);
    private final AtomicInteger 배분 = new AtomicInteger();
    private final List<Long> 잰_지연 = new CopyOnWriteArrayList<>();

    private AllocationScheduler scheduler(Scheduler timer) {
        return scheduler(timer, () -> Mono.fromRunnable(배분::incrementAndGet));
    }

    private AllocationScheduler scheduler(Scheduler timer, java.util.function.Supplier<Mono<Void>> 한_판) {
        return AllocationScheduler.of(TICK, FIRST, 리더::get, 한_판, 잰_지연::add, timer);
    }

    @Test
    @DisplayName("리더가_아니면_배분을_안_돈다")
    void 리더가_아니면_배분을_안_돈다() {
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        리더.set(false);
        AllocationScheduler scheduler = scheduler(timer);
        scheduler.start();

        timer.advanceTimeBy(FIRST.plus(TICK.multipliedBy(5)));

        assertThat(배분).hasValue(0);
        scheduler.stop(() -> { });
    }

    @Test
    @DisplayName("첫_판을_미룬다")
    void 첫_판을_미룬다() {
        // 기동 직후에는 가용량 보고가 아직 안 모여 있다. 그때 배분하면 하한으로
        // 나가서, 아무 문제 없는 쿠폰까지 줄을 세운다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        AllocationScheduler scheduler = scheduler(timer);
        scheduler.start();

        timer.advanceTimeBy(FIRST.minusNanos(1));
        assertThat(배분).hasValue(0);

        timer.advanceTimeBy(Duration.ofNanos(1));
        assertThat(배분).hasValue(1);
        scheduler.stop(() -> { });
    }

    @Test
    @DisplayName("한_판이_끝나야_다음_지연이_시작된다")
    void 한_판이_끝나야_다음_지연이_시작된다() {
        // 고정 간격이면 느린 구간에 틱이 쌓였다가 회복하는 순간 한꺼번에 터진다.
        // 한 판이 0.6틱 걸리면 완료 후 지연은 1.6틱마다 도는데, 고정 간격은
        // 그대로 1틱마다 돈다 — 3틱을 흘리면 2번과 3번으로 갈린다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        Duration 한_판_길이 = TICK.multipliedBy(6).dividedBy(10);
        AllocationScheduler scheduler = scheduler(timer, () -> Mono.defer(() -> {
            배분.incrementAndGet();
            return Mono.<Void>empty().delaySubscription(한_판_길이, timer);
        }));
        scheduler.start();

        timer.advanceTimeBy(FIRST.plus(TICK.multipliedBy(3)));

        assertThat(배분).hasValue(2);
        scheduler.stop(() -> { });
    }

    @Test
    @DisplayName("한_판이_터져도_루프는_돈다")
    void 한_판이_터져도_루프는_돈다() {
        // 여기서 멎으면 배분이 영영 안 돈다. 크레딧이 갱신되지 않으면 전 노드가
        // 낡은 값으로 판정하다 결국 fail-open 한다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        AtomicInteger 호출 = new AtomicInteger();
        AllocationScheduler scheduler = scheduler(timer, () -> 호출.incrementAndGet() == 1
                ? Mono.error(new IllegalStateException("끊겼다"))
                : Mono.fromRunnable(배분::incrementAndGet));
        scheduler.start();

        timer.advanceTimeBy(FIRST.plus(TICK.multipliedBy(2)));

        assertThat(배분).hasValueGreaterThanOrEqualTo(1);
        scheduler.stop(() -> { });
    }

    @Test
    @DisplayName("한_판이_멈춰도_다음_판이_돈다")
    void 한_판이_멈춰도_다음_판이_돈다() {
        // 무응답은 오류가 아니라서 오류 처리에 안 걸린다. 상한이 없으면 루프가
        // 조용히 멎고, 멎었다는 신호조차 안 나온다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        AtomicInteger 호출 = new AtomicInteger();
        AllocationScheduler scheduler = scheduler(timer, () -> 호출.incrementAndGet() == 1
                ? Mono.never()
                : Mono.fromRunnable(배분::incrementAndGet));
        scheduler.start();

        timer.advanceTimeBy(FIRST.plus(TICK.multipliedBy(4)));

        assertThat(배분).hasValueGreaterThanOrEqualTo(1);
        scheduler.stop(() -> { });
    }

    @Test
    @DisplayName("멈추면_다음_판이_안_돈다")
    void 멈추면_다음_판이_안_돈다() {
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        AllocationScheduler scheduler = scheduler(timer);
        scheduler.start();
        timer.advanceTimeBy(FIRST);
        assertThat(배분).hasValue(1);

        scheduler.stop(() -> { });
        timer.advanceTimeBy(TICK.multipliedBy(5));

        assertThat(배분).hasValue(1);
    }

    @Test
    @DisplayName("판마다_지연을_잰다")
    void 판마다_지연을_잰다() {
        // 이 값이 임계에 붙으면 스케줄러를 따로 떼야 한다는 신호다. 안 재면
        // 떼야 할 시점을 사고로 알게 된다.
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        AllocationScheduler scheduler = scheduler(timer,
                () -> Mono.<Void>empty().delaySubscription(Duration.ofMillis(300), timer));
        scheduler.start();

        timer.advanceTimeBy(FIRST.plus(TICK));

        assertThat(잰_지연).isNotEmpty().allSatisfy(잰 -> assertThat(잰).isGreaterThan(0L));
        scheduler.stop(() -> { });
    }

    @Test
    @DisplayName("두_번_시작해도_판은_하나만_돈다")
    void 두_번_시작해도_판은_하나만_돈다() {
        VirtualTimeScheduler timer = VirtualTimeScheduler.create();
        AllocationScheduler scheduler = scheduler(timer);
        scheduler.start();
        scheduler.start();

        timer.advanceTimeBy(FIRST);

        assertThat(배분).hasValue(1);
        scheduler.stop(() -> { });
    }

    @Test
    @DisplayName("설정이_잘못되면_안_뜬다")
    void 설정이_잘못되면_안_뜬다() {
        assertThatThrownBy(() -> AllocationScheduler.of(Duration.ZERO, FIRST,
                리더::get, Mono::empty, 잰_지연::add, VirtualTimeScheduler.create()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("tick");
        assertThatThrownBy(() -> AllocationScheduler.of(TICK, FIRST.negated(),
                리더::get, Mono::empty, 잰_지연::add, VirtualTimeScheduler.create()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageStartingWith("firstTickDelay");
    }
}

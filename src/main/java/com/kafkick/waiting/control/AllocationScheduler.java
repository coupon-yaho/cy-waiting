package com.kafkick.waiting.control;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.SignalType;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * 배분의 회차를 만든다. <b>리더 한 대만</b> 돈다. 고정 간격으로 잡으면 레디스가 느려질 때
 * 틱이 쌓였다 회복하는 순간 한꺼번에 터져 뒷단이 다시 넘어지므로, <b>한 회차가 끝난 뒤에
 * 다음 지연을 시작한다.</b>
 */
public final class AllocationScheduler {

    private static final Logger log = LoggerFactory.getLogger(AllocationScheduler.class);

    private final Duration tick;
    private final Duration firstTickDelay;
    private final BooleanSupplier isLeader;
    private final Supplier<Mono<Void>> allocate;
    private final RoundObserver observer;

    /** 회차가 어떻게 끝났는가. 한 통에 섞으면 시한이 회차 시간으로 적힌다. */
    public enum Outcome { OK, ERROR, TIMEOUT }

    /** 회차 하나의 시간과 결과를 받는다. */
    @FunctionalInterface
    public interface RoundObserver {
        void observe(long nanos, Outcome outcome);
    }
    private final Scheduler timer;

    /** 다음 회차를 적어도 이만큼 미룬다. 적용 차례를 회차 안에서 기다리면 틱 시한을 먹는다. */
    private final Supplier<Duration> holdOff;

    /** 리더가 아닐 때 다시 묻는 간격. 틱보다 짧아야 뜻이 있다 — 길면 틱을 쓴다. */
    private static final Duration IDLE_POLL = Duration.ofMillis(100);

    /** 직전 회차가 리더가 아니라 건너뛰었는가. 회차 완료 신호가 다른 스레드에서 읽을 수 있다. */
    private volatile boolean lastSkipped;

    /** 직전 리더 회차가 걸린 시간(나노). 다음 지연을 틱에 맞추는 데 쓴다. */
    private volatile long lastRoundNanos;

    private final AtomicBoolean running = new AtomicBoolean();
    private final FailureWindow failures;
    private volatile Disposable subscription;

    private AllocationScheduler(Duration tick, Duration firstTickDelay, BooleanSupplier isLeader,
            Supplier<Mono<Void>> allocate, RoundObserver observer, Scheduler timer,
            Supplier<Duration> holdOff) {
        if (tick == null || tick.isZero() || tick.isNegative()) {
            throw new IllegalArgumentException("tick 은 양수여야 한다: %s".formatted(tick));
        }
        if (firstTickDelay == null || firstTickDelay.isNegative()) {
            throw new IllegalArgumentException(
                    "firstTickDelay 는 음수일 수 없다: %s".formatted(firstTickDelay));
        }
        this.tick = tick;
        this.firstTickDelay = firstTickDelay;
        this.isLeader = Objects.requireNonNull(isLeader, "isLeader 는 필수다");
        this.allocate = Objects.requireNonNull(allocate, "allocate 는 필수다");
        this.observer = Objects.requireNonNull(observer, "observer 는 필수다");
        this.timer = Objects.requireNonNull(timer, "timer 는 필수다");
        this.holdOff = Objects.requireNonNull(holdOff, "holdOff 는 필수다");
        // 시계를 스케줄러에서 가져온다. 억제 로그의 지속 시간만 실시간을 타면
        // 그 값을 시험이 못 잰다.
        this.failures = FailureWindow.of(() -> timer.now(NANOSECONDS));
    }

    public static AllocationScheduler of(Duration tick, Duration firstTickDelay,
            BooleanSupplier isLeader, Supplier<Mono<Void>> allocate, LongConsumer lagNanos,
            Scheduler timer) {
        return new AllocationScheduler(tick, firstTickDelay, isLeader, allocate,
                timeOnly(lagNanos), timer, () -> Duration.ZERO);
    }

    /** 다음 회차 시작을 {@code holdOff} 만큼은 미룬다. */
    public static AllocationScheduler of(Duration tick, Duration firstTickDelay,
            BooleanSupplier isLeader, Supplier<Mono<Void>> allocate, LongConsumer lagNanos,
            Scheduler timer, Supplier<Duration> holdOff) {
        return new AllocationScheduler(tick, firstTickDelay, isLeader, allocate,
                timeOnly(lagNanos), timer, holdOff);
    }

    /** 결과까지 받는다. 틱 게이트는 성공한 회차만으로 잰다. */
    public static AllocationScheduler observed(Duration tick, Duration firstTickDelay,
            BooleanSupplier isLeader, Supplier<Mono<Void>> allocate, RoundObserver observer,
            Scheduler timer, Supplier<Duration> holdOff) {
        return new AllocationScheduler(tick, firstTickDelay, isLeader, allocate, observer, timer,
                holdOff);
    }

    static RoundObserver timeOnly(LongConsumer lagNanos) {
        Objects.requireNonNull(lagNanos, "lagNanos 는 필수다");
        return (nanos, outcome) -> lagNanos.accept(nanos);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        subscription = loop().subscribe();
    }

    /**
     * <b>콜백을 반드시 부른다.</b> 컨테이너가 이걸 기다리므로, 안 부르면 종료가
     * 그 자리에서 멎고 오케스트레이터가 강제로 끊는다 — 그때는 진행 중인 요청도
     * 함께 끊긴다.
     */
    public void stop(Runnable callback) {
        if (!running.compareAndSet(true, false)) {
            callback.run();
            return;
        }
        Disposable current = subscription;
        if (current != null) {
            current.dispose();
        }
        callback.run();
    }

    private Flux<Void> loop() {
        return Mono.defer(this::round)
                .then()
                .repeatWhen(done -> done.concatMap(ignored -> Mono.delay(nextDelay(), timer)))
                .delaySubscription(firstTickDelay, timer)
                .subscribeOn(timer);
    }

    /**
     * 다음 회차까지 쉴 시간. <b>리더가 아니던 회차 뒤에는 짧게 다시 묻는다</b> — 승계 첫 틱은
     * 울타리 잠금이 끝날 때까지 리더로 안 치는데, 한 틱을 다 쉬면 첫 배분이 그만큼 밀린다.
     * 묻는 것은 로컬 판정이라 비리더가 자주 물어도 레디스를 안 친다.
     */
    /** 시험이 값을 직접 본다 — 0 을 내는 뮤턴트가 루프를 쉼 없이 돌려 메모리를 태우기 전에 죽는다. */
    Duration nextDelay() {
        if (!lastSkipped) {
            // **틱에서 회차가 걸린 만큼 뺀다.** 통째로 쉬면 레디스가 느린 날 회차 시간만큼 주기가 늘어 발행이 준다.
            // 밀린 틱을 만회하지는 않고, 틱을 다 쓴 회차 뒤에도 4분의 1은 쉰다 — 느린 레디스를 쉼 없이 안 두드린다.
            Duration left = tick.minusNanos(lastRoundNanos);
            Duration minimumGap = tick.dividedBy(4);
            Duration gap = left.compareTo(minimumGap) < 0 ? minimumGap : left;
            Duration held = holdOff.get();
            return held.compareTo(gap) > 0 ? held : gap;
        }
        return IDLE_POLL.compareTo(tick) < 0 ? IDLE_POLL : tick;
    }

    /**
     * 실패가 이어지는 동안 경고는 한 번만 찍는다. 초당 한 회차라 매번 찍으면 몇 분짜리
     * 단절에 수백 줄이고, 정작 조사가 필요한 순간에 원인이 묻힌다.
     */
    private void failed(Throwable cause) {
        if (failures.entered()) {
            log.warn("배분 실패 — 다음 회차에 다시 시도한다", cause);
        }
    }

    private void recovered() {
        failures.exited().ifPresent(recovered -> log.info("배분 복귀 — {}초 만에, 그동안 {}회차 실패",
                recovered.elapsedSeconds(), recovered.swallowed()));
    }

    /**
     * 한 회차. <b>리더가 아니면 아무것도 안 한다.</b> 여기서 멎으면 크레딧이 영영 갱신되지
     * 않고 전 노드가 낡은 값으로 판정하다 결국 fail-open 하므로, 터지거나 멈춰도 루프는 돈다.
     */
    private Mono<Void> round() {
        lastSkipped = !isLeader.getAsBoolean();
        if (lastSkipped) {
            return Mono.empty();
        }
        long startedAt = timer.now(NANOSECONDS);
        AtomicReference<Outcome> outcome = new AtomicReference<>(Outcome.OK);
        return allocate.get()
                // 무응답은 오류가 아니라 오류 처리에 안 걸린다. 상한이 없으면
                // 루프가 조용히 멎고, 멎었다는 신호조차 안 나온다.
                .timeout(tick, timer)
                .doOnSuccess(ignored -> recovered())
                .doOnError(e -> outcome.set(
                        e instanceof TimeoutException ? Outcome.TIMEOUT : Outcome.ERROR))
                .doOnError(this::failed)
                .onErrorResume(e -> Mono.empty())
                // 끝나는 신호보다 먼저 적는다. 끝난 뒤에 적으면 반복이 다음 지연을 먼저 계산해 옛 값을 쓴다.
                .doOnTerminate(() -> lastRoundNanos = timer.now(NANOSECONDS) - startedAt)
                // **멈출 때 잘린 회차는 안 적는다.** 그 시간은 회차가 아니라 종료 시각이다.
                .doFinally(signal -> {
                    if (signal != SignalType.CANCEL) {
                        observer.observe(timer.now(NANOSECONDS) - startedAt, outcome.get());
                    }
                });
    }
}

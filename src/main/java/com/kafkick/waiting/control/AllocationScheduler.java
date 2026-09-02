package com.kafkick.waiting.control;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * 배분의 회차를 만든다. <b>리더 한 대만</b> 돈다.
 *
 * <p>주기를 고정 간격으로 잡으면 레디스가 느려질 때 틱이 큐에 쌓였다가 회복하는
 * 순간 한꺼번에 터진다. 크레딧이 몰려 나가면 뒷단이 그 순간 다시 넘어지므로,
 * <b>한 회차가 끝난 뒤에 다음 지연을 시작한다.</b>
 */
public final class AllocationScheduler {

    private static final Logger log = LoggerFactory.getLogger(AllocationScheduler.class);

    private final Duration tick;
    private final Duration firstTickDelay;
    private final BooleanSupplier isLeader;
    private final Supplier<Mono<Void>> allocate;
    private final LongConsumer lagNanos;
    private final Scheduler timer;

    private final AtomicBoolean running = new AtomicBoolean();
    private final FailureWindow failures;
    private volatile Disposable subscription;

    private AllocationScheduler(Duration tick, Duration firstTickDelay, BooleanSupplier isLeader,
            Supplier<Mono<Void>> allocate, LongConsumer lagNanos, Scheduler timer) {
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
        this.lagNanos = Objects.requireNonNull(lagNanos, "lagNanos 는 필수다");
        this.timer = Objects.requireNonNull(timer, "timer 는 필수다");
        // 시계를 스케줄러에서 가져온다. 억제 로그의 지속 시간만 실시간을 타면
        // 그 값을 시험이 못 잰다.
        this.failures = FailureWindow.of(() -> timer.now(NANOSECONDS));
    }

    public static AllocationScheduler of(Duration tick, Duration firstTickDelay,
            BooleanSupplier isLeader, Supplier<Mono<Void>> allocate, LongConsumer lagNanos,
            Scheduler timer) {
        return new AllocationScheduler(tick, firstTickDelay, isLeader, allocate, lagNanos, timer);
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
                .repeatWhen(done -> done.delayElements(tick, timer))
                .delaySubscription(firstTickDelay, timer)
                .subscribeOn(timer);
    }

    /**
     * 한 회차. <b>리더가 아니면 아무것도 안 한다.</b>
     *
     * <p>회차가 터지거나 멈춰도 루프는 돈다. 여기서 멎으면 크레딧이 영영 갱신되지
     * 않고, 전 노드가 낡은 값으로 판정하다 결국 fail-open 한다.
     */
    /**
     * 실패가 이어지는 동안 경고는 한 번만 찍는다.
     *
     * <p>초당 한 회차가라 매번 찍으면 몇 분짜리 단절에 수백 줄이고, 정작 조사가
     * 필요한 순간에 원인이 묻힌다.
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

    private Mono<Void> round() {
        if (!isLeader.getAsBoolean()) {
            return Mono.empty();
        }
        long startedAt = timer.now(NANOSECONDS);
        return allocate.get()
                // 무응답은 오류가 아니라 오류 처리에 안 걸린다. 상한이 없으면
                // 루프가 조용히 멎고, 멎었다는 신호조차 안 나온다.
                .timeout(tick, timer)
                .doOnSuccess(ignored -> recovered())
                .doOnError(this::failed)
                .onErrorResume(e -> Mono.empty())
                .doFinally(signal ->
                        lagNanos.accept(timer.now(NANOSECONDS) - startedAt));
    }
}

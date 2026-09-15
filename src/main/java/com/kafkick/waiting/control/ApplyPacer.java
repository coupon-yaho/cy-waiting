package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * 회차의 적용을 앞 회차 적용에서 한 틱 떨어뜨린다. <b>적용 한 번이 한 틱 몫을 들인다</b> — 회차 시작 간격만 맞추면
 * 끝에 적용한 느린 회차 뒤에 빠른 회차가 곧바로 적용해 두 틱 몫이 1초 안에 들어간다.
 */
public final class ApplyPacer {

    private static final ApplyPacer NONE = new ApplyPacer(Duration.ZERO, null);

    private final Duration spacing;
    private final Scheduler timer;

    /** 적용한 적이 있는가. <b>시각의 부호로 표시하지 않는다</b> — 단조 시계는 음수일 수 있다. */
    private volatile boolean applied;

    /** 마지막 적용 시각(나노). */
    private volatile long lastNanos;

    /** 마지막 적용 회차가 읽는 데 쓴 시간(나노). 대기는 뺀다. */
    private volatile long readNanos;

    private volatile boolean started;

    /** 마지막 회차가 읽기를 시작한 시각(나노). */
    private volatile long startedNanos;

    private ApplyPacer(Duration spacing, Scheduler timer) {
        this.spacing = spacing;
        this.timer = timer;
    }

    public static ApplyPacer of(Duration spacing, Scheduler timer) {
        return new ApplyPacer(Objects.requireNonNull(spacing, "spacing 은 필수다"),
                Objects.requireNonNull(timer, "timer 는 필수다"));
    }

    /** 간격을 안 두는 자리. 시험과 옛 배선이 쓴다. */
    public static ApplyPacer none() {
        return NONE;
    }

    /** 회차가 읽기를 시작했다. */
    public void roundStarted() {
        if (timer != null) {
            startedNanos = timer.now(TimeUnit.NANOSECONDS);
            started = true;
        }
    }

    /**
     * 다음 회차를 이만큼 늦게 시작해야 적용이 차례를 안 기다린다. <b>앞 회차가 읽는 데 쓴 만큼 당긴다</b> — 회차 안에서
     * 기다리면 그 대기가 틱 시한을 먹어 발행이 잘린다.
     */
    public Duration holdOff() {
        if (timer == null || !applied) {
            return Duration.ZERO;
        }
        long left = lastNanos + spacing.toNanos() - readNanos - timer.now(TimeUnit.NANOSECONDS);
        return left > 0 ? Duration.ofNanos(left) : Duration.ZERO;
    }

    /** 차례가 올 때까지 기다리고 이번 적용을 표시한다. 기다리다 취소되면 표시하지 않는다. */
    public Mono<Void> turn() {
        if (timer == null) {
            return Mono.empty();
        }
        return Mono.defer(() -> {
            long now = timer.now(TimeUnit.NANOSECONDS);
            long read = started ? now - startedNanos : 0;
            long left = applied ? spacing.toNanos() - (now - lastNanos) : 0;
            Mono<Void> wait = left > 0 ? Mono.delay(Duration.ofNanos(left), timer).then() : Mono.empty();
            return wait.then(Mono.fromRunnable(() -> {
                lastNanos = timer.now(TimeUnit.NANOSECONDS);
                readNanos = read;
                applied = true;
            }));
        });
    }
}

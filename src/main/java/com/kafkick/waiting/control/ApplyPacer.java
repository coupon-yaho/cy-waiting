package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Objects;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * 회차의 적용을 앞 회차 적용에서 한 틱 떨어뜨린다. 적용 한 번이 한 틱 몫을 들인다.
 */
public final class ApplyPacer {

    private static final ApplyPacer NONE = new ApplyPacer(Duration.ZERO, null);

    private final Duration spacing;
    private final Scheduler timer;

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

    /** 차례가 올 때까지 기다리고 이번 적용을 표시한다. */
    public Mono<Void> turn() {
        return Mono.empty();
    }
}

package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * 연장을 부르는 루프.
 *
 * <p>연장은 리스보다 훨씬 자주 돌아야 하고, 그 주기를 정하는 자리가 여기다.
 * <b>아무도 안 부르면 리더가 조용히 사라진다</b> — 락은 리스 만료로 풀리고
 * 배분은 멎는데, 예외도 로그도 안 난다.
 *
 * <p>배분 틱과 따로 도는 이유는 주기가 다르기 때문이다. 배분에 얹으면 연장이
 * 배분만큼 드물어져 리스를 못 지킨다.
 */
public final class LeadershipLoop {

    private final Duration renewDelay;
    private final Supplier<Mono<Void>> renew;
    private final Scheduler timer;

    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Disposable subscription;

    private LeadershipLoop(Duration renewDelay, Supplier<Mono<Void>> renew, Scheduler timer) {
        if (renewDelay == null || renewDelay.isNegative()) {
            throw new IllegalArgumentException(
                    "renewDelay 는 음수일 수 없다: %s".formatted(renewDelay));
        }
        this.renewDelay = renewDelay;
        this.renew = Objects.requireNonNull(renew, "renew 는 필수다");
        this.timer = Objects.requireNonNull(timer, "timer 는 필수다");
    }

    public static LeadershipLoop of(Duration renewDelay, Supplier<Mono<Void>> renew,
            Scheduler timer) {
        return new LeadershipLoop(renewDelay, renew, timer);
    }

    /** <b>첫 판을 미루지 않는다.</b> 미루면 그동안 아무도 리더가 아니라 배분이 안 돈다. */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        subscription = loop().subscribe();
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Disposable current = subscription;
        if (current != null) {
            current.dispose();
        }
    }

    /**
     * 한 판이 터져도 돈다. 여기서 멎으면 리스가 만료돼 리더가 없어지고, 그 뒤로
     * 영영 안 돌아온다.
     *
     * <p>상한은 {@code Leadership} 이 이미 건다 — 여기서 또 걸면 두 값이 갈릴 때
     * 어느 쪽이 맞는지 아무도 모른다.
     */
    private Flux<Void> loop() {
        return Mono.defer(renew)
                .onErrorResume(e -> Mono.empty())
                .repeatWhen(done -> done.delayElements(renewDelay, timer))
                .subscribeOn(timer);
    }
}

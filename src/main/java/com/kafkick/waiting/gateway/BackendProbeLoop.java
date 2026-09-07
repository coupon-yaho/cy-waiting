package com.kafkick.waiting.gateway;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * 프로브를 주기적으로 돌린다. <b>수명은 스프링에 맡긴다</b> — 직접 만들면 웹 서버가
 * 내려간 뒤 도는지 전에 도는지를 알 수 없다.
 */
public final class BackendProbeLoop implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(BackendProbeLoop.class);

    /**
     * 하트비트 뒤에 서고 웹 서버보다 먼저 내려간다.
     *
     * <p><b>드레이닝 동안에는 안 돈다.</b> 그 구간의 표본은 나가는 요청이 만들고,
     * 어차피 이 노드는 새 유입을 안 받는다 — 회복을 여기서 판정할 이유가 없다.
     */
    private static final int PHASE = Integer.MAX_VALUE - 100;

    private final Supplier<Mono<Void>> round;

    private final Duration interval;

    private final AtomicBoolean running = new AtomicBoolean();

    private volatile Disposable subscription;

    private volatile Scheduler owned;

    private BackendProbeLoop(Supplier<Mono<Void>> round, Duration interval) {
        this.round = Objects.requireNonNull(round, "round 는 필수다");
        this.interval = Objects.requireNonNull(interval, "interval 은 필수다");
    }

    public static BackendProbeLoop of(Supplier<Mono<Void>> round, Duration interval) {
        return new BackendProbeLoop(round, interval);
    }

    /**
     * <b>{@code Flux.interval} 을 안 쓴다.</b> 회차 하나가 간격을 넘기면 그쪽은
     * 기다리지 않고 넘침으로 스트림을 끝낸다 — 프로브가 꼭 필요한 상황(뒷단이
     * 느린 상황)에서 루프가 영구히 죽는다. 한 회차가 끝나야 다음을 예약한다.
     */
    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        // 전용 스케줄러다. 공용 풀을 쓰면 뒷단이 멎었을 때 이 대기가 남의 작업을 민다.
        Scheduler scheduler = Schedulers.newSingle("backend-probe", true);
        owned = scheduler;
        subscription = Mono.defer(round)
                .onErrorResume(error -> {
                    log.warn("합성 프로브 회차가 터졌다 — {}", error.toString());
                    return Mono.empty();
                })
                .then(Mono.delay(interval, scheduler))
                .repeat()
                .subscribe(tick -> { },
                        // 여기 오면 루프가 끝난 것이다. 지표가 0 으로 굳으므로
                        // 밖에서 보이지만, 왜 끝났는지는 이 줄에만 남는다.
                        error -> log.error("합성 프로브 루프가 끝났다 — 다시 뜰 때까지 "
                                + "회복 표본이 줄에서만 나온다", error),
                        () -> log.warn("합성 프로브 루프가 스스로 끝났다"));
        log.info("합성 프로브를 {} 간격으로 돈다", interval);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Disposable current = subscription;
        subscription = null;
        if (current != null) {
            current.dispose();
        }
        Scheduler scheduler = owned;
        owned = null;
        if (scheduler != null) {
            scheduler.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}

package com.kafkick.waiting.gateway;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

    /** 돌기 시작한 시각. 멈출 때 얼마나 돌았는지 같이 남긴다. */
    private final AtomicReference<Instant> startedAt = new AtomicReference<>();

    /** 회차가 터지기 시작한 시각. 구간의 첫 건만 남기는 자물쇠다. */
    private final AtomicReference<Instant> failingSince = new AtomicReference<>();

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
     * <b>{@code Flux.interval} 을 안 쓴다</b> (RX-2). 회차 하나가 간격을 넘기면
     * 그쪽은 기다리지 않고 넘침으로 스트림을 끝낸다 — 프로브가 꼭 필요한 상황에서
     * 루프가 영구히 죽는다.
     */
    @Override
    public void start() {
        // **CAS 를 먼저 한다.** 스케줄러를 먼저 만들면 두 번째 호출이 새 스레드를
        // 만들고 owned 를 덮어쓴 뒤 반환해, 원래 스레드가 영영 산다.
        if (!running.compareAndSet(false, true)) {
            return;
        }
        // 전용 스케줄러다. 공용 풀을 쓰면 뒷단이 멎었을 때 이 대기가 남의 작업을 민다.
        owned = Schedulers.newSingle("backend-probe", true);
        begin(owned);
    }

    /** 스케줄러를 밖에서 준다 — 시험이 가상 시간으로 돌리려면 필요하다. */
    public void start(Scheduler scheduler) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        begin(scheduler);
    }

    private void begin(Scheduler scheduler) {
        startedAt.set(Instant.now());
        log.info("합성 프로브를 {} 간격으로 돈다", interval);
        subscription = Mono.defer(round)
                .onErrorResume(this::swallow)
                .subscribeOn(scheduler)
                .repeatWhen(done -> done.delayElements(interval, scheduler))
                .subscribe(tick -> { },
                        // 여기 오면 루프가 끝난 것이다. 지표가 0 으로 굳으므로
                        // 밖에서 보이지만, 왜 끝났는지는 이 줄에만 남는다.
                        error -> log.error("합성 프로브 루프가 끝났다 — 다시 뜰 때까지 "
                                + "회복 표본이 줄에서만 나온다", error));
    }

    /**
     * <b>구간의 첫 건만 남긴다.</b> 1초 간격이면 하루 8만 줄이고, 그때 정작 봐야
     * 할 것이 묻힌다. 회차가 터져도 다음은 나가므로 결과는 "이번 표본이 빈 것" 이다.
     */
    private Mono<Void> swallow(Throwable error) {
        if (failingSince.compareAndSet(null, Instant.now())) {
            log.warn("합성 프로브 회차가 터졌다 — {}. 이번 표본이 비고 회복 판정이 "
                    + "그만큼 늦는다", error.toString());
        }
        return Mono.empty();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Instant began = startedAt.getAndSet(null);
        log.info("합성 프로브를 멈춘다 — {}초 돌았다",
                began == null ? 0 : Duration.between(began, Instant.now()).toSeconds());
        failingSince.set(null);
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

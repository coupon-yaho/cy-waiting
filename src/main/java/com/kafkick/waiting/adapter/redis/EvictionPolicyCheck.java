package com.kafkick.waiting.adapter.redis;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * 도는 레디스의 축출 정책을 본다 (CY-959). <b>축출은 오류가 안 난다</b> — 줄과 표가 조용히 사라지고
 * 순번 역행과 추월로만 드러난다. 기동을 막지 않고 게이지와 경고로 낸다. CONFIG 가 막힌 곳이 있어서다.
 */
public final class EvictionPolicyCheck implements SmartLifecycle {

    public static final String METRIC = "waiting.redis.eviction.safe";

    public static final int SAFE = 1;

    public static final int UNSAFE = 0;

    public static final int UNKNOWN = -1;

    private static final String REQUIRED = "noeviction";

    private static final Logger log = LoggerFactory.getLogger(EvictionPolicyCheck.class);

    private final Supplier<Mono<String>> policy;

    private final Duration interval;

    private final AtomicInteger state = new AtomicInteger(UNKNOWN);

    private final AtomicBoolean running = new AtomicBoolean();

    /** 못 읽은 구간의 첫 건만 남기는 자물쇠. */
    private final AtomicBoolean unreadable = new AtomicBoolean();

    private volatile Disposable subscription;

    private volatile Scheduler owned;

    private EvictionPolicyCheck(Supplier<Mono<String>> policy, Duration interval,
            MeterRegistry meters) {
        this.policy = Objects.requireNonNull(policy, "policy 는 필수다");
        this.interval = Objects.requireNonNull(interval, "interval 은 필수다");
        Gauge.builder(METRIC, state, AtomicInteger::get)
                .description("축출 정책이 noeviction 이면 1, 다르면 0, 못 봤으면 -1")
                .register(meters);
    }

    public static EvictionPolicyCheck of(Supplier<Mono<String>> policy, Duration interval,
            MeterRegistry meters) {
        return new EvictionPolicyCheck(policy, interval, meters);
    }

    /** 한 번 읽는다. <b>오류를 안 낸다</b> — 못 읽으면 앞서 본 답을 그대로 낸다. */
    public Mono<Integer> check() {
        return Mono.defer(policy)
                .map(this::judged)
                .onErrorResume(this::unread)
                .switchIfEmpty(Mono.defer(() -> unread(null)));
    }

    private int judged(String value) {
        unreadable.set(false);
        int next = REQUIRED.equals(value.strip()) ? SAFE : UNSAFE;
        int previous = state.getAndSet(next);
        if (next == UNSAFE && previous != UNSAFE) {
            log.warn("레디스 축출 정책이 {} 다 — 메모리 한도에서 줄과 표가 조용히 사라진다. "
                    + "CONFIG SET maxmemory-policy noeviction 과 설정 파일을 같이 고친다", value);
        } else if (next == SAFE && previous == UNSAFE) {
            log.info("레디스 축출 정책이 noeviction 으로 돌아왔다");
        }
        return next;
    }

    private Mono<Integer> unread(Throwable error) {
        if (unreadable.compareAndSet(false, true)) {
            log.warn("레디스 축출 정책을 못 읽었다 — {}. CONFIG 가 막혔다면 운영 쪽에서 "
                    + "noeviction 인지 따로 확인한다", error == null ? "빈 답" : error.toString());
        }
        return Mono.just(state.get());
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        owned = Schedulers.newSingle("redis-eviction-check", true);
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
        // check 가 오류를 삼키므로 repeatWhen 앞의 onErrorResume 은 거기 들어 있다 (RX-5).
        subscription = check()
                .subscribeOn(scheduler)
                .repeatWhen(done -> done.delayElements(interval, scheduler))
                .subscribe();
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
}

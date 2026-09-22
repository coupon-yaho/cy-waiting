package com.kafkick.waiting.adapter.redis;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 * 도는 레디스의 축출 정책을 본다. <b>축출은 오류가 안 난다</b> — 줄과 표가 조용히 사라지고
 * 순번 역행과 추월로만 드러난다. 기동을 막지 않고 게이지와 로그로 낸다. CONFIG 가 막힌 곳이 있어서다.
 */
public final class EvictionPolicyCheck implements SmartLifecycle {

    public static final String METRIC = "waiting.redis.eviction.safe";

    public static final int SAFE = 1;

    public static final int UNSAFE = 0;

    public static final int UNKNOWN = -1;

    /** 이만큼 연달아 못 읽으면 본 답을 버린다. 읽기가 막힌 뒤의 변경을 옛 답이 가리지 않게. */
    static final int STALE_AFTER_FAILURES = 3;

    static final Duration READ_TIMEOUT = Duration.ofSeconds(2);

    private static final String KEY = "maxmemory-policy";

    private static final String REQUIRED = "noeviction";

    private static final Logger log = LoggerFactory.getLogger(EvictionPolicyCheck.class);

    private final Supplier<Mono<Properties>> config;

    private final Duration interval;

    private final AtomicInteger state = new AtomicInteger(UNKNOWN);

    private final AtomicInteger failures = new AtomicInteger();

    private final AtomicReference<Instant> unsafeSince = new AtomicReference<>();

    private final AtomicReference<Instant> unreadableSince = new AtomicReference<>();

    private final AtomicBoolean running = new AtomicBoolean();

    private volatile Disposable subscription;

    private volatile Scheduler owned;

    private EvictionPolicyCheck(Supplier<Mono<Properties>> config, Duration interval,
            MeterRegistry meters) {
        this.config = Objects.requireNonNull(config, "config 는 필수다");
        this.interval = Objects.requireNonNull(interval, "interval 은 필수다");
        Gauge.builder(METRIC, state, AtomicInteger::get)
                .description("축출 정책이 noeviction 이면 1, 다르면 0, 못 봤으면 -1")
                .register(meters);
    }

    /** {@code config} 는 CONFIG GET 의 답이다. 클러스터면 노드마다 주소가 붙은 키로 온다. */
    public static EvictionPolicyCheck of(Supplier<Mono<Properties>> config, Duration interval,
            MeterRegistry meters) {
        return new EvictionPolicyCheck(config, interval, meters);
    }

    /** 한 번 읽는다. <b>오류를 안 낸다</b> — 못 읽으면 앞서 본 답을 낸다. */
    public Mono<Integer> check() {
        return Mono.defer(config)
                .timeout(READ_TIMEOUT)
                .mapNotNull(this::policies)
                .map(this::judged)
                .onErrorResume(this::unread)
                .switchIfEmpty(Mono.defer(() -> unread(null)));
    }

    /** 노드마다 하나씩이다. 없으면 null 로 빈 답이 된다. */
    private List<String> policies(Properties found) {
        List<String> values = found.stringPropertyNames().stream()
                .filter(name -> name.equals(KEY) || name.endsWith("." + KEY))
                .map(name -> found.getProperty(name).strip())
                .toList();
        return values.isEmpty() ? null : values;
    }

    private int judged(List<String> values) {
        readable();
        String unsafe = values.stream().filter(v -> !REQUIRED.equals(v)).findFirst().orElse(null);
        int next = unsafe == null ? SAFE : UNSAFE;
        state.set(next);
        if (unsafe != null && unsafeSince.compareAndSet(null, Instant.now())) {
            // 사람이 고쳐야 풀린다.
            log.error("레디스 축출 정책이 위험하다 policy={} — 메모리 한도에서 줄과 표가 조용히 "
                    + "사라진다. CONFIG SET maxmemory-policy noeviction 과 설정 파일을 같이 고친다",
                    unsafe);
        } else if (unsafe == null) {
            Instant began = unsafeSince.getAndSet(null);
            if (began != null) {
                log.info("레디스 축출 정책이 noeviction 으로 돌아왔다 — {}초 만에",
                        Duration.between(began, Instant.now()).toSeconds());
            }
        }
        return next;
    }

    private void readable() {
        failures.set(0);
        Instant began = unreadableSince.getAndSet(null);
        if (began != null) {
            log.info("레디스 축출 정책을 다시 읽었다 — {}초 만에",
                    Duration.between(began, Instant.now()).toSeconds());
        }
    }

    private Mono<Integer> unread(Throwable error) {
        if (unreadableSince.compareAndSet(null, Instant.now())) {
            log.warn("레디스 축출 정책을 못 읽었다 — CONFIG 가 막혔다면 운영 쪽에서 noeviction 인지 "
                    + "따로 확인한다", error == null ? new IllegalStateException("빈 답") : error);
        }
        if (failures.incrementAndGet() >= STALE_AFTER_FAILURES) {
            state.set(UNKNOWN);
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
        // check 가 오류를 삼킨다. 삼키는 자리가 repeatWhen 앞이어야 루프가 산다.
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

package com.kafkick.waiting.control;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.kafkick.waiting.domain.coupon.Tunables;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * 운영자가 적은 값을 <b>배분 회차 밖에서</b> 읽습니다 (P-1).
 *
 * <p>회차 안에서 읽으면 발행이 그 왕복에 매달립니다. 레디스가 500ms 느려지는 것만
 * 으로 틱 예산을 넘겨 스냅샷이 아예 안 나가고, 전 노드가 동시에 낡음으로 빠집니다.
 */
public final class TunablesRefresh {

    private static final Logger log = LoggerFactory.getLogger(TunablesRefresh.class);

    private final Supplier<Mono<String>> read;

    private final LongSupplier nanoTicker;

    private final Duration budget;

    private final Scheduler worker;

    /**
     * 아직 한 번도 못 읽었을 때 쓸 값.
     *
     * <p><b>승계 첫 회차가 위험합니다.</b> 새 리더의 캐시는 비어 있는데, 그 상태로
     * 발행하면 앞 리더가 싣던 값이 통째로 지워집니다 — 운영자가 걸어 둔 조치가
     * 리더 교체만으로 풀립니다.
     */
    private final Supplier<Optional<Tunables>> seed;

    /** 한 번이라도 읽었는가. 안 읽은 것과 읽어서 없는 것은 다르다. */
    private final AtomicBoolean everRead = new AtomicBoolean();

    /** 마지막으로 읽은 시각(나노). 못 읽는 구간을 지표로 내는 근거다. */
    private final AtomicLong lastReadNanos = new AtomicLong();

    /**
     * 마지막으로 읽은 값. <b>비어 있음이 "안 실려 왔다" 는 뜻입니다.</b>
     *
     * <p>기본값으로 채우면 그 값이 각 노드의 기동 설정을 덮어씁니다 — 운영자가
     * 키를 만든 적도 없는데 값이 바뀝니다.
     */
    private final AtomicReference<Optional<Tunables>> applied =
            new AtomicReference<>(Optional.empty());

    /** 실패를 구간으로 묶는다. 틱마다 찍으면 흔들리는 1분에 예순 줄이 쌓인다. */
    private final FailureWindow failures = FailureWindow.create();

    private TunablesRefresh(Supplier<Mono<String>> read, Supplier<Optional<Tunables>> seed,
            Duration budget, Scheduler worker, LongSupplier nanoTicker) {
        if (budget == null || budget.isZero() || budget.isNegative()) {
            throw new IllegalArgumentException("budget 은 양수여야 한다: %s".formatted(budget));
        }
        this.read = Objects.requireNonNull(read, "read 는 필수다");
        this.seed = Objects.requireNonNull(seed, "seed 는 필수다");
        this.budget = budget;
        this.worker = Objects.requireNonNull(worker, "worker 는 필수다");
        this.nanoTicker = Objects.requireNonNull(nanoTicker, "nanoTicker 는 필수다");
        this.lastReadNanos.set(nanoTicker.getAsLong());
    }

    public static TunablesRefresh of(Supplier<Mono<String>> read,
            Supplier<Optional<Tunables>> seed, Duration budget, Scheduler worker) {
        return new TunablesRefresh(read, seed, budget, worker, System::nanoTime);
    }

    /** 시각을 주입받는다. 실시계로 두면 못 읽는 구간을 못 잰다 (TS-4). */
    static TunablesRefresh of(Supplier<Mono<String>> read, Supplier<Optional<Tunables>> seed,
            Duration budget, Scheduler worker, LongSupplier nanoTicker) {
        return new TunablesRefresh(read, seed, budget, worker, nanoTicker);
    }

    /**
     * 한 번 읽습니다.
     *
     * <p><b>실패하면 마지막 값을 그대로 둡니다.</b> 기본값으로 되돌리면 장애가
     * 시작될 때마다 운영자가 걸어 둔 값이 사라지고, 그 원복은 사람이 바꾼 것과
     * 로그로 구별되지 않습니다.
     */
    public Mono<Void> refresh() {
        return read.get()
                .timeout(budget, worker)
                .map(raw -> Optional.of(Tunables.parse(raw)))
                // 키가 없으면 안 실린 것이다. 실패와 달리 마지막 값을 지운다 —
                // 운영자가 키를 지운 것은 "되돌린다" 는 뜻이다.
                .defaultIfEmpty(Optional.empty())
                .doOnNext(this::adopt)
                .onErrorResume(e -> {
                    if (failures.entered()) {
                        log.warn("튜너블을 못 읽는다 — 마지막 값을 그대로 쓴다. {}", e.toString());
                    }
                    return Mono.empty();
                })
                .then();
    }

    /**
     * 지금 실어 보낼 값.
     *
     * <p><b>한 번도 못 읽었으면 재료에 실려 있던 것을 그대로 이어 싣습니다.</b>
     * 승계 첫 회차에 빈 값을 실으면 앞 리더가 걸어 둔 조치가 지워집니다.
     */
    public Optional<Tunables> current() {
        return everRead.get() ? applied.get() : seed.get();
    }

    /**
     * 마지막으로 읽은 지 몇 초 됐는가.
     *
     * <p><b>지표가 이 값을 읽습니다.</b> 게이지가 마지막 값을 계속 내므로, 못 읽고
     * 있다는 사실은 이 값으로만 드러납니다 — 없으면 "5분째 못 받음" 을 못 겁니다.
     */
    public double staleSeconds() {
        return NANOSECONDS.toSeconds(nanoTicker.getAsLong() - lastReadNanos.get());
    }

    /**
     * <b>바뀔 때만 남깁니다.</b> 매 틱 찍으면 조사가 필요한 순간에 묻히고, 장애
     * 뒤에 무엇을 건드렸는지는 이 줄로만 되짚습니다 (6.8.5).
     */
    private void adopt(Optional<Tunables> now) {
        Optional<Tunables> before = everRead.get() ? applied.getAndSet(now) : seed.get();
        applied.set(now);
        everRead.set(true);
        lastReadNanos.set(nanoTicker.getAsLong());
        failures.exited().ifPresent(r -> log.info(
                "튜너블을 다시 읽는다 — {}초 동안 못 읽었다", r.elapsedSeconds()));
        if (before.equals(now)) {
            return;
        }
        log.info("튜너블 변경 — {} → {}", describe(before), describe(now));
    }

    private String describe(Optional<Tunables> value) {
        return value.map(t -> "한산 몫 %s · 걸림 %d초"
                .formatted(t.idleCreditRatio(), t.inFlightSeconds()))
                .orElse("안 걸림(각 노드 기동값)");
    }
}

package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.coupon.Tunables;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * 운영자가 적은 값을 <b>배분 판 밖에서</b> 읽습니다 (P-1).
 *
 * <p>판 안에서 읽으면 발행이 그 왕복에 매달립니다. 레디스가 500ms 느려지는 것만
 * 으로 틱 예산을 넘겨 스냅샷이 아예 안 나가고, 전 노드가 동시에 낡음으로 빠집니다.
 */
public final class TunablesRefresh {

    private static final Logger log = LoggerFactory.getLogger(TunablesRefresh.class);

    private final Supplier<Mono<String>> read;

    private final Duration budget;

    private final Scheduler worker;

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

    private TunablesRefresh(Supplier<Mono<String>> read, Duration budget, Scheduler worker) {
        if (budget == null || budget.isZero() || budget.isNegative()) {
            throw new IllegalArgumentException("budget 은 양수여야 한다: %s".formatted(budget));
        }
        this.read = Objects.requireNonNull(read, "read 는 필수다");
        this.budget = budget;
        this.worker = Objects.requireNonNull(worker, "worker 는 필수다");
    }

    public static TunablesRefresh of(Supplier<Mono<String>> read, Duration budget,
            Scheduler worker) {
        return new TunablesRefresh(read, budget, worker);
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

    /** 지금 실어 보낼 값. 비어 있으면 각 노드가 기동 설정으로 돈다. */
    public Optional<Tunables> current() {
        return applied.get();
    }

    /**
     * <b>바뀔 때만 남깁니다.</b> 매 틱 찍으면 조사가 필요한 순간에 묻히고, 장애
     * 뒤에 무엇을 건드렸는지는 이 줄로만 되짚습니다 (6.8.5).
     */
    private void adopt(Optional<Tunables> now) {
        Optional<Tunables> before = applied.getAndSet(now);
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

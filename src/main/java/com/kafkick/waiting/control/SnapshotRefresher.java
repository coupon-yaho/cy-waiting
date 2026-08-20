package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * 스냅샷을 주기적으로 받아 홀더에 갈아 끼운다.
 *
 * <p><b>실패해도 들고 있던 것을 지우지 않는다.</b> 지우면 레디스가 잠깐 끊긴
 * 사이 판정 재료가 사라진다 — 낡은 값으로 버티는 것과 아무 값 없이 버티는
 * 것은 다르다. 후자는 못 버티는 것이다.
 */
public final class SnapshotRefresher {

    private static final Logger log = LoggerFactory.getLogger(SnapshotRefresher.class);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(400);

    private final SnapshotCodec codec = SnapshotCodec.create();
    private final SnapshotHolder holder;
    private final Supplier<Mono<Map<String, String>>> source;
    private final Duration timeout;

    private SnapshotRefresher(SnapshotHolder holder,
            Supplier<Mono<Map<String, String>>> source, Duration timeout) {
        this.holder = holder;
        this.source = source;
        this.timeout = timeout;
    }

    public static SnapshotRefresher of(SnapshotHolder holder,
            Supplier<Mono<Map<String, String>>> source) {
        return new SnapshotRefresher(holder, source, DEFAULT_TIMEOUT);
    }

    /** 한 판의 상한을 주입한다. 발행 주기보다 짧아야 다음 판이 제때 돈다. */
    public static SnapshotRefresher of(SnapshotHolder holder,
            Supplier<Mono<Map<String, String>>> source, Duration timeout) {
        return new SnapshotRefresher(holder, source, timeout);
    }

    /**
     * 한 판. <b>실패를 흘려보내지 않는다</b> — 흘리면 루프가 그 자리에서 멎고,
     * 한 번 멎으면 영영 멎는다.
     */
    public Mono<Void> 한번() {
        return source.get()
                .timeout(timeout)
                .doOnNext(hash -> holder.replace(codec.decode(hash)))
                .doOnError(e -> log.warn("스냅샷 갱신 실패 — 들고 있던 것을 유지한다", e))
                .onErrorResume(e -> Mono.empty())
                .then();
    }

    /**
     * <b>완료한 뒤에 다음 판을 잡는다.</b> 고정 주기로 쏘면 한 판이 늦을 때
     * 다음 판이 큐에 쌓이고, 회복되는 순간 밀린 것이 한꺼번에 나간다.
     *
     * <p>전용 스케줄러를 쓴다. 기본 풀에 얹으면 요청 처리 뒤에 줄을 선다.
     */
    public Flux<Void> 루프(Duration interval, Scheduler scheduler) {
        return 한번().repeatWhen(done -> done.delayElements(interval, scheduler))
                .subscribeOn(scheduler);
    }

    /** 배경 루프 전용 스레드. 요청 경로와 섞이면 서로를 밀어낸다. */
    public static Scheduler 전용스케줄러() {
        return Schedulers.newSingle("snapshot-refresh", true);
    }
}

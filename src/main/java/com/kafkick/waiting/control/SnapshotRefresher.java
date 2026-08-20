package com.kafkick.waiting.control;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicReference;
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
    /**
     * 언제부터 못 받고 있나. 비어 있으면 정상이다.
     *
     * <p>진입과 해제를 쌍으로 남기고 해제에 지속 시간을 담는다 (LG-2). 매 판
     * 찍으면 20노드 30초 단절에 수백 줄이 쏟아지고(LG-3), 걷힌 시점을 가리키는
     * 줄은 하나도 없다 — 사후에 얼마나 영향받았는지 답할 수 없다.
     */
    private final AtomicReference<Instant> 못받는중 = new AtomicReference<>();
    private final SnapshotHolder holder;
    private final Supplier<Mono<Map<String, String>>> source;
    private final Duration timeout;
    private final Clock clock;

    private SnapshotRefresher(SnapshotHolder holder,
            Supplier<Mono<Map<String, String>>> source, Duration timeout) {
        this.holder = holder;
        this.source = source;
        this.timeout = timeout;
        this.clock = Clock.systemUTC();
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
        return 한번(Schedulers.parallel());
    }

    /**
     * 한 판. <b>타임아웃 타이머도 주어진 스케줄러에서 돈다</b> (RX-3).
     *
     * <p>공용 풀에 두면 부하로 그 풀이 밀릴 때 <b>포기 자체가 늦어져</b>
     * 나이가 임계를 넘는다 — 부하가 가장 높을 때 노드가 로테이션에서 빠진다.
     */
    public Mono<Void> 한번(Scheduler scheduler) {
        // **defer 로 감싼다.** source.get() 이 조립 중에 던지면 아래 오류
        // 처리가 못 받는다 — 루프를 만들기도 전에 터져 그 자리에서 멎는다.
        return Mono.defer(source)
                .timeout(timeout, scheduler)
                // **발행된 것만 받는다.** 빈 해시는 장애가 아니라 흔한 상태라
                // 성공 응답으로 온다 — 그대로 받으면 들고 있던 것이 지워지고
                // 전 쿠폰이 매진으로 보인다.
                // 발행 표시가 없으면 버리되 **조용히 버리지 않는다.** 필터는
                // 오류가 아니라서 아무 흔적을 안 남기는데, 스케줄러 판이 그
                // 필드를 아직 안 쓰면 전 노드가 영영 갱신을 못 한다.
                .map(codec::decode)
                // **받아들일 수 있는 것만 받는다.** 발행 표시가 없으면 스케줄러가
                // 낸 것이 아니고, 쿠폰을 하나도 못 읽었으면 형식이 갈린 것이다 —
                // 둘 다 그대로 받으면 홀더가 비고 전 쿠폰이 매진으로 보인다.
                //
                // 조용히 버리지 않는다. 필터는 오류가 아니라 흔적을 안 남기는데,
                // 스케줄러 판이 갈리면 전 노드가 영영 갱신을 못 한다.
                .doOnNext(snapshot -> {
                    if (!받아들일_수_있나(snapshot)) {
                        진입("받아들일 수 없는 스냅샷 — 발행={} 쿠폰={}",
                                !snapshot.publishedAt().equals(Instant.EPOCH),
                                snapshot.coupons().size());
                    }
                })
                .filter(this::받아들일_수_있나)
                .doOnNext(snapshot -> {
                    holder.replace(snapshot);
                    해제();
                })
                .doOnError(e -> 진입("스냅샷 갱신 실패 — 들고 있던 것을 유지한다: {}",
                        e.toString()))
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
        // **defer 로 감싼다.** 안 감싸면 한번() 이 여기서 한 번만 평가되고,
        // repeatWhen 은 그렇게 만들어진 같은 Mono 를 다시 구독한다 — 매 판
        // 새로 읽으라는 Supplier 의 계약이 깨진다.
        return Mono.defer(() -> 한번(scheduler))
                .repeatWhen(done -> done.delayElements(interval, scheduler))
                .subscribeOn(scheduler);
    }

    /** 못 받기 시작한 순간에만 남긴다. 그 뒤로는 조용하다 (LG-3). */
    private void 진입(String message, Object... args) {
        if (못받는중.compareAndSet(null, clock.instant())) {
            log.warn(message, args);
        }
    }

    /** 걷힌 순간에 지속 시간과 함께 남긴다 (LG-2). */
    private void 해제() {
        Instant 시작 = 못받는중.getAndSet(null);
        if (시작 != null) {
            log.info("스냅샷 갱신 복귀 — {}초 만에", Duration.between(시작, clock.instant())
                    .toSeconds());
        }
    }

    /**
     * 발행 표시가 있고 쿠폰을 하나 이상 읽었는가.
     *
     * <p>빈 해시는 장애가 아니라 흔한 상태고(복제본 승격·키 만료·리더 재선출),
     * 값 형식이 갈리면 발행 표시는 멀쩡한데 쿠폰만 전부 빠진다. 둘 다 정상
     * 응답으로 오므로 오류 경로로는 못 막는다.
     */
    private boolean 받아들일_수_있나(GatewaySnapshot snapshot) {
        return !snapshot.publishedAt().equals(Instant.EPOCH) && !snapshot.coupons().isEmpty();
    }

    /** 배경 루프 전용 스레드. 요청 경로와 섞이면 서로를 밀어낸다. */
    public static Scheduler 전용스케줄러() {
        return Schedulers.newSingle("snapshot-refresh", true);
    }
}

package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * 배분은 <b>리더 한 대만</b> 돈다.
 *
 * <p>모르는 것을 "리더다" 로 답하면 전 노드가 동시에 배분을 돌려 크레딧이 노드
 * 수만큼 부푼다. 이미 나간 통과는 못 물리므로 <b>모르면 아니라고 답한다.</b>
 */
public final class Leadership {

    private static final Logger log = LoggerFactory.getLogger(Leadership.class);

    private final String ownerId;
    private final Duration lease;
    private final Supplier<Mono<Boolean>> acquire;
    private final Supplier<Mono<Void>> release;

    private final AtomicBoolean leader = new AtomicBoolean();

    private Leadership(String ownerId, Duration lease,
            Supplier<Mono<Boolean>> acquire, Supplier<Mono<Void>> release) {
        if (ownerId == null || ownerId.isBlank()) {
            throw new IllegalArgumentException("ownerId 는 비어 있을 수 없다");
        }
        if (lease.isZero() || lease.isNegative()) {
            throw new IllegalArgumentException("lease 는 양수여야 한다: %s".formatted(lease));
        }
        this.ownerId = ownerId;
        this.lease = lease;
        this.acquire = acquire;
        this.release = release;
    }

    public static Leadership of(String ownerId, Duration lease,
            Supplier<Mono<Boolean>> acquire, Supplier<Mono<Void>> release) {
        return new Leadership(ownerId, lease, acquire, release);
    }

    /**
     * 기동마다 새로 만든다.
     *
     * <p>고정 ID 를 쓰면 재기동한 자신을 이전 소유자로 오인해, 죽기 전에 잡아 둔
     * 락을 새 프로세스가 자기 것으로 알고 연장한다.
     */
    public static String newOwnerId() {
        return UUID.randomUUID().toString();
    }

    public String ownerId() {
        return ownerId;
    }

    public Duration lease() {
        return lease;
    }

    /** 이 노드가 지금 리더인가. 요청 경로가 아니라 배분 틱이 묻는다. */
    public boolean isLeader() {
        return leader.get();
    }

    /**
     * 락을 잡거나 연장한다. <b>실패는 "리더가 아니다" 로 답한다.</b>
     *
     * <p>한 번 잡았다고 계속 리더로 두면, 레디스가 끊긴 사이 리스가 만료돼 다른
     * 노드가 잡았는데도 둘 다 리더라고 믿는다.
     */
    public Mono<Void> renew() {
        return Mono.defer(acquire)
                .doOnError(e -> log.warn("리더 확인 실패 — 리더가 아닌 것으로 본다: {}", e.toString()))
                .onErrorReturn(false)
                // 빈 응답은 오류가 아니라 조용한 실패다. 그대로 두면 doOnNext 가
                // 안 돌아 직전 상태가 남는다.
                .defaultIfEmpty(false)
                .doOnNext(this::transition)
                .then();
    }

    /**
     * 락을 놓는다. <b>실패해도 리더에서 내려온다.</b>
     *
     * <p>못 지웠으면 리스 만료로 풀린다. 여기서 리더로 남으면 다음 리더와 겹치는
     * 구간이 리스가 아니라 영영이 된다.
     */
    public Mono<Void> release() {
        if (!leader.get()) {
            return Mono.empty();
        }
        return Mono.defer(release)
                .doOnError(e -> log.warn("리더 해제 실패 — 리스 만료로 풀린다: {}", e.toString()))
                .onErrorResume(e -> Mono.empty())
                .doFinally(signal -> transition(false))
                .then();
    }

    private void transition(boolean now) {
        boolean was = leader.getAndSet(now);
        if (was == now) {
            return;
        }
        if (now) {
            log.info("리더가 됐다 — owner={} lease={}", ownerId, lease);
        } else {
            log.info("리더에서 내려왔다 — owner={}", ownerId);
        }
    }
}

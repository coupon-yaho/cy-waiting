package com.kafkick.waiting.control;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * 배분은 <b>리더 한 대만</b> 돈다.
 *
 * <p>모르는 것을 "리더다" 로 답하면 전 노드가 동시에 배분을 돌려 크레딧이 노드
 * 수만큼 부푼다. 이미 나간 통과는 못 물리므로 <b>모르면 아니라고 답한다.</b>
 *
 * <p><b>다만 "모른다" 를 즉시 하야로 옮기지는 않는다.</b> 로컬만 내려가고 락은
 * 여전히 이 노드 것이라, 남은 리스 동안 아무도 리더가 못 된다 — 안전은 안 늘고
 * 배분 공백만 생긴다. 그래서 두 가지를 가른다.
 *
 * <table border="1">
 * <caption>실패의 두 종류</caption>
 * <tr><th>무엇</th><th>어떻게</th><th>왜</th></tr>
 * <tr><td><b>사실</b> — 남이 쥐고 있다</td><td>즉시 내려온다</td>
 *     <td>확실히 아니므로 기다릴 이유가 없다</td></tr>
 * <tr><td><b>모른다</b> — 오류·빈 응답·멈춤·취소</td><td>리스가 판단한다</td>
 *     <td>아직 내 락일 수 있다. 리스가 이미 상한이다</td></tr>
 * </table>
 *
 * <p>그래서 {@link #isLeader()} 는 <b>나이가 있는 값</b>이다. 나이 없는 참을 들고
 * 있으면 STW 3초가 리스 2초를 넘길 때 깨어난 노드가 묵은 참을 답한다 — 락은
 * 교체됐는데 인식이 안 교체된 상태, 곧 리더가 둘이다.
 *
 * <p>덤으로 <b>연장 주기 계약이 런타임에 자기집행된다.</b> 자주 안 부르면 그냥
 * 리더가 아니게 된다. 주석이나 문서는 그걸 못 막는다.
 */
public final class Leadership {

    private static final Logger log = LoggerFactory.getLogger(Leadership.class);

    /** 계획서의 {@code renew = lease / 4}. 한 판이 이보다 길면 세 번 놓치기 전에 리스가 끝난다. */
    private static final int ATTEMPTS_PER_LEASE = 4;

    /** {@code CLOSED} 는 종단이다. 되살아나면 다음 리더와 겹친다. */
    private enum State { FOLLOWER, LEADER, CLOSED }

    private final String ownerId;
    private final Duration lease;
    private final Duration attemptTimeout;
    private final Supplier<Mono<LeaderLock>> acquire;
    private final Supplier<Mono<Void>> release;

    /**
     * <b>{@code nanoTime} 이다.</b> 벽시계는 스큐와 역행이 시나리오에 있고(C11·C12),
     * 리스 판정이 거기 걸리면 시계가 튈 때 리더가 둘이 된다.
     */
    private final LongSupplier ticker;

    private final AtomicReference<State> state = new AtomicReference<>(State.FOLLOWER);
    private final AtomicReference<Long> failingSince = new AtomicReference<>();

    /**
     * 마지막으로 락이 내 것이라고 <b>확인된</b> 시각.
     *
     * <p>응답이 온 시각이 아니라 <b>물으러 간 시각</b>을 담는다. 서버가 리스를 다시
     * 건 것은 왕복 중 어느 시점이라, 응답 시각으로 재면 왕복 시간만큼 남은 리스를
     * 과대평가한다. 짧게 보는 쪽이 안전한 방향이다.
     */
    private volatile long confirmedAt;

    private volatile long leaderSince;

    private Leadership(String ownerId, Duration lease, Duration attemptTimeout,
            Supplier<Mono<LeaderLock>> acquire, Supplier<Mono<Void>> release, LongSupplier ticker) {
        if (ownerId == null || ownerId.isBlank()) {
            throw new IllegalArgumentException("ownerId 는 비어 있을 수 없다");
        }
        if (lease.isZero() || lease.isNegative()) {
            throw new IllegalArgumentException("lease 는 양수여야 한다: %s".formatted(lease));
        }
        if (attemptTimeout.isZero() || attemptTimeout.isNegative()) {
            throw new IllegalArgumentException(
                    "attemptTimeout 은 양수여야 한다: %s".formatted(attemptTimeout));
        }
        // 어긋나면 안 뜨게 한다. 주석으로 적어 두면 값을 바꾸는 사람이 안 읽고,
        // 배분이 멎는 사고로 배운다.
        if (attemptTimeout.multipliedBy(ATTEMPTS_PER_LEASE).compareTo(lease) > 0) {
            throw new IllegalArgumentException(
                    "한 판이 리스의 1/%d 을 넘으면 연장을 놓치기 전에 리스가 끝난다: attemptTimeout=%s lease=%s"
                            .formatted(ATTEMPTS_PER_LEASE, attemptTimeout, lease));
        }
        this.ownerId = ownerId;
        this.lease = lease;
        this.attemptTimeout = attemptTimeout;
        this.acquire = Objects.requireNonNull(acquire, "acquire 는 필수다");
        this.release = Objects.requireNonNull(release, "release 는 필수다");
        this.ticker = Objects.requireNonNull(ticker, "ticker 는 필수다");
    }

    public static Leadership of(String ownerId, Duration lease, Duration attemptTimeout,
            Supplier<Mono<LeaderLock>> acquire, Supplier<Mono<Void>> release) {
        return new Leadership(ownerId, lease, attemptTimeout, acquire, release, System::nanoTime);
    }

    static Leadership of(String ownerId, Duration lease, Duration attemptTimeout,
            Supplier<Mono<LeaderLock>> acquire, Supplier<Mono<Void>> release, LongSupplier ticker) {
        return new Leadership(ownerId, lease, attemptTimeout, acquire, release, ticker);
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

    /**
     * 이 노드가 지금 리더인가. 요청 경로가 아니라 배분 틱이 묻는다.
     *
     * <p><b>확인된 지 리스가 지났으면 아니다.</b> 값이 늙는 경로 — 멈춤, 취소,
     * STW, 루프 정지 — 는 저마다 다른 모양이라 하나씩 막을 수 없다. 나이로 재면
     * 전부 한 자리에서 접힌다.
     */
    public boolean isLeader() {
        return state.get() == State.LEADER && ticker.getAsLong() - confirmedAt < lease.toNanos();
    }

    /**
     * 락을 잡거나 연장한다.
     *
     * <p>모르는 채로 끝난 판은 아무것도 안 바꾼다 — {@code confirmedAt} 이 안
     * 움직이므로 리스가 지나면 저절로 내려온다.
     *
     * <p>RULE-EXCEPTION(RX-6): 갱신 실패 시 앞 값을 유지하라는 규칙과 방향이 다르다.
     * 낡은 판정 재료로 판정하는 것은 유계지만 리더가 둘인 것은 유계가 아니다.
     * 다만 "즉시 버린다" 도 아니다 — 리스가 그 유계다.
     */
    public Mono<Void> renew() {
        return Mono.defer(() -> {
            if (state.get() == State.CLOSED) {
                return Mono.<LeaderLock>empty();
            }
            long startedAt = ticker.getAsLong();
            return acquire.get()
                    // 멈춤은 오류가 아니라서 오류 처리에 안 걸린다. 이게 없으면
                    // 루프가 조용히 멎고 leader 가 참으로 얼어붙는다.
                    .timeout(attemptTimeout)
                    .doOnNext(lock -> observed(lock, startedAt))
                    .doOnError(this::enterFailing)
                    .onErrorResume(e -> Mono.empty());
        }).doFinally(signal -> demoteIfExpired()).then();
    }

    /**
     * 락을 놓고 <b>종료한다.</b>
     *
     * <p>표시를 먼저 하는 것이 핵심이다. 락만 지우고 종료 표시를 안 하면, 종료 중
     * 스케줄러가 한 틱만 더 돌아도 방금 비운 락을 <b>죽는 노드가 다시 잡는다.</b>
     * 그러면 다음 리더가 리스 만료를 기다리게 되어 해제한 이득이 사라진다.
     */
    public Mono<Void> release() {
        return Mono.defer(() -> {
            if (state.getAndSet(State.CLOSED) != State.LEADER) {
                return Mono.<Void>empty();
            }
            long heldFor = ticker.getAsLong() - leaderSince;
            return release.get()
                    .timeout(attemptTimeout)
                    .doOnError(e -> log.warn("리더 해제 실패 — 리스 만료로 풀린다: {}", e.toString()))
                    .onErrorResume(e -> Mono.empty())
                    // 취소돼도 내려온다. 완료 신호에만 기대면 종료 중에 리더로
                    // 남아, 다음 리더와 겹치는 구간이 리스가 아니라 영영이 된다.
                    .doFinally(signal -> log.info("리더에서 내려왔다 — {}초 동안 리더였다, owner={}",
                            NANOSECONDS.toSeconds(heldFor), ownerId));
        }).then();
    }

    private void observed(LeaderLock lock, long startedAt) {
        exitFailing();
        if (lock.acquired()) {
            confirmedAt = startedAt;
            if (state.compareAndSet(State.FOLLOWER, State.LEADER)) {
                leaderSince = startedAt;
                log.info("리더가 됐다 — owner={}, lease={}초", ownerId, lease.toSeconds());
            }
            return;
        }
        // 사실을 알았다 — 리스를 기다릴 이유가 없다.
        if (state.compareAndSet(State.LEADER, State.FOLLOWER)) {
            log.info("리더를 잃었다 — {}초 동안 리더였다, 지금 소유자는 {}, owner={}",
                    heldSeconds(), lock.owner(), ownerId);
        }
    }

    /** 확인 없이 리스가 지났다. {@link #isLeader()} 가 이미 거짓이고, 여기서는 그걸 기록한다. */
    private void demoteIfExpired() {
        if (state.get() != State.LEADER || ticker.getAsLong() - confirmedAt < lease.toNanos()) {
            return;
        }
        if (state.compareAndSet(State.LEADER, State.FOLLOWER)) {
            log.warn("확인 없이 리스가 지나 리더에서 내려왔다 — {}초 동안 리더였다, owner={}",
                    heldSeconds(), ownerId);
        }
    }

    private long heldSeconds() {
        return NANOSECONDS.toSeconds(ticker.getAsLong() - leaderSince);
    }

    /**
     * 실패가 이어지는 동안 경고는 한 번만 찍는다.
     *
     * <p>연장은 리스의 1/4 마다 돈다. 매 판 찍으면 5분 단절에 노드마다 수백 줄이고,
     * 여러 노드가 동시에 겪는 일이라 그만큼 곱해진다.
     */
    private void enterFailing(Throwable cause) {
        if (failingSince.compareAndSet(null, ticker.getAsLong())) {
            log.warn("리더 확인 실패 — 리스가 남은 동안은 리더로 둔다, owner={}: {}",
                    ownerId, cause.toString());
        }
    }

    private void exitFailing() {
        Long since = failingSince.getAndSet(null);
        if (since != null) {
            log.info("리더 확인 복구 — {}초 만에, owner={}",
                    NANOSECONDS.toSeconds(ticker.getAsLong() - since), ownerId);
        }
    }
}

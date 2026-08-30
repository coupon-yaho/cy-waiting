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
 * <p>실패를 둘로 가른다. <b>사실</b>("남이 쥐고 있다")은 즉시 내려오고,
 * <b>모름</b>(오류·빈 응답·멈춤·취소)은 리스가 판단한다 — 로컬만 내려가도 락은
 * 여전히 이 노드 것이라, 즉시 하야하면 남은 리스 동안 아무도 리더가 못 된다.
 *
 * @see <a href="../../../../../../../ai/journal/2026/08/AIJ-0042-leadership-lease.md">AIJ-0042</a>
 */
public final class Leadership {

    private static final Logger log = LoggerFactory.getLogger(Leadership.class);

    /**
     * 마지막 성공부터 회복하는 판이 끝날 때까지 들어가는 <b>시도의 수</b>.
     *
     * <p>실제 예산은 {@code 4 × (시도 + 지연) + 시도 ≤ lease} 인데 지연은 여기서
     * 모른다. 지연이 0 이상이므로 <b>{@code 5 × 시도 ≤ lease} 는 반드시 필요하다</b> —
     * 이것만으로 예산을 닫지는 못하지만, 넘으면 확실히 깨진다.
     */
    private static final int ATTEMPTS_PER_LEASE = 5;

    /** {@code CLOSED} 는 종단이다. 되살아나면 다음 리더와 겹친다. */
    private enum State { FOLLOWER, LEADER, CLOSED }

    /**
     * 셋을 <b>한 덩어리로</b> 든다. 따로 두면 상태를 공개한 뒤에 시각을 쓰게 되어,
     * 그 사이에 읽는 쪽이 0 을 본다 — "3600초 동안 리더였다" 가 그렇게 나온다.
     *
     * @param confirmedAt 확인된 시각. 응답이 아니라 <b>물으러 간</b> 시각이다
     */
    private record Standing(State state, long confirmedAt, long leaderSince, long fence) {

        Standing closed() {
            return new Standing(State.CLOSED, confirmedAt, leaderSince, fence);
        }
    }

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

    private final AtomicReference<Standing> standing =
            new AtomicReference<>(new Standing(State.FOLLOWER, 0, 0, 0));
    /**
     * 억제 구간. {@code null} 이면 지금은 실패 중이 아니다.
     *
     * <p>시작 시각과 삼킨 판 수를 따로 두면 비우는 것과 읽는 것 사이가 벌어져,
     * 그때 들어온 실패가 이번 회복 로그에도 다음에도 안 들어간다.
     */
    private record Failing(long since, int swallowed) {
    }

    private final AtomicReference<Failing> failing = new AtomicReference<>();

    private Leadership(String ownerId, Duration lease, Duration attemptTimeout,
            Supplier<Mono<LeaderLock>> acquire, Supplier<Mono<Void>> release, LongSupplier ticker) {
        if (ownerId == null || ownerId.isBlank()) {
            throw new IllegalArgumentException("ownerId 는 비어 있을 수 없다");
        }
        // 이게 없으면 아래에서 이름 없는 NPE 가 난다. 기동 실패의 원인을 읽는
        // 난이도가 어느 인자를 빠뜨렸느냐에 따라 달라진다.
        Objects.requireNonNull(lease, "lease 는 필수다");
        Objects.requireNonNull(attemptTimeout, "attemptTimeout 은 필수다");
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
                    "attemptTimeout 이 리스의 1/%d 을 넘으면 회복하는 판 전에 리스가 끝난다: attemptTimeout=%s lease=%s"
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
    /**
     * 내 판 번호. <b>리더가 아니면 0 이다</b> (CY-766).
     *
     * <p>되돌릴 수 없는 쓰기가 이 번호를 들고 나간다. 0 이면 줄 옆의 울타리가
     * 전부 거절한다 — 안 지우는 쪽이라 안전한 방향이다.
     */
    public long fence() {
        return isLeader() ? standing.get().fence() : 0;
    }

    public boolean isLeader() {
        Standing now = standing.get();
        if (now.state() != State.LEADER) {
            return false;
        }
        if (!expired(now)) {
            return true;
        }
        // **강등을 여기서 한다.** 연장 루프의 끝에 달면 루프가 멎었을 때 안 돌고,
        // 하필 그때가 이 기록이 유일한 신호인 순간이다. 배분 틱은 매초 묻는다.
        // 판 번호도 같이 버린다 — 리스가 지난 노드의 번호는 이제 옛 판이다.
        if (standing.compareAndSet(now, new Standing(State.FOLLOWER,
                now.confirmedAt(), now.leaderSince(), 0))) {
            log.warn("확인 없이 리스가 지나 리더에서 내려왔다 — {}초 동안 리더였다, owner={}",
                    heldSeconds(now), ownerId);
        }
        return false;
    }

    private boolean expired(Standing now) {
        return ticker.getAsLong() - now.confirmedAt() >= lease.toNanos();
    }

    /**
     * 락을 잡거나 연장한다. 모르는 채로 끝난 판은 아무것도 안 바꾼다 —
     * {@code confirmedAt} 이 안 움직이므로 리스가 지나면 저절로 내려온다.
     */
    // RULE-EXCEPTION(RX-6): 판정 재료를 유지하라는 규칙과 방향이 다르다. 낡은
    // 재료로 판정하는 것은 유계지만 리더가 둘인 것은 유계가 아니다. 다만
    // "즉시 버린다" 도 아니다 — 리스가 그 유계다.
    public Mono<Void> renew() {
        return Mono.defer(() -> {
            if (standing.get().state() == State.CLOSED) {
                return Mono.<Void>empty();
            }
            long startedAt = ticker.getAsLong();
            return acquire.get()
                    // 멈춤은 오류가 아니라서 오류 처리에 안 걸린다. 이게 없으면
                    // 루프가 조용히 멎고 참으로 얼어붙는다.
                    .timeout(attemptTimeout)
                    .flatMap(lock -> observed(lock, startedAt))
                    .doOnError(this::enterFailing)
                    .onErrorResume(e -> Mono.empty());
        }).then();
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
            Standing before = standing.getAndUpdate(Standing::closed);
            if (before.state() == State.CLOSED) {
                return Mono.<Void>empty();
            }
            // **로컬 상태로 거르지 않는다.** FOLLOWER 는 "락이 남의 것" 과 "리스가
            // 지나 내려왔지만 서버 락은 아직 내 것" 을 둘 다 뜻한다. 뒤엣것에서
            // 안 지우면 정상 종료인데도 다음 리더가 리스 만료를 기다린다.
            // 지우는 쪽이 안전한 것은 스크립트가 소유자를 확인하기 때문이다.
            long heldFor = ticker.getAsLong() - before.leaderSince();
            boolean wasLeader = before.state() == State.LEADER;
            return releaseLock()
                    // 취소돼도 내려온다. 완료 신호에만 기대면 종료 중에 리더로
                    // 남아, 다음 리더와 겹치는 구간이 리스가 아니라 영영이 된다.
                    .doFinally(signal -> {
                        if (wasLeader) {
                            log.info("리더에서 내려왔다 — {}초 동안 리더였다, owner={}",
                                    NANOSECONDS.toSeconds(heldFor), ownerId);
                        }
                    });
        }).then();
    }

    private Mono<Void> releaseLock() {
        return release.get()
                .timeout(attemptTimeout)
                .doOnError(e -> log.warn("리더 해제 실패 — 리스 만료로 풀린다: {}", e.toString()))
                .onErrorResume(e -> Mono.empty());
    }

    private Mono<Void> observed(LeaderLock lock, long startedAt) {
        exitFailing();
        if (!lock.acquired()) {
            // 사실을 알았다 — 리스를 기다릴 이유가 없다.
            // **판 번호도 같이 버린다.** 남겨 두면 강등된 노드가 자기가 쥐었던
            // 판인 척 되돌릴 수 없는 쓰기를 낸다.
            Standing before = standing.getAndUpdate(s -> s.state() == State.LEADER
                    ? new Standing(State.FOLLOWER, s.confirmedAt(), s.leaderSince(), 0)
                    : s);
            if (before.state() == State.LEADER) {
                log.info("리더를 잃었다 — {}초 동안 리더였다, 지금 소유자는 {}, 남은 리스 {}ms, owner={}",
                        heldSeconds(before), lock.describeOwner(), lock.ttlMillis(), ownerId);
            }
            return Mono.empty();
        }

        Standing before = standing.getAndUpdate(s -> switch (s.state()) {
            case CLOSED -> s;
            // **뒤로 밀지 않는다.** 겹친 두 판 중 늦게 도착한 옛 판이 확인 시각을
            // 되돌리면, 멀쩡한 리더가 헛강등되고 거짓 경고가 찍힌다.
            case LEADER -> new Standing(State.LEADER,
                    Math.max(s.confirmedAt(), startedAt), s.leaderSince(), lock.fence());
            case FOLLOWER -> new Standing(State.LEADER, startedAt, startedAt, lock.fence());
        });

        if (before.state() == State.CLOSED) {
            // 종료 표시보다 먼저 출발한 획득이 뒤늦게 성공했다. 그대로 두면
            // **죽는 노드가 락을 쥔 채 나가고** 다음 리더가 리스 만료를 기다린다.
            log.warn("종료 뒤에 락을 잡았다 — 곧바로 다시 지운다, owner={}", ownerId);
            return releaseLock();
        }
        if (before.state() == State.FOLLOWER) {
            log.info("리더가 됐다 — owner={}, lease={}초", ownerId, lease.toSeconds());
        }
        return Mono.empty();
    }

    private long heldSeconds(Standing since) {
        return NANOSECONDS.toSeconds(ticker.getAsLong() - since.leaderSince());
    }

    /**
     * 실패가 이어지는 동안 경고는 한 번만 찍고 <b>삼킨 판을 센다.</b>
     *
     * <p>매 판 찍으면 몇 분짜리 단절에 노드마다 수백 줄이다. 그렇다고 지속 시간만
     * 남기면 한 판이 실패한 것인지 수백 판인지 사후에 못 가린다.
     */
    private void enterFailing(Throwable cause) {
        long now = ticker.getAsLong();
        Failing before = failing.getAndUpdate(f -> f == null
                ? new Failing(now, 1)
                : new Failing(f.since(), f.swallowed() + 1));
        if (before == null) {
            log.warn("리더 확인 실패 — 리스가 남은 동안은 리더로 둔다, owner={}", ownerId, cause);
        }
    }

    private void exitFailing() {
        Failing before = failing.getAndSet(null);
        if (before == null) {
            return;
        }
        log.info("리더 확인 복구 — {}초 만에, 그동안 {}판 실패, owner={}",
                NANOSECONDS.toSeconds(ticker.getAsLong() - before.since()), before.swallowed(), ownerId);
    }
}

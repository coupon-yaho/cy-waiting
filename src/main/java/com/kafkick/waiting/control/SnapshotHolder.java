package com.kafkick.waiting.control;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * 판정 재료를 들고 있고 <b>낡음을 두 종류로</b> 구분한다.
 *
 * <p>{@code fetchStale} 은 이 노드의 루프가 멎은 것이라 503 이고, {@code dataStale}
 * 은 스케줄러가 멎은 것이라 200 을 유지한다. 그래서 앞엣것은 "받아왔는가" 가 아니라
 * <b>"루프가 도는가"</b> 로 잰다 — 받아오기 실패는 모든 노드에 동시에 오고, 그걸
 * 노드별 신호로 흘리면 전 노드가 한꺼번에 빠진다 (AIJ-0033).
 */
public final class SnapshotHolder {

    private final Duration fetchStaleAfter;
    private final Duration dataStaleAfter;
    private final Clock clock;

    /**
     * 스냅샷과 수신 시각을 <b>한 덩어리로</b> 든다.
     *
     * <p>둘을 따로 두면 갱신 도중 읽는 쪽이 새 스냅샷과 옛 수신 시각을 함께
     * 본다. 그러면 방금 갱신했는데도 {@code fetchStale} 이 참이 되고, 그 값이
     * 503 경로에 물려 있어 <b>정상 노드가 빠진다.</b>
     */
    private record 상태(GatewaySnapshot snapshot, Instant fetchedAt) {
    }

    /**
     * <b>락을 쓰지 않는다.</b> 읽는 쪽이 요청 경로라, 여기서 잠그면 판정마다
     * 경합이 생긴다. 참조 교체는 원자적이고 담긴 것이 전부 불변이라 족하다.
     */
    private volatile 상태 current = new 상태(GatewaySnapshot.EMPTY, Instant.EPOCH);

    /**
     * 갱신 루프가 마지막으로 한 바퀴 돈 시각. <b>성패와 무관하다.</b>
     *
     * <p>{@code null} 은 루프가 아직 한 번도 못 돈 것이다 — 기동 직후이거나
     * 루프를 띄우지 못했다는 뜻이라 판정 재료가 없다. 이때는 낡음으로 본다.
     *
     * <p>{@code 상태} 와 달리 따로 둔다. 스냅샷과 짝지어 읽을 필요가 없고,
     * 실패한 갱신도 이것만 갱신하기 때문이다.
     */
    private volatile Instant lastTick;

    public static SnapshotHolder of(Duration fetchStaleAfter, Duration dataStaleAfter,
            Clock clock) {
        return new SnapshotHolder(fetchStaleAfter, dataStaleAfter, clock);
    }

    private SnapshotHolder(Duration fetchStaleAfter, Duration dataStaleAfter, Clock clock) {
        this.fetchStaleAfter = fetchStaleAfter;
        this.dataStaleAfter = dataStaleAfter;
        this.clock = clock;
    }

    public GatewaySnapshot current() {
        return current.snapshot();
    }

    /** 통째로 갈아 끼운다. 실패한 갱신은 이걸 부르지 않는다 — 옛 값이 남는다. */
    public void replace(GatewaySnapshot snapshot) {
        Instant now = clock.instant();
        this.current = new 상태(snapshot, now);
        this.lastTick = now;
    }

    /**
     * 루프가 한 바퀴 돌았다고 알린다. <b>받아오기에 실패해도 부른다.</b>
     *
     * <p>이걸 성공했을 때만 부르면 {@code fetchStale} 이 다시 "받아왔는가" 로
     * 돌아가고, 공유 원인 장애가 전 노드 동시 이탈이 된다.
     */
    public void 루프가_돌았다() {
        this.lastTick = clock.instant();
    }

    /**
     * 루프가 아직 한 번도 안 돌았나 — 기동 직후 구간이다.
     *
     * <p><b>돌다 멎은 것과 갈라 두려고 노출한다.</b> {@link #tickAge()} 로는 둘이
     * 같은 값인데, 앞엣것을 재기동 신호로 쓰면 첫 판을 못 돈 파드가 죽고 다시
     * 떠서 또 죽는다 — 크래시 루프다. liveness 는 이것이 거짓일 때만 본다.
     */
    public boolean 첫_회전_전인가() {
        return lastTick == null;
    }

    /**
     * 마지막으로 <b>받아 온</b> 뒤 흐른 시간. 실패한 갱신은 이걸 안 움직인다.
     *
     * <p>판정에는 안 쓴다 — 그것이 이 클래스 주석의 요지다. 헬스 응답 detail 에
     * {@link #dataAge()} 와 나란히 실어 사람이 원인을 가리게 하는 값이다.
     */
    public Duration fetchAge() {
        return Duration.between(current.fetchedAt(), clock.instant());
    }

    /**
     * 루프가 마지막으로 돈 뒤 흐른 시간. 한 번도 안 돌았으면 무한이다.
     *
     * <p>이 노드가 마지막으로 <i>받아 온</i> 뒤가 아니다 — 위 클래스 주석 참조.
     */
    public Duration tickAge() {
        Instant tick = lastTick;
        return tick == null ? ChronoUnit.FOREVER.getDuration()
                : Duration.between(tick, clock.instant());
    }

    /**
     * 스케줄러가 발행한 뒤 흐른 시간. 이 노드의 사정과 무관하다.
     *
     * <p><b>음수는 0 으로 본다.</b> 리더 시계가 앞서면 발행 시각이 미래로 와서
     * 나이가 음수가 되고, 그러면 {@code dataStale} 이 영영 거짓이 된다 —
     * 스케줄러가 죽어도 아무 노드가 fail-open 에 못 들어간다. 조용히 보정하지
     * 않고 {@link #시계가_앞섰나()} 로 드러낸다.
     */
    public Duration dataAge() {
        Duration age = Duration.between(current.snapshot().publishedAt(), clock.instant());
        return age.isNegative() ? Duration.ZERO : age;
    }

    /** 발행 시각이 이 노드의 현재보다 미래인가 — 시계가 갈렸다는 신호다. */
    public boolean 시계가_앞섰나() {
        return Duration.between(current.snapshot().publishedAt(), clock.instant()).isNegative();
    }

    /** 임계와 같으면 아직 낡지 않았다 — 넘어야 낡음이다. */
    public boolean isFetchStale() {
        return tickAge().compareTo(fetchStaleAfter) > 0;
    }

    /**
     * <b>시계가 갈리면 곧바로 낡음이다.</b> 나이를 0 으로만 보정하면 갱신이
     * 멎어도 임계가 지날 때까지 최신으로 취급된다 — 그동안 아무 노드도
     * fail-open 에 못 들어간다.
     */
    public boolean isDataStale() {
        return 시계가_앞섰나() || dataAge().compareTo(dataStaleAfter) > 0;
    }
}

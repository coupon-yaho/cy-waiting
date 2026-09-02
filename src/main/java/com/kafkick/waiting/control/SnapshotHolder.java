package com.kafkick.waiting.control;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

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
     * 판정에 쓰는 셋을 <b>한 덩어리로</b> 든다. {@code lastTick} 이 {@code null}
     * 이면 루프가 아직 한 바퀴도 못 돈 것이다.
     *
     * <p>따로 두면 읽는 쪽이 새 스냅샷과 옛 시각을 짝지어 본다. 그러면 방금
     * 갱신했는데도 낡음이 되고, 그 값이 노드를 빼는 경로에 물려 있다.
     */
    /**
     * @param ageAtFetch 받아온 순간에 잰 재료의 나이. <b>레디스 시계 하나로</b>
     *                   잰 값이라 노드 시계가 어긋나도 안 흔들린다
     */
    private record Held(GatewaySnapshot snapshot, Instant fetchedAt, Instant lastTick,
            Duration ageAtFetch) {
    }

    /**
     * <b>락을 쓰지 않는다.</b> 읽는 쪽이 요청 경로라, 여기서 잠그면 판정마다
     * 경합이 생긴다. 참조 교체는 원자적이고 담긴 것이 전부 불변이라 족하다.
     */
    private final AtomicReference<Held> current =
            new AtomicReference<>(
                    new Held(GatewaySnapshot.EMPTY, Instant.EPOCH, null, Duration.ZERO));

    public static SnapshotHolder of(Duration fetchStaleAfter, Duration dataStaleAfter,
            Clock clock) {
        return new SnapshotHolder(fetchStaleAfter, dataStaleAfter, clock);
    }

    private SnapshotHolder(Duration fetchStaleAfter, Duration dataStaleAfter, Clock clock) {
        // **이 값은 남의 불변식도 정한다.** 판정 쪽 래치 수명이 여기서 나오므로,
        // 1 초 미만이면 래치가 사실상 없어지고 그 자리가 추월 창이 된다.
        if (dataStaleAfter == null || dataStaleAfter.compareTo(Duration.ofSeconds(1)) < 0) {
            throw new IllegalArgumentException(
                    "dataStaleAfter 는 1초 이상이어야 한다: " + dataStaleAfter);
        }
        this.fetchStaleAfter = fetchStaleAfter;
        this.dataStaleAfter = dataStaleAfter;
        this.clock = clock;
    }

    public GatewaySnapshot current() {
        return current.get().snapshot();
    }

    /**
     * 스냅샷을 아직 믿는 한계.
     *
     * <p><b>줄을 세운 기록이 이보다 먼저 풀리면 추월이 난다</b> (불변식 4).
     */
    public Duration dataStaleAfter() {
        return dataStaleAfter;
    }

    /**
     * 판정과 진단을 <b>같은 순간에서</b> 뽑는다.
     *
     * <p>따로 읽으면 그 사이에 갱신이 들어와, 못 받는다고 하면서 재료는 있다고
     * 하는 응답이 나온다. 첫 스냅샷이 도착하는 순간에 정확히 걸린다.
     */
    public View view() {
        Held held = current.get();
        Instant now = clock.instant();
        Duration dataAge = dataAgeOf(held, now);
        return new View(held.snapshot(), age(held.fetchedAt(), now),
                held.lastTick() == null ? null : age(held.lastTick(), now),
                dataAge.isNegative() ? Duration.ZERO : dataAge, dataAge.isNegative());
    }

    private Duration age(Instant since, Instant now) {
        return Duration.between(since, now);
    }

    /**
     * <b>절대 시각을 빼지 않는다.</b> 받아온 순간에 잰 나이에 그 뒤로 이 노드가
     * 흐른 시간을 더한다 — 앞은 레디스 시계 하나로, 뒤는 노드 시계 하나로 잰
     * 값이라 어느 쪽도 두 시계의 차가 아니다.
     */
    private Duration dataAgeOf(Held held, Instant now) {
        return held.ageAtFetch().plus(Duration.between(held.fetchedAt(), now));
    }

    /**
     * 한 순간의 판정 재료.
     *
     * @param tickAge 루프가 한 번도 안 돌았으면 {@code null}
     */
    public record View(GatewaySnapshot snapshot, Duration fetchAge, Duration tickAge,
            Duration dataAge, boolean clockAhead) {

        public boolean isBeforeFirstTick() {
            return tickAge == null;
        }
    }

    /** 이 뷰가 낡았나 — 임계는 홀더가 안다. */
    public boolean isFetchStale(View view) {
        return view.tickAge() != null && view.tickAge().compareTo(fetchStaleAfter) > 0;
    }

    public boolean isDataStale(View view) {
        return view.clockAhead() || view.dataAge().compareTo(dataStaleAfter) > 0;
    }

    /**
     * 통째로 갈아 끼운다. 실패한 갱신은 이걸 부르지 않는다 — 옛 값이 남는다.
     *
     * <p>성공한 회차는 셋을 다 정한다. 앞 값을 볼 것이 없어 CAS 가 필요 없다.
     */
    public void replace(GatewaySnapshot snapshot) {
        Instant now = clock.instant();
        // 시각을 못 받았으면 두 벽시계를 비교하는 수밖에 없다. 받아오는 쪽이
        // 레디스 시각을 같이 읽으므로 운영에서는 아래 형태를 쓴다.
        current.set(new Held(snapshot, now, now, Duration.between(snapshot.publishedAt(), now)));
    }

    /**
     * 받아온 순간의 <b>레디스 시각</b>과 함께 갈아 끼운다.
     *
     * <p>재료의 나이를 여기서 한 번만 재고, 그 뒤로는 이 노드가 흐른 시간만
     * 더한다 — 두 벽시계를 빼는 자리가 없어진다.
     */
    public void replace(GatewaySnapshot snapshot, long serverSec) {
        Instant now = clock.instant();
        // **부호를 안 지운다.** 발행 시각이 미래면 그건 시계가 갈렸다는 신호라,
        // 조용히 0 으로 접으면 스케줄러가 죽어도 아무도 모른다.
        current.set(new Held(snapshot, now, now,
                Duration.between(snapshot.publishedAt(), Instant.ofEpochSecond(serverSec))));
    }

    /**
     * 루프가 한 바퀴 돌았다고 알린다. <b>받아오기에 실패해도 부른다.</b>
     *
     * <p>성공했을 때만 부르면 낡음이 다시 "받아왔는가" 가 되고, 공유 원인
     * 장애가 전 노드 동시 이탈이 된다. 스냅샷은 앞 값을 그대로 이어야 하므로
     * 읽고 쓰는 사이에 갱신이 끼지 않게 CAS 로 돌린다.
     */
    public void loopTicked() {
        Instant now = clock.instant();
        current.updateAndGet(s -> new Held(s.snapshot(), s.fetchedAt(), now, s.ageAtFetch()));
    }

    /**
     * 루프가 아직 한 번도 안 돌았나 — 기동 직후 구간이다.
     *
     * <p><b>돌다 멎은 것과 갈라 두려고 노출한다.</b> 재기동 신호로 쓰면 첫 회차를
     * 못 돈 파드가 죽고 다시 떠서 또 죽는다 — 크래시 루프다.
     */
    public boolean isBeforeFirstTick() {
        return current.get().lastTick() == null;
    }

    /**
     * 마지막으로 <b>받아 온</b> 뒤 흐른 시간. 실패한 갱신은 이걸 안 움직인다.
     *
     * <p>판정에는 안 쓴다. 헬스 detail 에 {@link #dataAge()} 와 나란히 실어
     * 사람이 원인을 가리게 하는 값이다.
     */
    public Duration fetchAge() {
        return Duration.between(current.get().fetchedAt(), clock.instant());
    }

    /**
     * 루프가 마지막으로 돈 뒤 흐른 시간. 한 번도 안 돌았으면 무한이다.
     *
     * <p>이 노드가 마지막으로 <i>받아 온</i> 뒤가 아니다 — 위 클래스 주석 참조.
     */
    public Duration tickAge() {
        Instant tick = current.get().lastTick();
        return tick == null ? ChronoUnit.FOREVER.getDuration()
                : Duration.between(tick, clock.instant());
    }

    /**
     * 스케줄러가 발행한 뒤 흐른 시간. 이 노드의 사정과 무관하다.
     *
     * <p><b>음수는 0 으로 본다.</b> 리더 시계가 앞서면 발행 시각이 미래로 와서
     * 나이가 음수가 되고, 그러면 {@code dataStale} 이 영영 거짓이 된다 —
     * 스케줄러가 죽어도 아무 노드가 fail-open 에 못 들어간다. 조용히 보정하지
     * 않고 {@link #isClockAhead()} 로 드러낸다.
     */
    public Duration dataAge() {
        return view().dataAge();
    }

    /** 발행 시각이 이 노드의 현재보다 미래인가 — 시계가 갈렸다는 신호다. */
    public boolean isClockAhead() {
        return view().clockAhead();
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
        return isClockAhead() || dataAge().compareTo(dataStaleAfter) > 0;
    }
}

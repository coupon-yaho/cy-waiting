package com.kafkick.waiting.control;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * 판정 재료를 들고 있고 <b>낡음을 두 종류로</b> 구분한다.
 *
 * <p>{@code fetchStale} 은 이 노드의 갱신 루프가 멎은 것이라 503 이고,
 * {@code dataStale} 은 스케줄러가 멎은 것이라 200 을 유지한다. 섞으면 스케줄러
 * 장애가 전 게이트웨이 동시 이탈로 번져 100% 장애가 된다.
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
        this.current = new 상태(snapshot, clock.instant());
    }

    /** 이 노드가 마지막으로 받아 온 뒤 흐른 시간. */
    public Duration fetchAge() {
        return Duration.between(current.fetchedAt(), clock.instant());
    }

    /** 스케줄러가 발행한 뒤 흐른 시간. 이 노드의 사정과 무관하다. */
    public Duration dataAge() {
        return Duration.between(current.snapshot().publishedAt(), clock.instant());
    }

    /** 임계와 같으면 아직 낡지 않았다 — 넘어야 낡음이다. */
    public boolean isFetchStale() {
        return fetchAge().compareTo(fetchStaleAfter) > 0;
    }

    public boolean isDataStale() {
        return dataAge().compareTo(dataStaleAfter) > 0;
    }
}

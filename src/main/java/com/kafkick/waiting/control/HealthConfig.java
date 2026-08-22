package com.kafkick.waiting.control;

import java.time.Clock;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 헬스 배선.
 *
 * <p>빈 이름이 곧 그룹에 적는 이름이다. 이름을 바꾸면 설정의 그룹이 조용히
 * 비고, <b>빈 그룹은 항상 통과</b>한다.
 */
@Configuration
public class HealthConfig {

    /** 이 노드의 루프가 멎었다고 볼 임계. 스냅샷 주기의 몇 배로 둔다. */
    private static final Duration FETCH_STALE_AFTER = Duration.ofSeconds(3);

    /** 스케줄러가 멎었다고 볼 임계. 리더 승계보다 넉넉해야 교체가 낡음으로 안 번진다. */
    private static final Duration DATA_STALE_AFTER = Duration.ofSeconds(5);

    /** 각 노드가 판정 재료를 받아 가는 주기. */
    private static final Duration FETCH_INTERVAL = Duration.ofMillis(500);

    @Bean
    SnapshotHolder snapshotHolder() {
        return SnapshotHolder.of(FETCH_STALE_AFTER, DATA_STALE_AFTER, Clock.systemUTC());
    }

    @Bean
    ShutdownState shutdownState() {
        return ShutdownState.create();
    }

    /**
     * 판정 재료를 받아 오는 루프.
     *
     * <p><b>이게 없으면 홀더가 영원히 빈다.</b> 받는 판정이 영구히 거절하고,
     * 살아 있음 판정은 첫 판 전이라 통과하므로 재기동도 안 된다 — 뜨긴 뜨는데
     * 아무것도 안 하는 파드가 된다.
     */
    @Bean
    SnapshotRefresher snapshotRefresher(SnapshotHolder holder, SnapshotSource source) {
        return SnapshotRefresher.of(holder, source::load);
    }

    @Bean
    SnapshotRefreshLifecycle snapshotRefreshLifecycle(SnapshotRefresher refresher,
            ShutdownState shutdown) {
        return SnapshotRefreshLifecycle.of(refresher, shutdown, FETCH_INTERVAL);
    }

    @Bean
    JudgingHealth judging(SnapshotHolder holder, ShutdownState shutdown) {
        return JudgingHealth.of(holder, shutdown);
    }

    @Bean
    LoopAliveHealth loopAlive(SnapshotHolder holder, ShutdownState shutdown) {
        return LoopAliveHealth.of(holder, shutdown);
    }
}

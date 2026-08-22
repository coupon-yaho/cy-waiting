package com.kafkick.waiting.control;

import java.time.Clock;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 헬스 배선.
 *
 * <p>빈 이름이 곧 그룹에 적는 이름이다. 이름을 바꾸면 설정의 그룹이 조용히
 * 비고, <b>빈 그룹은 항상 통과</b>한다 — 확인이 사라진 것을 아무도 모른다.
 */
@Configuration
public class HealthConfig {

    /** 이 노드의 루프가 멎었다고 볼 임계. 스냅샷 주기의 몇 배로 둔다. */
    private static final Duration FETCH_STALE_AFTER = Duration.ofSeconds(3);

    /** 스케줄러가 멎었다고 볼 임계. 리더 승계보다 넉넉해야 교체가 낡음으로 안 번진다. */
    private static final Duration DATA_STALE_AFTER = Duration.ofSeconds(5);

    @Bean
    SnapshotHolder snapshotHolder() {
        return SnapshotHolder.of(FETCH_STALE_AFTER, DATA_STALE_AFTER, Clock.systemUTC());
    }

    @Bean
    JudgingHealth judging(SnapshotHolder holder) {
        return JudgingHealth.of(holder);
    }

    @Bean
    LoopAliveHealth loopAlive(SnapshotHolder holder) {
        return LoopAliveHealth.of(holder);
    }
}

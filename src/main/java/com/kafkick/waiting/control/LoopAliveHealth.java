package com.kafkick.waiting.control;

import java.util.Objects;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * 루프가 멎었으면 <b>이 프로세스의 결함</b>이다 — 빼는 것이 아니라 재기동이다.
 *
 * <p>받아오기 실패는 여기 안 넣는다. 공유 원인일 수 있고, 그러면 전 노드가 동시에
 * 재기동해 그 자체가 전면 장애가 된다.
 */
public final class LoopAliveHealth implements HealthIndicator {

    private final SnapshotHolder holder;
    private final ShutdownState shutdown;

    private LoopAliveHealth(SnapshotHolder holder, ShutdownState shutdown) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
        this.shutdown = Objects.requireNonNull(shutdown, "shutdown 은 필수다");
    }

    public static LoopAliveHealth of(SnapshotHolder holder, ShutdownState shutdown) {
        return new LoopAliveHealth(holder, shutdown);
    }

    /**
     * <b>한 번이라도 돈 뒤</b> 임계를 넘겼을 때만 죽는다.
     *
     * <p>기동 직후를 정지로 세면 첫 판을 못 돈 파드가 죽고, 재기동해도 또 첫 판
     * 전이라 또 죽는다. 그 구간은 받는 쪽과 기동 프로브가 맡는다.
     */
    @Override
    public Health health() {
        // **드레이닝 중에는 루프를 안 본다.** 종료하려고 내린 루프를 정지로 세면
        // 진행 중인 요청을 든 파드가 그 자리에서 끊긴다.
        if (shutdown.isDraining()) {
            return Health.up().withDetail("draining", true).build();
        }
        if (holder.isBeforeFirstTick()) {
            return Health.up().withDetail("firstTick", false).build();
        }
        Health.Builder builder = holder.isFetchStale() ? Health.down() : Health.up();
        return builder
                .withDetail("firstTick", true)
                .withDetail("tickAgeSec", holder.tickAge().toSeconds())
                .build();
    }
}

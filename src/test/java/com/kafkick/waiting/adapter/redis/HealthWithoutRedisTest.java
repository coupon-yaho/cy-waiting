package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.JudgingHealth;
import com.kafkick.waiting.control.SnapshotHolder;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * <b>레디스를 끊어도 받는 판정은 200 을 유지한다.</b>
 *
 * <p>요청 경로에서 레디스를 안 치므로 끊겨도 판정은 계속할 수 있다. 여기에
 * 의존성을 넣으면 레디스 장애가 곧 전면 장애가 된다 — 전 노드가 한꺼번에
 * 빠지기 때문이다.
 */
@Tag("integration")
@SpringBootTest
class HealthWithoutRedisTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(10);

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private JudgingHealth judging;

    @Autowired
    private SnapshotHolder holder;

    @Test
    @DisplayName("레디스가_끊겨도_받는_것을_유지한다")
    void 레디스가_끊겨도_받는_것을_유지한다() {
        holder.replace(new GatewaySnapshot(Map.of(), GatewaySnapshot.EMPTY.meta(),
                java.time.Instant.now()));
        assertThat(judging.health().getStatus()).isEqualTo(Status.UP);

        // 실제로 못 쓰게 만든다. 설정만 보면 배선이 바뀌었을 때 안 드러난다.
        redis.execute(connection -> connection.serverCommands()
                .setConfig("maxmemory", "1")).blockLast(WAIT);
        try {
            assertThat(judging.health().getStatus()).isEqualTo(Status.UP);
        } finally {
            redis.execute(connection -> connection.serverCommands()
                    .setConfig("maxmemory", "0")).blockLast(WAIT);
        }
    }

}

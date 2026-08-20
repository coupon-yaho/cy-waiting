package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.chaos.RedisFaults;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 레디스가 끊겼다 붙는 동안 판정 재료가 살아 있는지 본다.
 *
 * <p>인메모리 대역으로는 이걸 못 본다 — 끊김은 예외 종류가 아니라 <b>연결의
 * 상태</b>라, 실물이어야 회복 경로가 재현된다 (TS-3).
 */
@Tag("chaos")
class SnapshotRefreshIntegrationTest {

    private static final String KEY = com.kafkick.waiting.adapter.redis.RedisKeys.SNAPSHOT;
    private static final Instant 지금 = Instant.parse("2026-08-20T00:00:00Z");

    private RedisFaults faults;

    @BeforeEach
    void 준비() {
        faults = RedisFaults.시작한다();
    }

    @AfterEach
    void 정리() {
        faults.close();
    }

    @Test
    @DisplayName("끊긴_동안에도_판정_재료가_남는다")
    void 끊긴_동안에도_판정_재료가_남는다() {
        StatefulRedisConnection<String, String> redis = faults.연결한다();
        redis.sync().hset(KEY, Map.of(
                "#credit", "1000", "#nodes", "2", "#published", "1787184000",
                "c1", "ADAPTIVE:QUEUEING:100:500:2000:1.0"));

        SnapshotHolder holder = SnapshotHolder.of(Duration.ofSeconds(2), Duration.ofSeconds(5),
                Clock.fixed(지금, ZoneOffset.UTC));
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> Mono.fromCallable(() -> redis.sync().hgetall(KEY)),
                Duration.ofSeconds(2));

        refresher.한번().block(Duration.ofSeconds(5));
        assertThat(holder.current().coupons()).containsOnlyKeys("c1");

        faults.끊는다();
        refresher.한번().block(Duration.ofSeconds(5));

        // **레디스가 죽었는데 판정은 계속된다.** 이것이 이 게이트웨이의 전제다.
        assertThat(holder.current().coupons()).containsOnlyKeys("c1");
        assertThat(holder.current().meta().globalCredit()).isEqualTo(1000);
    }

    @Test
    @DisplayName("붙으면_다시_갱신된다")
    void 붙으면_다시_갱신된다() {
        StatefulRedisConnection<String, String> redis = faults.연결한다();
        redis.sync().hset(KEY, Map.of(
                "#credit", "1000", "#nodes", "2", "#published", "1787184000",
                "c1", "ADAPTIVE:QUEUEING:100:500:2000:1.0"));

        SnapshotHolder holder = SnapshotHolder.of(Duration.ofSeconds(2), Duration.ofSeconds(5),
                Clock.fixed(지금, ZoneOffset.UTC));
        SnapshotRefresher refresher = SnapshotRefresher.of(holder,
                () -> Mono.fromCallable(() -> redis.sync().hgetall(KEY)),
                Duration.ofSeconds(2));
        refresher.한번().block(Duration.ofSeconds(5));

        faults.끊는다();
        refresher.한번().block(Duration.ofSeconds(5));
        faults.붙인다();

        // **끊긴 사이 들고 있던 c1 은 회복 뒤 사라지는 것이 맞다.** 컨테이너와
        // 함께 데이터도 죽었기 때문이다 — 여기서 검증할 것은 옛 값의 생존이
        // 아니라 **루프가 되살아나 지금의 레디스를 다시 읽는가** 다.
        StatefulRedisConnection<String, String> 다시 = faults.연결한다();
        다시.sync().hset(KEY, Map.of("c2", "ALWAYS:IDLE:0:9:0:1.0"));
        Awaitility.await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    refresher.한번().block(Duration.ofSeconds(5));
                    assertThat(holder.current().coupons()).containsOnlyKeys("c2");
                });
        다시.close();
    }
}

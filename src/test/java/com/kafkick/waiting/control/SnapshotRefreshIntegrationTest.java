package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.chaos.RedisFaults;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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

    /** 스케줄러가 실제로 쓰는 모양. 전역값이 빠진 해시는 발행되지 않는다. */
    private static Map<String, String> 스냅샷(String... 쿠폰) {
        Map<String, String> m = new LinkedHashMap<>(Map.of(
                "#credit", "1000", "#nodes", "2", "#published", "1787184000"));
        for (int i = 0; i < 쿠폰.length; i += 2) {
            m.put(쿠폰[i], 쿠폰[i + 1]);
        }
        return m;
    }

    private SnapshotRefresher 붙인다(SnapshotHolder holder,
            StatefulRedisConnection<String, String> redis) {
        return SnapshotRefresher.of(holder,
                () -> Mono.fromCallable(() -> redis.sync().hgetall(KEY))
                        .subscribeOn(Schedulers.boundedElastic()),
                Duration.ofSeconds(2));
    }

    private static SnapshotHolder 홀더() {
        return SnapshotHolder.of(Duration.ofSeconds(2), Duration.ofSeconds(5),
                Clock.fixed(지금, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("끊긴_동안에도_판정_재료가_남는다")
    void 끊긴_동안에도_판정_재료가_남는다() {
        StatefulRedisConnection<String, String> redis = faults.연결한다();
        redis.sync().hset(KEY, 스냅샷("c1", "ADAPTIVE:QUEUEING:100:500:2000:1.0"));

        SnapshotHolder holder = 홀더();
        SnapshotRefresher refresher = 붙인다(holder, redis);
        refresher.once().block(Duration.ofSeconds(10));
        assertThat(holder.current().coupons()).containsOnlyKeys("c1");

        // **끊기지 않았다면 보일 것을 심어 둔다.** 안 심으면 "장애를 견뎠다"
        // 와 "장애가 없었다" 가 같은 결과를 내서, 주입을 꺼도 통과한다 (TS-9).
        redis.sync().hset(KEY, Map.of("c2", "ALWAYS:IDLE:0:9:0:1.0"));

        faults.끊는다();
        refresher.once().block(Duration.ofSeconds(10));

        // **레디스가 죽었는데 판정은 계속된다.** 이것이 이 게이트웨이의 전제다.
        // c2 가 안 보이는 것이 곧 장애가 실제로 주입됐다는 증거다.
        assertThat(holder.current().coupons()).containsOnlyKeys("c1");
        assertThat(holder.current().meta().globalCredit()).isEqualTo(1000);
    }

    @Test
    @DisplayName("붙으면_다시_갱신된다")
    void 붙으면_다시_갱신된다() {
        StatefulRedisConnection<String, String> redis = faults.연결한다();
        redis.sync().hset(KEY, 스냅샷("c1", "ADAPTIVE:QUEUEING:100:500:2000:1.0"));

        SnapshotHolder holder = 홀더();
        SnapshotRefresher refresher = 붙인다(holder, redis);
        refresher.once().block(Duration.ofSeconds(10));

        faults.끊는다();
        refresher.once().block(Duration.ofSeconds(10));
        faults.붙인다();

        // **끊긴 사이 들고 있던 c1 은 회복 뒤 사라지는 것이 맞다** — 컨테이너와
        // 함께 데이터도 죽었다. 볼 것은 루프가 되살아나 지금의 레디스를 다시
        // 읽는가다. 넣는 해시도 스케줄러가 쓰는 모양이어야 한다 — 전역값이
        // 빠지면 발행되지 않은 것으로 보고 거부된다.
        StatefulRedisConnection<String, String> 다시 = faults.연결한다();
        다시.sync().hset(KEY, 스냅샷("c2", "ALWAYS:IDLE:0:9:0:1.0"));
        SnapshotRefresher 회복 = 붙인다(holder, 다시);
        Awaitility.await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    회복.once().block(Duration.ofSeconds(5));
                    assertThat(holder.current().coupons()).containsOnlyKeys("c2");
                });
        다시.close();
    }
}

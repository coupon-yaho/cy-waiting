package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/**
 * 시계가 뒤처진 복제본을 승격해도 <b>순서가 역행하지 않는다</b> (G3.10 · C13).
 *
 * <p>score 가 벽시계라(A-9) 승격된 복제본의 시계가 뒤처져 있으면 <b>그 구간
 * 전체가 한꺼번에 추월당한다.</b> {@code maxscore} 바닥값이 이걸 막는다 —
 * 같은 Lua 안에서 갱신되므로 ZSET 과 함께 복제된다 (2.1절).
 */
@Tag("chaos")
class ReplicaPromotionTest {

    private static final String COUPON = "c1";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String ALIVE = RedisKeys.alive(COUPON, 1, 0);

    private static final long NOW = 1_800_000_000L;
    private static final int BEFORE = 100;
    private static final int AFTER = 50;

    private final List<GenericContainer<?>> 띄운것 = new ArrayList<>();
    private final List<RedisClient> 클라이언트 = new ArrayList<>();
    private final List<StatefulRedisConnection<String, String>> 연결 = new ArrayList<>();
    private Network network;

    /**
     * <b>연결 → 클라이언트 → 컨테이너 순으로 닫는다.</b> {@link RedisClient} 는
     * 제 Netty 이벤트 루프를 만들어서, {@code shutdown()} 없이는 그 스레드가
     * JVM 종료까지 남는다.
     */
    @AfterEach
    void 정리() {
        연결.forEach(StatefulRedisConnection::close);
        클라이언트.forEach(RedisClient::shutdown);
        띄운것.forEach(GenericContainer::stop);
        연결.clear();
        클라이언트.clear();
        띄운것.clear();
        if (network != null) {
            network.close();
        }
    }

    @SuppressWarnings("resource")   // 정리()가 닫는다
    private GenericContainer<?> 레디스(String alias) {
        GenericContainer<?> container = new GenericContainer<>(RedisContainerSupport.IMAGE)
                .withExposedPorts(6379)
                .withNetwork(network)
                .withNetworkAliases(alias)
                .withCommand("redis-server", "--appendonly", "no");
        container.start();
        띄운것.add(container);
        return container;
    }

    private StatefulRedisConnection<String, String> 붙는다(GenericContainer<?> c) {
        RedisClient client = RedisClient.create(
                "redis://%s:%d".formatted(c.getHost(), c.getMappedPort(6379)));
        클라이언트.add(client);
        StatefulRedisConnection<String, String> connection = client.connect();
        연결.add(connection);
        return connection;
    }

    /** {@code {score, floorApplied, alreadyQueued}}. score 는 문자열로 온다. */
    @SuppressWarnings("unchecked")
    private static List<Object> 등록한다(
            StatefulRedisConnection<String, String> redis, String member) {
        return (List<Object>) redis.sync().eval(LuaScripts.of("enqueue.lua"),
                ScriptOutputType.MULTI,
                new String[] {QUEUE, MAX_SCORE, ALIVE},
                // 상한 없음. 0 은 이 뜻이 아니다 — 0 은 한 명도 안 받는다는 뜻이다.
                member, "86400", "30", "-1", String.valueOf(NOW));
    }

    /** 컨테이너의 시계. 호스트 시계를 못 돌리므로 여기서 기준을 얻는다. */
    private static long 지금_마이크로초(StatefulRedisConnection<String, String> redis) {
        List<String> time = redis.sync().time();
        return Long.parseLong(time.get(0)) * 1_000_000L + Long.parseLong(time.get(1));
    }

    private static long score(List<Object> result) {
        return Long.parseLong(result.get(0).toString());
    }

    private static boolean 바닥값이_적용됐나(List<Object> result) {
        return ((Long) result.get(1)) == 1L;
    }

    @Test
    @DisplayName("시계가_뒤처진_복제본을_승격해도_순서가_역행하지_않는다")
    void 시계가_뒤처진_복제본을_승격해도_순서가_역행하지_않는다() {
        network = Network.newNetwork();
        GenericContainer<?> primary = 레디스("primary");
        GenericContainer<?> replica = 레디스("replica");

        var 주 = 붙는다(primary);
        var 복제 = 붙는다(replica);
        복제.sync().replicaof("primary", 6379);

        // **시계를 한 시간 앞세운다.** 컨테이너 시계는 호스트와 공유라 못
        // 돌린다. 하지만 승격된 복제본이 겪는 상태는 "제 시계가 뒤처졌다" 가
        // 아니라 관측 가능한 조건 하나다 — `TIME < maxscore`. 주가 앞선
        // 시계로 쌓은 큐를 물려받은 것과 같은 상태를 그대로 만든다.
        //
        // **컨테이너의 TIME 에서 유도한다.** 고정 시각을 박으면 실제 시각이
        // 그걸 지나는 날 바닥값이 안 걸리고, 시험은 조용히 아무것도 검증하지
        // 않게 된다 — 실패도 그날에야 난다.
        long 미래 = 지금_마이크로초(주) + 3600L * 1_000_000L;
        주.sync().set(MAX_SCORE, String.valueOf(미래));

        long 마지막 = 0;
        for (int i = 0; i < BEFORE; i++) {
            마지막 = score(등록한다(주, "m" + i));
        }
        // 앞세운 시계로 쌓였는지 확인한다. 아니면 이 시험은 평범한 큐를 본다.
        assertThat(마지막).isGreaterThan(미래);

        // 복제가 따라잡아야 승격 후의 상태가 의미를 갖는다.
        long 기대 = 마지막;
        Awaitility.await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    String floor = 복제.sync().get(MAX_SCORE);
                    return floor != null && Long.parseLong(floor) == 기대;
                });

        복제.sync().replicaofNoOne();

        List<String> 역행 = new ArrayList<>();
        AtomicReference<Long> 직전 = new AtomicReference<>(마지막);
        int 바닥값적용 = 0;
        for (int i = 0; i < AFTER; i++) {
            List<Object> result = 등록한다(복제, "post" + i);
            long score = score(result);
            if (바닥값이_적용됐나(result)) {
                바닥값적용++;
            }
            if (score <= 직전.get()) {
                역행.add("post%d score=%d ≤ 직전 %d".formatted(i, score, 직전.get()));
            }
            직전.set(score);
        }

        assertThat(역행)
                .withFailMessage("승격 후 순서가 역행했다 %d 건%n%s",
                        역행.size(), String.join("\n", 역행))
                .isEmpty();
        // 바닥값이 한 번도 안 걸렸다면 시계가 뒤처진 상태가 아니었던 것이고,
        // 그러면 이 시험은 아무것도 검증하지 않았다.
        assertThat(바닥값적용)
                .withFailMessage("바닥값이 한 번도 안 걸렸다 — 뒤처진 시계를 재현하지 못했다")
                .isEqualTo(AFTER);
    }
}

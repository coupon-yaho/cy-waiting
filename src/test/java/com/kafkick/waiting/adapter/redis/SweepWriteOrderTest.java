package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 청소가 실패해도 <b>자리와 기록이 함께 움직인다</b>.
 *
 * <p>제거를 먼저 하면 터졌을 때 자리도 잃고 식별도 안 되는 사람이 남는다.
 * 인자 상한이 이제 그 지점에 못 가게 막지만, 상한을 올리면 다시 그 자리다.
 */
@Tag("chaos")
class SweepWriteOrderTest {

    /** 스크립트가 받는 검사 범위 상한. 기록이 쌍이라 여기서 먼저 걸린다. */
    private static final int MAX_SCAN = 3999;

    private static final String COUPON = "order";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String GRACE = RedisKeys.grace(COUPON, 1, 0);
    private static final String ALIVE = RedisKeys.alive(COUPON, 1, 0);

    private RedisClient client;
    private StatefulRedisConnection<String, String> connection;

    @BeforeEach
    void 준비() {
        client = RedisClient.create("redis://%s:%d".formatted(
                RedisContainerSupport.REDIS.getHost(),
                RedisContainerSupport.REDIS.getFirstMappedPort()));
        connection = client.connect();
        connection.sync().del(QUEUE, GRACE, ALIVE);
    }

    @AfterEach
    void 정리() {
        connection.sync().del(QUEUE, GRACE, ALIVE);
        connection.close();
        client.shutdown();
    }

    /**
     * 전원이 이탈한 큐를 만든다 — 생존 신호를 아예 안 넣는다.
     *
     * <p><b>나눠 넣는다.</b> 한 번에 넣으면 픽스처가 같은 인자 한계에 먼저
     * 걸려, 검증하려던 자리에 닿기도 전에 시험이 죽는다.
     */
    private void 이탈자를_채운다(int count) {
        int batch = 1000;
        for (int start = 0; start < count; start += batch) {
            List<String> args = new ArrayList<>();
            for (int i = start; i < Math.min(start + batch, count); i++) {
                args.add(String.valueOf(i));
                args.add("m" + i);
            }
            connection.sync().eval(
                    "redis.call('ZADD', KEYS[1], unpack(ARGV)) return 1",
                    ScriptOutputType.INTEGER, new String[] {QUEUE},
                    args.toArray(String[]::new));
        }
    }

    private Object 청소한다(int limit) {
        return connection.sync().eval(LuaScripts.of("sweep.lua"), ScriptOutputType.MULTI,
                new String[] {QUEUE, GRACE, ALIVE},
                String.valueOf(limit), "1800000000", "300", "50", "0");
    }

    @Test
    @DisplayName("상한을_넘기면_아무도_안_빠진다")
    void 상한을_넘기면_아무도_안_빠진다() {
        // **거절이 쓰기 앞이다.** 인자를 다 본 뒤에 거절하므로 큐도 기록도
        // 안 건드린다 — 그 인원이 흔적 없이 사라지는 일이 없다.
        int 인원 = MAX_SCAN + 1;
        이탈자를_채운다(인원);

        assertThatThrownBy(() -> 청소한다(인원))
                .hasMessageContaining("검사 범위는");

        // **수만 세면 다른 사람이 같은 수로 남아도 통과한다.** 자리는 순서가
        // 곧 의미라, 누가 어느 순서로 남았는지까지 본다.
        List<String> 남은사람 = connection.sync().zrange(QUEUE, 0, -1);
        List<String> 원래순서 = new ArrayList<>();
        for (int i = 0; i < 인원; i++) {
            원래순서.add("m" + i);
        }
        assertThat(남은사람)
                .withFailMessage("실패했는데 큐가 바뀌었다 — 자리도 기록도 없이 증발했다")
                .containsExactlyElementsOf(원래순서);
        assertThat(connection.sync().zscore(QUEUE, "m0")).isZero();
        assertThat(connection.sync().hlen(GRACE)).isZero();
    }

    @Test
    @DisplayName("담을_수_있는_인원은_자리와_기록이_함께_움직인다")
    void 담을_수_있는_인원은_자리와_기록이_함께_움직인다() {
        int 인원 = MAX_SCAN;
        이탈자를_채운다(인원);

        청소한다(인원);

        assertThat(connection.sync().zcard(QUEUE)).isZero();
        assertThat(connection.sync().hlen(GRACE)).isEqualTo(인원);
    }
}

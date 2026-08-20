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
 * <p>제거를 먼저 하면 그 뒤 단계가 터졌을 때 <b>자리도 잃고 재방문자로도
 * 식별 안 되는 사람</b>이 남는다 — 이 스크립트를 Lua 로 둔 이유가 정확히
 * 그것을 막는 것이다. 기록을 먼저 하면 실패 시 남는 것은 "아직 안 빠진
 * 사람" 이라 다음 틱에 다시 처리된다.
 */
@Tag("chaos")
class SweepWriteOrderTest {

    /** {@code unpack} 인자 한계. 기록은 쌍이라 이 절반에서 먼저 걸린다. */
    private static final int UNPACK_LIMIT = 8000;

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
    @DisplayName("한_번에_못_담는_인원이면_아무도_빠지지_않는다")
    void 한_번에_못_담는_인원이면_아무도_빠지지_않는다() {
        // 제거가 먼저면 여기서 큐만 비고 기록이 0 이 된다 — 그 인원이
        // 흔적 없이 사라지고 호출부는 사라진 사실조차 모른다.
        int 인원 = UNPACK_LIMIT / 2;
        이탈자를_채운다(인원);

        assertThatThrownBy(() -> 청소한다(인원))
                .hasMessageContaining("unpack");

        assertThat(connection.sync().zcard(QUEUE))
                .withFailMessage("실패했는데 큐에서 빠졌다 — 자리도 기록도 없이 증발했다")
                .isEqualTo(인원);
        assertThat(connection.sync().hlen(GRACE)).isZero();
    }

    @Test
    @DisplayName("담을_수_있는_인원은_자리와_기록이_함께_움직인다")
    void 담을_수_있는_인원은_자리와_기록이_함께_움직인다() {
        int 인원 = UNPACK_LIMIT / 2 - 1;
        이탈자를_채운다(인원);

        청소한다(인원);

        assertThat(connection.sync().zcard(QUEUE)).isZero();
        assertThat(connection.sync().hlen(GRACE)).isEqualTo(인원);
    }
}

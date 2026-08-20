package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 게이트웨이 노드를 늘리고 줄인다 (4.0.3) — {@code N} 이 바뀌는 순간을 만든다.
 *
 * <p>{@code N} 은 각 노드의 몫을 정하는 분모다. 이걸 못 흔들면 F5(비대칭 반영)를
 * 검증할 수 없고, 그러면 <b>노드가 줄 때 크레딧이 초과 발행되는지</b>를 못 본다.
 */
@Tag("chaos")
class GatewayNodesTest {

    private RedisFaults redis;
    private StatefulRedisConnection<String, String> connection;
    private GatewayNodes nodes;

    @BeforeEach
    void 준비() {
        redis = RedisFaults.시작한다();
        connection = redis.연결한다();
        nodes = new GatewayNodes(connection, Duration.ofSeconds(10));
    }

    @AfterEach
    void 정리() {
        connection.close();
        redis.close();
    }

    @Test
    @DisplayName("등록하면_늘고_해제하면_준다")
    void 등록하면_늘고_해제하면_준다() {
        nodes.등록한다("node-1");
        nodes.등록한다("node-2");
        assertThat(nodes.살아있는_수()).isEqualTo(2);

        nodes.해제한다("node-1");
        assertThat(nodes.살아있는_수()).isEqualTo(1);
    }

    @Test
    @DisplayName("하트비트가_끊긴_노드는_세지_않는다")
    void 하트비트가_끊긴_노드는_세지_않는다() {
        // TTL 만 믿지 않는다 — TTL 은 지우는 시점이지 신선한 시점이 아니다.
        // 낡은 하트비트를 남긴 채로도 분모에서 빠져야 한다.
        nodes.등록한다("살아있음");
        nodes.낡은_하트비트를_심는다("죽었음", Duration.ofSeconds(30));

        assertThat(nodes.살아있는_수()).isEqualTo(1);
    }

    @Test
    @DisplayName("같은_노드를_두_번_등록해도_하나다")
    void 같은_노드를_두_번_등록해도_하나다() {
        nodes.등록한다("node-1");
        nodes.등록한다("node-1");

        assertThat(nodes.살아있는_수()).isEqualTo(1);
    }
}

package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;

/**
 * 제어 평면이 레디스를 칠 수 있는지 본다.
 *
 * <p>요청 경로는 레디스를 치지 않지만(RD-4) <b>스케줄러와 큐는 쳐야 한다.</b>
 * 배선이 없으면 Phase 4 착수 시점에야 드러난다.
 */
@SpringBootTest
class RedisWiringTest {

    @Autowired
    private ReactiveRedisConnectionFactory connectionFactory;

    @Test
    @DisplayName("리액티브_커넥션_팩토리가_뜬다")
    void 리액티브_커넥션_팩토리가_뜬다() {
        assertThat(connectionFactory).isNotNull();
    }

    @Test
    @DisplayName("드라이버는_Lettuce다")
    void 드라이버는_Lettuce다() {
        // Jedis 는 블로킹이라 이 프로젝트에서 쓸 수 없다 (RX-1).
        // 실수로 바뀌면 요청 경로가 아니라 스케줄러가 먼저 멎는다.
        assertThat(RedisClient.class).isNotNull();
        assertThat(connectionFactory.getClass().getName()).contains("Lettuce");
    }
}

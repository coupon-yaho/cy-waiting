package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 커넥션 설정.
 *
 * <p>제어 평면은 <b>틱 안에 끝나야 한다</b>. 명령 하나가 틱보다 오래 붙들면
 * 스케줄러가 그 틱을 통째로 놓치고, 그동안 배분이 멎는다.
 */
@SpringBootTest
class RedisConnectionSettingsTest {

    /** 스케줄러 틱. 명령 타임아웃은 이보다 짧아야 한다. */
    private static final Duration TICK = Duration.ofSeconds(1);

    @Autowired
    private DataRedisProperties properties;

    @Test
    @DisplayName("커넥션_풀을_쓰지_않는다")
    void 커넥션_풀을_쓰지_않는다() {
        // Lettuce 는 커넥션 하나를 멀티플렉싱한다. 풀을 얹으면 커넥션이 늘 뿐
        // 처리량은 안 늘고, 노드마다 커넥션 수가 달라져 레디스 쪽 한계를
        // 예측할 수 없게 된다. commons-pool2 가 없으면 풀은 애초에 안 켜진다.
        assertThatThrownBy(() -> Class.forName("org.apache.commons.pool2.ObjectPool"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    @DisplayName("명령_타임아웃이_설정되어_있다")
    void 명령_타임아웃이_설정되어_있다() {
        // 기본값은 무한이다. 무한 대기는 스케줄러를 멎게 한다.
        assertThat(properties.getTimeout()).isNotNull();
    }

    @Test
    @DisplayName("명령_타임아웃이_틱보다_짧다")
    void 명령_타임아웃이_틱보다_짧다() {
        assertThat(properties.getTimeout()).isLessThan(TICK);
    }

    @Test
    @DisplayName("연결_타임아웃이_리스보다_짧다")
    void 연결_타임아웃이_리스보다_짧다() {
        // 연결이 리스(2초)보다 오래 걸리면 그 사이 리더십을 잃는다.
        assertThat(properties.getConnectTimeout()).isNotNull();
        assertThat(properties.getConnectTimeout()).isLessThan(Duration.ofSeconds(2));
    }
}

package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;

/**
 * 시간 예산 검증이 실제로 무는가.
 *
 * <p>통과만 하는 검사는 모든 설정을 통과시킨다. 값을 바꾸는 사람이 주석을
 * 안 읽어도 <b>안 뜨게</b> 만드는 것이 이 검증의 목적이라, 그 동작을 고정한다.
 */
class RedisBudgetGuardTest {

    private DataRedisProperties props(Duration timeout, Duration connect) {
        DataRedisProperties p = new DataRedisProperties();
        p.setTimeout(timeout);
        p.setConnectTimeout(connect);
        return p;
    }

    @Test
    @DisplayName("예산_안이면_뜬다")
    void 예산_안이면_뜬다() {
        RedisConfig config =
                new RedisConfig(props(Duration.ofMillis(500), Duration.ofSeconds(1)));

        // 예외가 안 나는 것이 단언이다. 경계 바로 안쪽 값을 쓴다.
        assertThatCode(config::시간_예산을_확인한다).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("명령_타임아웃이_틱_이상이면_안_뜬다")
    void 명령_타임아웃이_틱_이상이면_안_뜬다() {
        RedisConfig config =
                new RedisConfig(props(Duration.ofSeconds(1), Duration.ofSeconds(1)));

        assertThatThrownBy(config::시간_예산을_확인한다)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("timeout");
    }

    @Test
    @DisplayName("연결_타임아웃이_리스_이상이면_안_뜬다")
    void 연결_타임아웃이_리스_이상이면_안_뜬다() {
        RedisConfig config =
                new RedisConfig(props(Duration.ofMillis(500), Duration.ofSeconds(2)));

        assertThatThrownBy(config::시간_예산을_확인한다)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connect-timeout");
    }

    @Test
    @DisplayName("설정이_없으면_안_뜬다")
    void 설정이_없으면_안_뜬다() {
        // 기본값은 무한이다. 무한 대기는 스케줄러를 멎게 한다.
        assertThatThrownBy(() -> new RedisConfig(props(null, null)).시간_예산을_확인한다())
                .isInstanceOf(IllegalStateException.class);
    }
}

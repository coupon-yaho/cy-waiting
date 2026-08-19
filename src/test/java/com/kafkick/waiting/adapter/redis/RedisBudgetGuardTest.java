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
        RedisTimeBudget budget =
                RedisTimeBudget.of(props(Duration.ofMillis(500), Duration.ofSeconds(1)));

        // 예외가 안 나는 것이 단언이다. 경계 바로 안쪽 값을 쓴다.
        assertThatCode(budget::verify).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("명령_타임아웃이_틱_이상이면_안_뜬다")
    void 명령_타임아웃이_틱_이상이면_안_뜬다() {
        RedisTimeBudget budget =
                RedisTimeBudget.of(props(Duration.ofSeconds(1), Duration.ofSeconds(1)));

        assertThatThrownBy(budget::verify)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("timeout");
    }

    @Test
    @DisplayName("연결_타임아웃이_리스_이상이면_안_뜬다")
    void 연결_타임아웃이_리스_이상이면_안_뜬다() {
        RedisTimeBudget budget =
                RedisTimeBudget.of(props(Duration.ofMillis(500), Duration.ofSeconds(2)));

        assertThatThrownBy(budget::verify)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connect-timeout");
    }

    @Test
    @DisplayName("설정이_없으면_안_뜬다")
    void 설정이_없으면_안_뜬다() {
        // 기본값은 무한이다. 무한 대기는 스케줄러를 멎게 한다.
        assertThatThrownBy(() -> RedisTimeBudget.of(props(null, null)).verify())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("타임아웃이_0이면_안_뜬다")
    void 타임아웃이_0이면_안_뜬다() {
        // 0 은 예산 안이지만 즉시 실패라 아무것도 못 한다.
        assertThatThrownBy(() -> RedisTimeBudget.of(props(Duration.ZERO, Duration.ofSeconds(1))).verify())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("양수");
        assertThatThrownBy(() -> RedisTimeBudget.of(props(Duration.ofMillis(500), Duration.ZERO)).verify())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("양수");
    }

    @Test
    @DisplayName("타임아웃이_음수면_안_뜬다")
    void 타임아웃이_음수면_안_뜬다() {
        // 음수는 드라이버에 따라 무한 대기가 된다 — 막으려던 것이 그대로 난다.
        assertThatThrownBy(() ->
                RedisTimeBudget.of(props(Duration.ofMillis(-1), Duration.ofSeconds(1))).verify())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("양수");
        assertThatThrownBy(() ->
                RedisTimeBudget.of(props(Duration.ofMillis(500), Duration.ofMillis(-1))).verify())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("양수");
    }

    @Test
    @DisplayName("예산_경계_바로_아래는_뜬다")
    void 예산_경계_바로_아래는_뜬다() {
        // 경계를 초과로만 잡으면 딱 틱만큼 걸리는 명령이 통과한다.
        assertThatCode(() -> RedisTimeBudget.of(
                props(Duration.ofMillis(999), Duration.ofMillis(1999))).verify())
                .doesNotThrowAnyException();
    }
}

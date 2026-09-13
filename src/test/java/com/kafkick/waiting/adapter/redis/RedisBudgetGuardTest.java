package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.ControlPlaneProperties;
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

    private static final ControlPlaneProperties 운영값 = ControlPlaneProperties.defaults();

    /** 연장 시도를 1초로 넓힌 제어 평면. 틱 경계를 연장 경계와 따로 재려고 둔다. */
    private static final ControlPlaneProperties 넓은_연장 = new ControlPlaneProperties(
            운영값.scheduler(),
            new ControlPlaneProperties.Leader(Duration.ofSeconds(5), Duration.ofSeconds(1),
                    Duration.ZERO),
            운영값.capacity());

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
                RedisTimeBudget.of(props(Duration.ofMillis(500), Duration.ofSeconds(1)), 운영값);

        // 예외가 안 나는 것이 단언이다. 경계 바로 안쪽 값을 쓴다.
        assertThatCode(budget::verify).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("명령_타임아웃이_틱_이상이면_안_뜬다")
    void 명령_타임아웃이_틱_이상이면_안_뜬다() {
        RedisTimeBudget budget =
                RedisTimeBudget.of(props(Duration.ofSeconds(1), Duration.ofSeconds(1)), 넓은_연장);

        assertThatThrownBy(budget::verify)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("timeout")
                .hasMessageContaining("틱");
    }

    /**
     * <b>명령 상한이 연장 시도보다 길면 안 뜬다</b> (CY-847). 그 사이의 지연에서는 명령은
     * 되는데 연장만 끊겨, 리스가 지나 리더가 0 이 되고 배분이 통째로 멎는다.
     */
    @Test
    @DisplayName("명령_타임아웃이_연장_시도보다_길면_안_뜬다")
    void 명령_타임아웃이_연장_시도보다_길면_안_뜬다() {
        Duration 시도 = 운영값.leader().attempt();

        assertThatThrownBy(() -> RedisTimeBudget.of(
                props(시도.plusMillis(1), Duration.ofSeconds(1)), 운영값).verify())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("연장");
        assertThatCode(() -> RedisTimeBudget.of(props(시도, Duration.ofSeconds(1)), 운영값).verify())
                .as("같으면 명령과 연장이 같이 끊겨 그 밴드가 없다")
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("연결_타임아웃이_리스_이상이면_안_뜬다")
    void 연결_타임아웃이_리스_이상이면_안_뜬다() {
        RedisTimeBudget budget = RedisTimeBudget.of(
                props(Duration.ofMillis(500), 운영값.leader().lease()), 운영값);

        assertThatThrownBy(budget::verify)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connect-timeout");
    }

    @Test
    @DisplayName("설정이_없으면_안_뜬다")
    void 설정이_없으면_안_뜬다() {
        // 기본값은 무한이다. 무한 대기는 스케줄러를 멎게 한다.
        assertThatThrownBy(() -> RedisTimeBudget.of(props(null, null), 운영값).verify())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("타임아웃이_0이면_안_뜬다")
    void 타임아웃이_0이면_안_뜬다() {
        // 0 은 예산 안이지만 즉시 실패라 아무것도 못 한다.
        assertThatThrownBy(() -> RedisTimeBudget.of(
                props(Duration.ZERO, Duration.ofSeconds(1)), 운영값).verify())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("양수");
        assertThatThrownBy(() -> RedisTimeBudget.of(
                props(Duration.ofMillis(500), Duration.ZERO), 운영값).verify())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("양수");
    }

    @Test
    @DisplayName("타임아웃이_음수면_안_뜬다")
    void 타임아웃이_음수면_안_뜬다() {
        // 음수는 드라이버에 따라 무한 대기가 된다 — 막으려던 것이 그대로 난다.
        assertThatThrownBy(() -> RedisTimeBudget.of(
                props(Duration.ofMillis(-1), Duration.ofSeconds(1)), 운영값).verify())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("양수");
        assertThatThrownBy(() -> RedisTimeBudget.of(
                props(Duration.ofMillis(500), Duration.ofMillis(-1)), 운영값).verify())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("양수");
    }

    @Test
    @DisplayName("예산_경계_바로_아래는_뜬다")
    void 예산_경계_바로_아래는_뜬다() {
        // 경계를 초과로만 잡으면 딱 틱만큼 걸리는 명령이 통과한다.
        assertThatCode(() -> RedisTimeBudget.of(
                props(Duration.ofMillis(999), Duration.ofMillis(4999)), 넓은_연장).verify())
                .doesNotThrowAnyException();
    }
}

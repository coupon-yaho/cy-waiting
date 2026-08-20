package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.chaos.RedisFaults;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 레디스를 끊었다 붙이는 수단이 실제로 동작하는지 본다 (4.0.2).
 *
 * <p>게이트 열 개 중 여섯이 카오스를 요구하는데 이 페이즈에 주입 수단이
 * 없었다. Phase 8 이 다시 만들지 않도록 {@code testFixtures} 에 둔다.
 */
@Tag("chaos")
class RedisFaultsTest {

    private RedisFaults faults;

    @AfterEach
    void 정리() {
        if (faults != null) {
            faults.close();
        }
    }

    @Test
    @DisplayName("끊으면_명령이_실패하고_붙이면_돌아온다")
    void 끊으면_명령이_실패하고_붙이면_돌아온다() {
        faults = RedisFaults.시작한다();

        try (StatefulRedisConnection<String, String> before = faults.연결한다()) {
            assertThat(before.sync().ping()).isEqualTo("PONG");
        }

        faults.끊는다();
        assertThatThrownBy(() -> {
            try (StatefulRedisConnection<String, String> down = faults.연결한다()) {
                down.sync().ping();
            }
            // RuntimeException 으로 두면 픽스처 내부 오류도 통과한다 —
            // "끊겼다" 가 아니라 "무언가 터졌다" 를 재게 된다.
        }).isInstanceOf(RedisConnectionException.class);

        faults.붙인다();
        try (StatefulRedisConnection<String, String> after = faults.연결한다()) {
            assertThat(after.sync().ping()).isEqualTo("PONG");
        }
    }

    @Test
    @DisplayName("붙인_뒤에도_주소가_그대로다")
    void 붙인_뒤에도_주소가_그대로다() {
        // 주소가 바뀌면 붙어 있던 클라이언트가 재연결로 회복되지 못한다 —
        // 그러면 회복 시험이 회복이 아니라 재배선을 검증하게 된다.
        faults = RedisFaults.시작한다();
        String before = faults.주소();

        faults.끊는다();
        faults.붙인다();

        assertThat(faults.주소()).isEqualTo(before);
    }
}

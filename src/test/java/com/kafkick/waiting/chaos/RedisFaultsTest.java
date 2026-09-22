package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.chaos.RedisFaults;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.awaitility.Awaitility;
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
    @DisplayName("끊기_전에_맺은_연결이_스스로_돌아온다")
    void 끊기_전에_맺은_연결이_스스로_돌아온다() {
        // **주소가 유지되는 이유가 이것이다.** 새 연결을 다시 맺어 확인하면
        // 재배선을 재는 것이지 회복을 재는 게 아니다. 장애 전에 맺어 둔
        // 연결이 그대로 살아 돌아와야 회복 시험이 성립한다.
        faults = RedisFaults.시작한다();
        StatefulRedisConnection<String, String> 붙어있던연결 = faults.연결한다();
        assertThat(붙어있던연결.sync().ping()).isEqualTo("PONG");

        faults.끊는다();
        faults.붙인다();

        Awaitility.await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() ->
                        assertThat(붙어있던연결.sync().ping()).isEqualTo("PONG"));
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

    @Test
    @DisplayName("얼리면_명령이_실패하지_않고_매달렸다가_녹이면_끝난다")
    void 얼리면_명령이_실패하지_않고_매달렸다가_녹이면_끝난다() throws Exception {
        // **끊는 것과 다르다.** 끊으면 명령이 곧장 실패할 수 있어 "매달린다" 를 전제로 한 시험이
        // 러너 속도에 갈렸다 (CY-991). 얼리면 연결이 산 채로 명령만 멈춘다.
        faults = RedisFaults.시작한다();
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        assertThat(연결.sync().ping()).isEqualTo("PONG");

        faults.얼린다();
        CompletableFuture<String> 응답 =
                연결.async().ping().toCompletableFuture();
        assertThatThrownBy(() -> 응답.get(500, TimeUnit.MILLISECONDS))
                .as("얼어 있는 동안은 끝나지 않는다 — 실패로 끝나면 이 시한 전에 다른 예외가 난다")
                .isInstanceOf(TimeoutException.class);

        faults.녹인다();
        assertThat(응답.get(10, TimeUnit.SECONDS))
                .as("녹이면 매달렸던 명령이 그대로 끝난다").isEqualTo("PONG");
    }
}

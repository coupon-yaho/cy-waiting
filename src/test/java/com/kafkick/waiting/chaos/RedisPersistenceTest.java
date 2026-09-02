package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 하네스 자기검증. <b>영속이 안 켜졌으면 C12 는 통째로 날아간 상태를 재게 된다</b> —
 * 그러면 "일부만 증발" 이라는 그 시나리오의 전제가 없어지고, 남은 대기자를
 * 재등록자가 추월하는지가 아예 안 재진다.
 */
@Tag("chaos")
class RedisPersistenceTest {

    private static final Duration 한계 = Duration.ofSeconds(30);

    @Test
    @DisplayName("영속을_켜면_죽였다_붙여도_남는다")
    void 영속을_켜면_죽였다_붙여도_남는다() {
        try (RedisFaults faults = RedisFaults.영속으로_시작한다()) {
            try (StatefulRedisConnection<String, String> 연결 = faults.연결한다()) {
                연결.sync().set("c12:probe", "살아남아야 한다");
                // **강제로 내려쓴다.** everysec 은 최대 1초 분량을 잃는데,
                // 그 1초가 이 시험의 판정 대상이 아니다. 여기서 재는 것은
                // 영속이 켜졌는가 하나다.
                연결.sync().bgrewriteaof();
                Awaitility.await().atMost(한계).until(() ->
                        !연결.sync().info("persistence").contains("aof_rewrite_in_progress:1"));
            }

            faults.끊는다();
            faults.붙인다();

            try (StatefulRedisConnection<String, String> 연결 = faults.연결한다()) {
                assertThat(연결.sync().get("c12:probe"))
                        .as("영속이 켜졌으면 kill -9 뒤에도 값이 남는다")
                        .isEqualTo("살아남아야 한다");
            }
        }
    }

    @Test
    @DisplayName("영속을_끄면_죽였다_붙이면_사라진다")
    void 영속을_끄면_죽였다_붙이면_사라진다() {
        try (RedisFaults faults = RedisFaults.시작한다()) {
            try (StatefulRedisConnection<String, String> 연결 = faults.연결한다()) {
                연결.sync().set("c12:probe", "사라져야 한다");
            }

            faults.끊는다();
            faults.붙인다();

            try (StatefulRedisConnection<String, String> 연결 = faults.연결한다()) {
                // 이쪽이 C1 이 쓰는 모양이다. 둘이 안 갈리면 C12 가 C1 을 다시 잰다.
                assertThat(연결.sync().get("c12:probe"))
                        .as("영속이 꺼졌으면 통째로 날아간다").isNull();
            }
        }
    }
}

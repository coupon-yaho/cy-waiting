package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.Delay;
import java.time.Duration;
import java.util.stream.LongStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 레디스 재연결 지연에 상한을 건다 (CY-816).
 *
 * <p>기본값은 두 배씩 늘어 30초까지 간다. 레디스가 오래 죽었다 살아나면 다음 시도까지 그만큼 기다려,
 * 실측에서 첫 스냅샷까지 16초가 걸렸다. 계획서 한계는 5초다.
 */
class RedisReconnectDelayTest {

    private static final Duration 상한 = Duration.ofSeconds(1);

    private Delay 걸린_지연() {
        ClientResources.Builder builder = ClientResources.builder();
        new RedisConfig().redisReconnectDelay().customize(builder);
        ClientResources resources = builder.build();
        try {
            return resources.reconnectDelay();
        } finally {
            resources.shutdown();
        }
    }

    @Test
    @DisplayName("오래_실패해도_재연결_지연이_상한을_안_넘는다")
    void 오래_실패해도_재연결_지연이_상한을_안_넘는다() {
        Delay 지연 = 걸린_지연();

        for (long 시도 = 1; 시도 <= 40; 시도++) {
            assertThat(지연.createDelay(시도)).as("%d 번째 시도", 시도).isLessThanOrEqualTo(상한);
        }
    }

    /** <b>흔들어 준다.</b> 전 노드가 같은 박자로 재연결하면 살아난 레디스를 한꺼번에 때린다. */
    @Test
    @DisplayName("상한에서도_재연결_시각이_노드마다_흩어진다")
    void 상한에서도_재연결_시각이_노드마다_흩어진다() {
        Delay 지연 = 걸린_지연();

        assertThat(LongStream.range(0, 50)
                .mapToObj(i -> 지연.createDelay(30)).distinct().count())
                .as("상한에 닿은 뒤에도 값이 하나로 굳지 않는다").isGreaterThan(1);
    }
}

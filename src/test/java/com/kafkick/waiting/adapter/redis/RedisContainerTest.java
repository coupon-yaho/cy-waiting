package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 실물 레디스에 붙는다.
 *
 * <p>인메모리 대역으로는 Lua 의 복제 동작도 시계도 확인할 수 없다 (TS-3).
 * 이 페이즈가 지키려는 것이 정확히 그 둘이라 대역을 쓰지 않는다.
 */
@Tag("integration")
@SpringBootTest
class RedisContainerTest extends RedisContainerSupport {

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Test
    @DisplayName("컨테이너가_뜨고_PING에_응답한다")
    void 컨테이너가_뜨고_PING에_응답한다() {
        String pong = redis.getConnectionFactory().getReactiveConnection()
                .ping().block(Duration.ofSeconds(5));

        assertThat(pong).isEqualTo("PONG");
    }

    @Test
    @DisplayName("이미지_태그가_7_이상이다")
    void 이미지_태그가_7_이상이다() {
        // TIME 과 효과 기반 복제를 쓴다. 낮은 판에서는 조용히 다르게 동작한다.
        String tag = IMAGE.getVersionPart();
        int major = Integer.parseInt(tag.split("[.-]")[0]);

        assertThat(major).isGreaterThanOrEqualTo(7);
    }

    @Test
    @DisplayName("쓰고_읽으면_같은_값이_나온다")
    void 쓰고_읽으면_같은_값이_나온다() {
        redis.opsForValue().set("probe", "1").block(Duration.ofSeconds(5));

        assertThat(redis.opsForValue().get("probe").block(Duration.ofSeconds(5))).isEqualTo("1");
    }
}

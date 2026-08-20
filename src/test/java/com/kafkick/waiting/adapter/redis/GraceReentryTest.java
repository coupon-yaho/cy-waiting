package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * 유예 재입장은 <b>자리를 보관하지 않는다</b> (D-11 · G3.8).
 *
 * <p>보관하면 이탈자가 돌아올 때마다 <b>성실히 기다린 사람이 밀린다.</b>
 * 불변식 4 는 장애 중에도 적용된다.
 */
@Tag("integration")
@SpringBootTest
class GraceReentryTest extends RedisContainerSupport {

    private static final String NOW = "1800000000";

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String COUPON = "grace";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String GRACE = RedisKeys.grace(COUPON, 1, 0);

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> enqueueScript;

    @BeforeEach
    void 준비() {
        enqueueScript = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE, GRACE).block(WAIT);
    }

    private void enqueue(String memberId) {
        redis.execute(enqueueScript,
                        List.of(QUEUE, MAX_SCORE, RedisKeys.alive(COUPON, 1, 0)),
                        List.of(memberId, "86400", "30", "0", NOW))
                .blockFirst(WAIT);
    }

    private long rankOf(String memberId) {
        Double score = redis.opsForZSet().score(QUEUE, memberId).block(WAIT);
        return redis.opsForZSet()
                .count(QUEUE, Range.leftUnbounded(Range.Bound.exclusive(score)))
                .block(WAIT);
    }

    @Test
    @DisplayName("재입장은_새_순번으로_등록된다")
    void 재입장은_새_순번으로_등록된다() {
        enqueue("leaver");
        double original = redis.opsForZSet().score(QUEUE, "leaver").block(WAIT);

        // 이탈 — 자리를 잃고 유예 기록만 남는다
        redis.opsForZSet().remove(QUEUE, "leaver").block(WAIT);
        redis.opsForHash().put(GRACE, "leaver", "left").block(WAIT);
        enqueue("waiter");

        enqueue("leaver");

        assertThat(redis.opsForZSet().score(QUEUE, "leaver").block(WAIT))
                .isGreaterThan(original);
    }

    @Test
    @DisplayName("재입장이_남은_대기자의_순위를_늘리지_않는다")
    void 재입장이_남은_대기자의_순위를_늘리지_않는다() {
        enqueue("leaver");
        enqueue("waiter1");
        enqueue("waiter2");

        redis.opsForZSet().remove(QUEUE, "leaver").block(WAIT);
        redis.opsForHash().put(GRACE, "leaver", "left").block(WAIT);
        long before1 = rankOf("waiter1");
        long before2 = rankOf("waiter2");

        enqueue("leaver");

        // 이 시퀀스에서는 아무도 빠지지 않으므로 순위는 **그대로**여야 한다.
        // 이하로 두면 예기치 않은 삭제나 재정렬도 통과시킨다.
        assertThat(rankOf("waiter1")).isEqualTo(before1);
        assertThat(rankOf("waiter2")).isEqualTo(before2);
    }

    @Test
    @DisplayName("재입장자는_줄_맨_뒤에_선다")
    void 재입장자는_줄_맨_뒤에_선다() {
        // 자리를 보관하면 성실히 기다린 사람이 밀린다.
        enqueue("leaver");
        redis.opsForZSet().remove(QUEUE, "leaver").block(WAIT);
        for (int i = 0; i < 5; i++) {
            enqueue("w" + i);
        }

        enqueue("leaver");

        assertThat(rankOf("leaver")).isEqualTo(5);
    }

    @Test
    @DisplayName("유예_기록이_있어도_자리를_돌려주지_않는다")
    void 유예_기록이_있어도_자리를_돌려주지_않는다() {
        // 기록은 재방문자 식별용이지 자리 보관용이 아니다.
        enqueue("leaver");
        redis.opsForZSet().remove(QUEUE, "leaver").block(WAIT);
        redis.opsForHash().put(GRACE, "leaver", "left").block(WAIT);
        enqueue("newcomer");

        enqueue("leaver");

        assertThat(rankOf("leaver")).isEqualTo(1);
        assertThat(redis.opsForHash().get(GRACE, "leaver").block(WAIT)).isEqualTo("left");
    }
}

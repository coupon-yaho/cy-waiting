package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * 등록의 생존 신호와 길이 상한.
 *
 * <p>상한은 <b>2차 방어</b>다. 1차는 도메인이 보지만 낡은 스냅샷으로 판정하므로
 * 여기서 한 번 더 본다.
 */
@Tag("integration")
@SpringBootTest
class EnqueueGuardTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String COUPON = "guard";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> script;

    @BeforeEach
    void 준비() {
        script = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE).block(WAIT);
        for (int i = 0; i < 10; i++) {
            redis.delete(RedisKeys.alive(COUPON, 1, 0, "m" + i)).block(WAIT);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Object> enqueue(String memberId, String aliveTtl, String cap) {
        return (List<Object>) redis.execute(
                        script,
                        List.of(QUEUE, MAX_SCORE, RedisKeys.alive(COUPON, 1, 0, memberId)),
                        List.of(memberId, "86400", aliveTtl, cap))
                .blockFirst(WAIT);
    }

    @Test
    @DisplayName("등록하면_생존_키가_TTL과_함께_생긴다")
    void 등록하면_생존_키가_TTL과_함께_생긴다() {
        enqueue("m1", "30", "0");

        assertThat(redis.getExpire(RedisKeys.alive(COUPON, 1, 0, "m1")).block(WAIT))
                .isBetween(Duration.ofSeconds(25), Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("생존_TTL은_주입받는다")
    void 생존_TTL은_주입받는다() {
        // 폴링 간격에서 나오는 값이라 스크립트에 박으면 둘이 갈라진다.
        enqueue("m2", "90", "0");

        assertThat(redis.getExpire(RedisKeys.alive(COUPON, 1, 0, "m2")).block(WAIT))
                .isBetween(Duration.ofSeconds(85), Duration.ofSeconds(90));
    }

    @Test
    @DisplayName("재등록도_생존_신호를_갱신한다")
    void 재등록도_생존_신호를_갱신한다() {
        // 순번은 그대로지만 살아 있다는 신호는 새로 찍혀야 한다.
        // 안 그러면 성실히 새로고침하는 사람이 이탈자로 지워진다.
        enqueue("m3", "30", "0");
        redis.expire(RedisKeys.alive(COUPON, 1, 0, "m3"), Duration.ofSeconds(2)).block(WAIT);

        enqueue("m3", "30", "0");

        assertThat(redis.getExpire(RedisKeys.alive(COUPON, 1, 0, "m3")).block(WAIT))
                .isGreaterThan(Duration.ofSeconds(20));
    }

    @Test
    @DisplayName("상한을_넘으면_등록하지_않고_거부_신호를_낸다")
    void 상한을_넘으면_등록하지_않고_거부_신호를_낸다() {
        enqueue("m1", "30", "2");
        enqueue("m2", "30", "2");

        List<Object> rejected = enqueue("m3", "30", "2");

        assertThat(String.valueOf(rejected.get(0))).isEqualTo("-1");
        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(2);
    }

    @Test
    @DisplayName("이미_선_사람은_상한과_무관하게_자리를_지킨다")
    void 이미_선_사람은_상한과_무관하게_자리를_지킨다() {
        // 줄이 길어진 것이 그 사람 잘못이 아닌데 자리를 잃으면 안 된다.
        enqueue("m1", "30", "2");
        enqueue("m2", "30", "2");
        double first = redis.opsForZSet().score(QUEUE, "m1").block(WAIT);

        List<Object> again = enqueue("m1", "30", "1");

        assertThat(String.valueOf(again.get(2))).isEqualTo("1");
        assertThat(redis.opsForZSet().score(QUEUE, "m1").block(WAIT)).isEqualTo(first);
    }

    @Test
    @DisplayName("상한이_0이면_제한하지_않는다")
    void 상한이_0이면_제한하지_않는다() {
        for (int i = 0; i < 5; i++) {
            enqueue("m" + i, "30", "0");
        }

        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(5);
    }

    @Test
    @DisplayName("잘못된_인자는_아무것도_쓰지_않는다")
    void 잘못된_인자는_아무것도_쓰지_않는다() {
        // Lua 는 중간 오류를 되돌리지 않는다. 쓰기 전에 전부 검증한다.
        assertThatThrownBy(() -> enqueue("m1", "0", "0")).rootCause()
                .hasMessageContaining("alive TTL");
        assertThatThrownBy(() -> enqueue("m1", "30", "-1")).rootCause()
                .hasMessageContaining("큐 길이 상한");

        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isZero();
        assertThat(redis.hasKey(MAX_SCORE).block(WAIT)).isFalse();
        // alive 만 생기는 회귀를 잡는다. 검증이 첫 쓰기 앞이라는 계약은
        // 세 키 전부에 걸린다 — 하나라도 새면 계약이 아니다.
        assertThat(redis.hasKey(RedisKeys.alive(COUPON, 1, 0, "m1")).block(WAIT)).isFalse();
    }
}

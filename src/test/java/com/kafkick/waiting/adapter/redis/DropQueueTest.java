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
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 매진 큐의 실제 삭제 (7.3 · 5.3.1).
 *
 * <p><b>이 저장소에서 되돌릴 수 없는 유일한 쓰기다.</b> 지운 줄을 되살리는
 * 코드가 없으므로, 지우기 직전에 재고를 다시 보고 살아난 줄은 안 지운다.
 */
@Tag("integration")
@SpringBootTest
class DropQueueTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String COUPON = "drop-me";

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private AllocationRedisPort port;

    @BeforeEach
    void 준비() {
        port = AllocationRedisPort.of(redis, 1);
        redis.delete(RedisKeys.queue(COUPON, 1, 0), RedisKeys.alive(COUPON, 1, 0),
                RedisKeys.stock(COUPON), RedisKeys.admitted(COUPON, 1, 0),
                RedisKeys.grace(COUPON, 1, 0), RedisKeys.maxScore(COUPON, 1, 0)).block(WAIT);
    }

    private void 줄을_세운다() {
        redis.opsForZSet().add(RedisKeys.queue(COUPON, 1, 0), "m1", 100).block(WAIT);
        redis.opsForZSet().add(RedisKeys.alive(COUPON, 1, 0), "m1", 200).block(WAIT);
        redis.opsForValue().set(RedisKeys.admitted(COUPON, 1, 0), "50").block(WAIT);
        redis.opsForHash().put(RedisKeys.grace(COUPON, 1, 0), "m0", "a:100").block(WAIT);
        redis.opsForValue().set(RedisKeys.maxScore(COUPON, 1, 0), "999").block(WAIT);
    }

    private boolean 있나(String key) {
        return Boolean.TRUE.equals(redis.hasKey(key).block(WAIT));
    }

    @Test
    @DisplayName("매진된_큐를_지운다")
    void 매진된_큐를_지운다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of(COUPON)).block(WAIT)).containsExactly(COUPON);

        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).as("줄").isFalse();
        assertThat(있나(RedisKeys.alive(COUPON, 1, 0))).as("생존 신호").isFalse();
    }

    /**
     * <b>임계·유예·바닥값은 남긴다</b> (5.3 표). 임계를 지우면 이미 입장한
     * 사람이 두 번째 토큰을 받고, 바닥값을 지우면 새 score 가 앞으로 가
     * 줄 선 사람이 통째로 추월당한다.
     */
    @Test
    @DisplayName("지우는_것은_줄과_생존_신호뿐이다")
    void 지우는_것은_줄과_생존_신호뿐이다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        port.dropSoldOutQueues(List.of(COUPON)).block(WAIT);

        assertThat(있나(RedisKeys.admitted(COUPON, 1, 0))).as("입장 임계").isTrue();
        assertThat(있나(RedisKeys.grace(COUPON, 1, 0))).as("유예 기록").isTrue();
        assertThat(있나(RedisKeys.maxScore(COUPON, 1, 0))).as("시계 바닥값").isTrue();
    }

    /**
     * <b>지우기 직전에 재고를 다시 본다</b> (5.3.1 · CY-765).
     *
     * <p>수집과 삭제 사이에 재입고되면 살아난 줄을 지운다. 메모리 안의 취소는
     * 다음 스냅샷이 와야 도는데 삭제는 그 전에 나간다.
     */
    @Test
    @DisplayName("재입고됐으면_안_지운다")
    void 재입고됐으면_안_지운다() {
        줄을_세운다();
        // 판이 시작될 때는 매진이었고, 지우기 직전에 재고가 돌아왔다.
        redis.opsForValue().set(RedisKeys.stock(COUPON), "5").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of(COUPON)).block(WAIT))
                .as("지운 것만 돌려준다").isEmpty();

        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).as("줄이 살아 있다").isTrue();
        assertThat(있나(RedisKeys.alive(COUPON, 1, 0))).as("생존 신호도").isTrue();
    }

    /**
     * <b>재고를 못 읽으면 안 지운다.</b> 못 읽은 것은 매진이 아니다 (CY-702).
     * 여기서 지우면 재고 키를 잃은 쿠폰의 줄이 통째로 사라진다.
     */
    @Test
    @DisplayName("재고를_못_읽으면_안_지운다")
    void 재고를_못_읽으면_안_지운다() {
        줄을_세운다();
        // 재고 키가 없다.

        assertThat(port.dropSoldOutQueues(List.of(COUPON)).block(WAIT)).isEmpty();

        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).as("줄이 살아 있다").isTrue();
    }

    /** 수가 아닌 재고도 못 읽은 것이다. 0 으로 읽으면 그 줄이 사라진다. */
    @Test
    @DisplayName("재고가_수가_아니면_안_지운다")
    void 재고가_수가_아니면_안_지운다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "몇 개더라").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of(COUPON)).block(WAIT)).isEmpty();

        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).isTrue();
    }

    /** 한 쿠폰이 살아나도 나머지는 지운다. 판 하나가 통째로 멎으면 안 된다. */
    @Test
    @DisplayName("살아난_쿠폰만_건너뛴다")
    void 살아난_쿠폰만_건너뛴다() {
        String 산것 = "alive-one";
        redis.delete(RedisKeys.queue(산것, 1, 0), RedisKeys.stock(산것)).block(WAIT);
        redis.opsForZSet().add(RedisKeys.queue(산것, 1, 0), "m1", 100).block(WAIT);
        redis.opsForValue().set(RedisKeys.stock(산것), "7").block(WAIT);
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of(산것, COUPON)).block(WAIT))
                .containsExactly(COUPON);

        assertThat(있나(RedisKeys.queue(산것, 1, 0))).as("살아난 쪽은 그대로").isTrue();
        redis.delete(RedisKeys.queue(산것, 1, 0), RedisKeys.stock(산것)).block(WAIT);
    }
}

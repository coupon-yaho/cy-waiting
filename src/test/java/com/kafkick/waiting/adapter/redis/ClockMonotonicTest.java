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
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * 시계가 뒤로 가도 줄 선 사람을 추월시키지 않는다 (F2 · 불변식 4).
 *
 * <p>순번이 카운터가 아니라 <b>벽시계</b>라(A-9) NTP 보정이나 복제본 승격으로
 * 뒤로 갈 수 있다. 실제로 시계를 돌리지 않고 시험한다 — 바닥값을 미래로 두는
 * 것이 <b>시계가 그만큼 뒤처진 것과 같다.</b>
 */
@Tag("integration")
@SpringBootTest
class ClockMonotonicTest extends RedisContainerSupport {

    private static final long TTL_SECONDS = 86_400;
    private static final Duration WAIT = Duration.ofSeconds(5);

    private static final String COUPON = "c1";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> script;

    @BeforeEach
    void 준비() {
        script = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE).block(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> enqueue(String memberId) {
        return (List<Object>) redis.execute(
                        script,
                        List.of(QUEUE, MAX_SCORE),
                        List.of(memberId, String.valueOf(TTL_SECONDS)))
                .blockFirst(WAIT);
    }

    private long scoreOf(String memberId) {
        return redis.opsForZSet().score(QUEUE, memberId).block(WAIT).longValue();
    }

    private long appliedFlag(List<Object> result) {
        return Long.parseLong(String.valueOf(result.get(1)));
    }

    @Test
    @DisplayName("정상_등록은_바닥값을_적용하지_않는다")
    void 정상_등록은_바닥값을_적용하지_않는다() {
        List<Object> result = enqueue("m1");

        assertThat(result).hasSize(2);
        assertThat(appliedFlag(result)).isZero();
        assertThat(scoreOf("m1")).isPositive();
    }

    @Test
    @DisplayName("시계를_되돌려도_뒤에_온_사람이_앞서지_않는다")
    void 시계를_되돌려도_뒤에_온_사람이_앞서지_않는다() {
        enqueue("A");
        long scoreA = scoreOf("A");

        // 바닥값을 A 보다 한참 앞에 둔다 — 시계가 그만큼 뒤처진 상태와 같다.
        redis.opsForValue().set(MAX_SCORE, String.valueOf(scoreA + 10_000_000)).block(WAIT);
        enqueue("B");

        assertThat(scoreOf("B")).isGreaterThan(scoreA);
    }

    @Test
    @DisplayName("큐가_빈_동안_시계가_되돌아가도_막힌다")
    void 큐가_빈_동안_시계가_되돌아가도_막힌다() {
        enqueue("A");
        long scoreA = scoreOf("A");

        // 전원 입장 — ZSET 은 비지만 바닥값은 남는다. ZSET 의 마지막 원소를
        // 읽는 방식으로는 이 경우를 못 막는다. 그게 이 키의 존재 이유다.
        redis.delete(QUEUE).block(WAIT);
        redis.opsForValue().set(MAX_SCORE, String.valueOf(scoreA + 10_000_000)).block(WAIT);
        enqueue("B");

        assertThat(scoreOf("B")).isGreaterThan(scoreA);
    }

    @Test
    @DisplayName("바닥값이_적용되면_그_사실이_반환된다")
    void 바닥값이_적용되면_그_사실이_반환된다() {
        // 조용히 보정하지 않는다. 시계가 뒤로 간 사실을 알 수 있어야
        // "순서는 맞는데 왜 다 같은 score 인가" 를 나중에 밝힐 수 있다.
        enqueue("A");
        redis.opsForValue().set(MAX_SCORE, String.valueOf(scoreOf("A") + 60_000_000)).block(WAIT);

        assertThat(appliedFlag(enqueue("B"))).isOne();
    }

    @Test
    @DisplayName("연속_등록에서_순번이_단조_증가한다")
    void 연속_등록에서_순번이_단조_증가한다() {
        // 같은 마이크로초에 둘이 들어와도 뒤엣것이 앞서면 안 된다.
        long previous = 0;
        for (int i = 0; i < 200; i++) {
            enqueue("m" + i);
            long score = scoreOf("m" + i);
            assertThat(score).isGreaterThan(previous);
            previous = score;
        }
    }

    @Test
    @DisplayName("바닥값에_TTL이_걸린다")
    void 바닥값에_TTL이_걸린다() {
        // 오픈 중에는 등록마다 밀려나 안 사라지고, 끝난 쿠폰은 하루 뒤 사라진다.
        // 쿠폰 일정을 몰라도 성립한다.
        enqueue("m1");

        assertThat(redis.getExpire(MAX_SCORE).block(WAIT))
                .isBetween(Duration.ofSeconds(TTL_SECONDS - 10), Duration.ofSeconds(TTL_SECONDS));
    }
}

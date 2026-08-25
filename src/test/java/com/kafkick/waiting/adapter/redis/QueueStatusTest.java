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
 * 순번 조회.
 *
 * <p>조회·하트비트·배수 판정이 <b>한 번에</b> 일어나야 한다. 나눠 치면 한쪽만
 * 성공한 상태가 생기고, 그때 성실히 새로고침하는 사람이 이탈자로 지워진다.
 */
@Tag("integration")
@SpringBootTest
class QueueStatusTest extends RedisContainerSupport {

    private static final String NOW = "1800000000";

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String COUPON = "status";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String ADMITTED = RedisKeys.admitted(COUPON, 1, 0);
    private static final String GRACE = RedisKeys.grace(COUPON, 1, 0);
    private static final String ALIVE_TTL = "30";

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> enqueueScript;
    private RedisScript<List> statusScript;

    @BeforeEach
    void 준비() {
        enqueueScript = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        statusScript = RedisScript.of(new ClassPathResource("redis/queue_status.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE, ADMITTED, GRACE).block(WAIT);
        for (int i = 0; i < 10; i++) {
            redis.delete(alive("m" + i)).block(WAIT);
        }
    }

    private String alive(String memberId) {
        return RedisKeys.alive(COUPON, 1, 0);
    }

    private void enqueue(String memberId) {
        redis.execute(enqueueScript, List.of(QUEUE, MAX_SCORE, alive(memberId), ADMITTED),
                        List.of(memberId, "86400", ALIVE_TTL, "-1", NOW))
                .blockFirst(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> status(String memberId) {
        return (List<Object>) redis.execute(
                        statusScript,
                        List.of(QUEUE, ADMITTED, alive(memberId), GRACE),
                        List.of(memberId, ALIVE_TTL, NOW))
                .blockFirst(WAIT);
    }

    private String state(List<Object> r) {
        return String.valueOf(r.get(0));
    }

    private long rank(List<Object> r) {
        return Long.parseLong(String.valueOf(r.get(1)));
    }

    @Test
    @DisplayName("내_앞에_몇_명인지_돌려준다")
    void 내_앞에_몇_명인지_돌려준다() {
        enqueue("m0");
        enqueue("m1");
        enqueue("m2");

        assertThat(rank(status("m0"))).isZero();
        assertThat(rank(status("m1"))).isEqualTo(1);
        assertThat(rank(status("m2"))).isEqualTo(2);
    }

    @Test
    @DisplayName("큐에_없으면_NOT_QUEUED를_받는다")
    void 큐에_없으면_NOT_QUEUED를_받는다() {
        // 0번째와 구분한다. 없는 것과 맨 앞인 것은 다르다 — 뭉치면
        // 유실된 사람에게 "곧 입장" 을 보여 주게 된다.
        List<Object> result = status("ghost");

        assertThat(state(result)).isEqualTo("NOT_QUEUED");
        assertThat(rank(result)).isEqualTo(-1);
    }

    @Test
    @DisplayName("조회하면_생존_TTL이_연장된다")
    void 조회하면_생존_TTL이_연장된다() {
        // **경계에 붙이지 않는다.** 2초로 줄여 두면 CI 지연 한 번에 키가
        // 먼저 사라져 갱신이 아예 안 돌고, 그러면 이 시험이 불안정해진다.
        enqueue("m0");
        redis.opsForZSet().add(alive("m0"), "m0", Long.parseLong(NOW) - 100).block(WAIT);

        assertThat(state(status("m0"))).isEqualTo("WAITING");

        // score 가 만료 시각이다. 조회 한 번에 미래로 밀려야 한다.
        assertThat(redis.opsForZSet().score(alive("m0"), "m0").block(WAIT))
                .isEqualTo(Long.parseLong(NOW) + 30);
    }

    @Test
    @DisplayName("임계가_없으면_아무도_입장하지_않는다")
    void 임계가_없으면_아무도_입장하지_않는다() {
        enqueue("m0");

        assertThat(state(status("m0"))).isEqualTo("WAITING");
    }

    @Test
    @DisplayName("내_순번이_임계_이하면_입장이다")
    void 내_순번이_임계_이하면_입장이다() {
        enqueue("m0");
        enqueue("m1");
        double scoreM0 = redis.opsForZSet().score(QUEUE, "m0").block(WAIT);
        redis.opsForValue().set(ADMITTED, String.valueOf((long) scoreM0)).block(WAIT);

        assertThat(state(status("m0"))).isEqualTo("ADMITTED");
        assertThat(state(status("m1"))).isEqualTo("WAITING");
    }

    @Test
    @DisplayName("입장이_확정되면_큐에서_빠진다")
    void 입장이_확정되면_큐에서_빠진다() {
        // 안 빼면 대기 인원이 계속 부풀고 ETA 가 전부 틀어진다.
        enqueue("m0");
        enqueue("m1");
        double scoreM0 = redis.opsForZSet().score(QUEUE, "m0").block(WAIT);
        redis.opsForValue().set(ADMITTED, String.valueOf((long) scoreM0)).block(WAIT);

        status("m0");

        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(1);
        assertThat(rank(status("m1"))).isZero();
    }

    @Test
    @DisplayName("입장한_사람은_다시_물어도_입장이다")
    void 입장한_사람은_다시_물어도_입장이다() {
        enqueue("m0");
        redis.opsForValue()
                .set(ADMITTED, String.valueOf(redis.opsForZSet().score(QUEUE, "m0").block(WAIT).longValue()))
                .block(WAIT);
        status("m0");

        // **응답을 놓치면 복구 수단이 없다.** 탭이 둘이거나 재시도만 해도 큐에는
        // 이미 없으니, 여기서 매진을 내면 자기 차례를 받은 사람이 다시 서야 하고
        // 그동안 온 사람들 뒤로 간다.
        assertThat(state(status("m0"))).isEqualTo("ADMITTED");

        // 다만 입장 처리는 한 번뿐이다 — 유예 기록만 읽고 돌아간다.
        assertThat(redis.opsForZSet().score(QUEUE, "m0").block(WAIT)).isNull();
        assertThat(redis.opsForHash().get(GRACE, "m0").block(WAIT)).isEqualTo("admitted");
    }

    @Test
    @DisplayName("입장한_사람은_유예_기록에_남는다")
    void 입장한_사람은_유예_기록에_남는다() {
        enqueue("m0");
        redis.opsForValue()
                .set(ADMITTED, String.valueOf(redis.opsForZSet().score(QUEUE, "m0").block(WAIT).longValue()))
                .block(WAIT);

        status("m0");

        assertThat(redis.opsForHash().get(GRACE, "m0").block(WAIT)).isEqualTo("admitted");
    }

    @Test
    @DisplayName("앞사람이_빠지면_내_순번이_줄어든다")
    void 앞사람이_빠지면_내_순번이_줄어든다() {
        // 순위를 저장하지 않고 매번 센다. 저장하면 앞사람이 빠질 때마다
        // 전원을 갱신해야 한다.
        for (int i = 0; i < 5; i++) {
            enqueue("m" + i);
        }
        assertThat(rank(status("m4"))).isEqualTo(4);

        redis.opsForZSet().remove(QUEUE, "m0", "m1").block(WAIT);

        assertThat(rank(status("m4"))).isEqualTo(2);
    }

    @Test
    @DisplayName("잘못된_TTL은_아무것도_쓰지_않는다")
    void 잘못된_TTL은_아무것도_쓰지_않는다() {
        enqueue("m0");
        double before = redis.opsForZSet().score(QUEUE, "m0").block(WAIT);
        redis.delete(alive("m0")).block(WAIT);

        assertThatThrownBy(() ->
                redis.execute(statusScript, List.of(QUEUE, ADMITTED, alive("m0"), GRACE),
                                List.of("m0", "0", NOW))
                        .blockFirst(WAIT))
                .rootCause()
                .hasMessageContaining("alive TTL");

        // 쓰기 대상 전부를 본다. 하나만 보면 ZREM 이나 HSET 이 먼저
        // 일어난 회귀를 통과시킨다. 순번은 **그대로**여야 한다 —
        // 있기만 하면 되는 게 아니라 값이 안 바뀌어야 한다.
        assertThat(redis.opsForZSet().score(alive("m0"), "m0").block(WAIT)).isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "m0").block(WAIT)).isEqualTo(before);
        assertThat(redis.opsForHash().hasKey(GRACE, "m0").block(WAIT)).isFalse();
    }
}

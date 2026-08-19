package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
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
 * 새로고침 연타에도 자리는 하나다 (G3.1).
 *
 * <p>자리가 둘이 되면 대기 인원이 부풀고 <b>ETA 가 전부 틀어진다.</b> 조회와
 * 등록을 나누면 그 사이에 다른 요청이 끼어들어 실제로 그렇게 된다 — Lua 로
 * 묶는 이유가 이것이다.
 */
@Tag("integration")
@SpringBootTest
class ConcurrentEnqueueTest extends RedisContainerSupport {

    private static final long TTL_SECONDS = 86_400;
    private static final Duration WAIT = Duration.ofSeconds(10);

    private static final String COUPON = "c1";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String ALIVE_TTL = "30";
    private static final String NO_CAP = "0";

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> script;

    @BeforeEach
    void 준비() {
        script = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE).block(WAIT);
    }

    private String alive(String memberId) {
        return RedisKeys.alive(COUPON, 1, 0, memberId);
    }

    private void enqueue(String memberId) {
        redis.execute(script, List.of(QUEUE, MAX_SCORE, alive(memberId)),
                        List.of(memberId, String.valueOf(TTL_SECONDS), ALIVE_TTL, NO_CAP))
                .blockFirst(WAIT);
    }

    /** 스레드를 동시에 풀어 실제 경합을 만든다. 순차 반복은 이 결함을 못 잡는다. */
    private void 동시에(int threads, IntConsumer body) throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                int index = i;
                pool.execute(() -> {
                    try {
                        start.await();
                        body.accept(index);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @DisplayName("같은_사용자_100_동시_등록에서_자리가_정확히_1개다")
    void 같은_사용자_100_동시_등록에서_자리가_정확히_1개다() throws InterruptedException {
        동시에(100, i -> enqueue("same-member"));

        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(1);
    }

    @Test
    @DisplayName("동시_등록에서도_순번이_겹치지_않는다")
    void 동시_등록에서도_순번이_겹치지_않는다() throws InterruptedException {
        // 같은 마이크로초에 여럿이 들어오면 score 가 겹칠 수 있다. 겹치면
        // ZSET 이 사전순으로 재정렬해 **등록 순서와 다른 줄**이 된다.
        int people = 200;
        동시에(people, i -> enqueue("m" + i));

        Set<Double> scores = ConcurrentHashMap.newKeySet();
        redis.opsForZSet().rangeWithScores(QUEUE, Range.closed(0L, (long) people))
                .doOnNext(tuple -> scores.add(tuple.getScore()))
                .blockLast(WAIT);

        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(people);
        assertThat(scores).hasSize(people);
    }

    @Test
    @DisplayName("서로_다른_사용자가_동시에_와도_전원_자리를_받는다")
    void 서로_다른_사용자가_동시에_와도_전원_자리를_받는다() throws InterruptedException {
        동시에(100, i -> enqueue("member-" + i));

        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(100);
    }

    @Test
    @DisplayName("동시_재등록이_원래_순번을_흔들지_않는다")
    void 동시_재등록이_원래_순번을_흔들지_않는다() throws InterruptedException {
        enqueue("m1");
        double first = redis.opsForZSet().score(QUEUE, "m1").block(WAIT);

        동시에(50, i -> enqueue("m1"));

        assertThat(redis.opsForZSet().score(QUEUE, "m1").block(WAIT)).isEqualTo(first);
        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(1);
    }
}

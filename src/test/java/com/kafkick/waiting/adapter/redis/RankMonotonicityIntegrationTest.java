package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
 * <b>순위는 뒤로 가지 않는다</b> (G3.11 · 불변식 3).
 *
 * <p>Phase 2 의 단조성 테스트는 <b>"입력이 단조면 출력도 단조"</b> 까지만 봤다.
 * {@code localRank} 자체가 단조라는 보장은 여기가 진다 — 등록·입장·이탈이
 * 섞인 실제 시퀀스에서 {@code ZCOUNT} 가 늘지 않는지 본다.
 */
@Tag("integration")
@SpringBootTest
class RankMonotonicityIntegrationTest extends RedisContainerSupport {

    private static final long SEED = 20260819L;
    private static final int OPERATIONS = 100_000;
    private static final int PEOPLE = 300;
    private static final Duration WAIT = Duration.ofSeconds(10);

    private static final String COUPON = "rank";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String ADMITTED = RedisKeys.admitted(COUPON, 1, 0);
    private static final String GRACE = RedisKeys.grace(COUPON, 1, 0);

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> enqueueScript;

    @BeforeEach
    void 준비() {
        enqueueScript = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE, ADMITTED, GRACE).block(WAIT);
    }

    private void enqueue(String memberId) {
        redis.execute(enqueueScript,
                        List.of(QUEUE, MAX_SCORE, RedisKeys.alive(COUPON, 1, 0, memberId)),
                        List.of(memberId, "86400", "30", "0"))
                .blockFirst(WAIT);
    }

    @Test
    @DisplayName("등록_입장_이탈을_섞은_10만_시퀀스에서_순위가_증가하지_않는다")
    void 등록_입장_이탈을_섞은_10만_시퀀스에서_순위가_증가하지_않는다() {
        Random rnd = new Random(SEED);
        List<String> waiting = new ArrayList<>();
        Map<String, Long> lastRank = new HashMap<>();
        int violations = 0;

        for (int op = 0; op < OPERATIONS; op++) {
            int action = rnd.nextInt(100);

            if (action < 50 && waiting.size() < PEOPLE) {
                // 등록 — 뒤에 붙는다. 앞선 사람의 순위는 안 바뀐다.
                String id = "m" + op;
                enqueue(id);
                waiting.add(id);
            } else if (action < 80 && !waiting.isEmpty()) {
                // 입장 — 맨 앞이 빠진다. 뒤엣사람 순위가 하나씩 줄어든다.
                String front = waiting.remove(0);
                redis.opsForZSet().remove(QUEUE, front).block(WAIT);
                lastRank.remove(front);
            } else if (!waiting.isEmpty()) {
                // 이탈 — 중간에서 빠진다. 뒤엣사람 순위가 줄어든다.
                String gone = waiting.remove(rnd.nextInt(waiting.size()));
                redis.opsForZSet().remove(QUEUE, gone).block(WAIT);
                lastRank.remove(gone);
            }

            if (waiting.isEmpty()) {
                continue;
            }

            // 표본 하나만 확인한다. 매번 전원을 세면 10만 회가 안 끝난다.
            String sample = waiting.get(rnd.nextInt(waiting.size()));
            Double score = redis.opsForZSet().score(QUEUE, sample).block(WAIT);
            if (score == null) {
                continue;
            }
            long rank = redis.opsForZSet()
                    .count(QUEUE, org.springframework.data.domain.Range.leftUnbounded(
                            org.springframework.data.domain.Range.Bound.exclusive(score)))
                    .block(WAIT);

            Long previous = lastRank.put(sample, rank);
            if (previous != null && rank > previous) {
                violations++;
            }
        }

        assertThat(violations)
                .withFailMessage("순위 역행 %d 건 (시드 %d)", violations, SEED)
                .isZero();
    }
}

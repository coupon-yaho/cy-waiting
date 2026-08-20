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
 * {@code localRank} 자체가 단조라는 보장은 여기가 진다.
 *
 * <p><b>매 연산마다 전원을 본다.</b> 표본 하나만 보면 안 본 사람의 순위가
 * 올랐다가 다음 표본 선택 전에 내려오는 경우를 통째로 놓친다.
 */
@Tag("integration")
@SpringBootTest
class RankMonotonicityIntegrationTest extends RedisContainerSupport {

    private static final long SEED = 20260819L;
    private static final int OPERATIONS = 3_000;
    private static final int PEOPLE = 200;
    private static final Duration WAIT = Duration.ofSeconds(10);

    private static final String COUPON = "rank";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> enqueueScript;

    @BeforeEach
    void 준비() {
        enqueueScript = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE).block(WAIT);
    }

    private void enqueue(String memberId) {
        redis.execute(enqueueScript,
                        List.of(QUEUE, MAX_SCORE, RedisKeys.alive(COUPON, 1, 0, memberId)),
                        List.of(memberId, "86400", "30", "0"))
                .blockFirst(WAIT);
    }

    /** 큐 전체를 순서대로 한 번에 읽는다. 사람마다 왕복하면 10만 회가 안 끝난다. */
    private List<String> queueOrder() {
        return redis.opsForZSet().range(QUEUE, org.springframework.data.domain.Range.closed(0L, -1L))
                .collectList()
                .block(WAIT);
    }

    @Test
    @DisplayName("등록_입장_이탈을_섞은_시퀀스에서_순위가_증가하지_않는다")
    void 등록_입장_이탈을_섞은_시퀀스에서_순위가_증가하지_않는다() {
        Random rnd = new Random(SEED);
        List<String> expected = new ArrayList<>();
        Map<String, Integer> lastRank = new HashMap<>();
        List<String> violations = new ArrayList<>();

        for (int op = 0; op < OPERATIONS; op++) {
            int action = rnd.nextInt(100);

            if (action < 50 && expected.size() < PEOPLE) {
                String id = "m" + op;
                enqueue(id);
                expected.add(id);
            } else if (action < 80 && !expected.isEmpty()) {
                // 입장 — 맨 앞이 빠진다
                String front = expected.remove(0);
                redis.opsForZSet().remove(QUEUE, front).block(WAIT);
                lastRank.remove(front);
            } else if (!expected.isEmpty()) {
                // 이탈 — 중간에서 빠진다
                String gone = expected.remove(rnd.nextInt(expected.size()));
                redis.opsForZSet().remove(QUEUE, gone).block(WAIT);
                lastRank.remove(gone);
            }

            List<String> actual = queueOrder();

            // **기대한 줄과 실제 줄이 같아야 한다.** 다르면 사라졌거나 순서가
            // 뒤집힌 것이고, 둘 다 그냥 넘기면 안 되는 사고다.
            if (!actual.equals(expected)) {
                violations.add("op %d: 줄이 어긋났다 — 기대 %s / 실제 %s"
                        .formatted(op, expected, actual));
                break;
            }

            // 매 연산마다 **전원**의 순위를 본다
            for (int rank = 0; rank < actual.size(); rank++) {
                String member = actual.get(rank);
                Integer previous = lastRank.put(member, rank);
                if (previous != null && rank > previous) {
                    violations.add("op %d: %s 순위 %d → %d".formatted(op, member, previous, rank));
                }
            }
        }

        assertThat(violations)
                .withFailMessage("순위 역행 %d 건 (시드 %d)%n%s",
                        violations.size(), SEED, String.join("\n", violations))
                .isEmpty();
    }
}

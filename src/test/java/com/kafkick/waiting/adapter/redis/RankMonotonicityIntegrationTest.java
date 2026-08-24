package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
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
class RankMonotonicityIntegrationTest {

    private static final String NOW = "1800000000";

    private static final long SEED = 20260819L;
    private static final int OPERATIONS = 3_000;
    private static final int PEOPLE = 200;
    /**
     * 한 명령을 기다려 주는 시간.
     *
     * <p>이 하네스는 쓰기를 6천 번 몰아친다. {@code appendfsync everysec} 인
     * 레디스가 느린 디스크를 만나면 그동안 초 단위로 멎는다 — 붐비는 러너에서
     * 실제로 5초를 넘겼다. 재는 것은 지연이 아니라 순서다.
     */
    private static final Duration WAIT = Duration.ofSeconds(30);

    private static final String COUPON = "rank";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);

    /**
     * <b>연결을 직접 만든다.</b> 운영의 500ms 는 요청 경로 예산인데 여기는 연산
     * 3,000 건을 순차로 왕복하는 하네스라 운영이 안 만드는 부하를 스스로 만든다.
     * 붐비는 CI 러너에서 명령 하나가 예산을 넘어 실제로 깨졌다.
     *
     * <p>{@link RedisTimeBudget} 은 긴 값으로 기동을 막는다. 옳은 규칙이라
     * 시험이 우회할 대상이 아니다. 재는 것은 정렬 집합의 순서뿐이다.
     */
    private static final Duration BULK_TIMEOUT = WAIT;

    /**
     * <b>이 시험만의 레디스다.</b> 재는 것은 정렬 집합의 순서지 내구성이 아니다.
     *
     * <p>공유 컨테이너는 운영 설정이라 매초 fsync 를 한다. 쓰기를 6천 번 몰아치는
     * 이 하네스가 그 fsync 에 걸려 명령 하나가 30초를 넘겼다 — 세 번 연속으로.
     */
    @SuppressWarnings("resource")   // JVM 종료까지 살려 둔다
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
                    .withExposedPorts(6379)
                    .withCommand("redis-server", "--appendonly", "no", "--save", "");

    static {
        REDIS.start();
    }

    private static LettuceConnectionFactory factory;
    private static ReactiveStringRedisTemplate redis;

    @BeforeAll
    static void 연결() {
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379)),
                LettuceClientConfiguration.builder().commandTimeout(BULK_TIMEOUT).build());
        factory.afterPropertiesSet();
        redis = new ReactiveStringRedisTemplate(factory);
    }

    @AfterAll
    static void 정리() {
        factory.destroy();
    }

    private RedisScript<List> enqueueScript;

    @BeforeEach
    void 준비() {
        enqueueScript = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE).block(WAIT);
    }

    private void enqueue(String memberId) {
        redis.execute(enqueueScript,
                        List.of(QUEUE, MAX_SCORE, RedisKeys.alive(COUPON, 1, 0)),
                        List.of(memberId, "86400", "30", "0", NOW))
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

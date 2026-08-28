package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * <b>순위는 뒤로 가지 않는다</b> (G3.11 · 불변식 3).
 *
 * <p>Phase 2 는 "입력이 단조면 출력도 단조" 까지만 봤다. 순위 자체가 단조라는
 * 보장은 여기가 진다. <b>매 연산마다 전원을 본다</b> — 표본 하나만 보면 안 본
 * 사람의 순위가 올랐다가 다음 표본 전에 내려오는 경우를 놓친다.
 *
 * <p>스크립트를 직접 부른다. 반응형 경로는 {@code QueueRedisPortTest} 가 진다.
 */
@Tag("integration")
class RankMonotonicityIntegrationTest {

    private static final String NOW = "1800000000";

    private static final long SEED = 20260819L;

    /**
     * 연산 수.
     *
     * <p>블로킹 클라이언트로 바꾸고 되돌렸다. 반응형을 수천 번 감쌌을 때는 800
     * 으로 줄여도 멎었는데, 지금은 3,000 이 몇 초에 끝난다.
     */
    private static final int OPERATIONS = 3_000;

    private static final int PEOPLE = 200;
    /**
     * 한 명령을 기다려 주는 시간.
     *
     * <p>재는 것은 지연이 아니라 순서다. 붐비는 러너에서 명령 하나가 초 단위로
     * 멎어도 순서는 그대로다.
     */
    private static final Duration WAIT = Duration.ofSeconds(30);

    private static final String COUPON = "rank";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);

    /**
     * <b>연결을 직접 만든다.</b> 운영의 500ms 는 요청 경로 예산인데 여기는 연산을
     * 순차로 몰아치는 하네스라 운영이 안 만드는 부하를 스스로 만든다.
     *
     * <p>{@link RedisTimeBudget} 은 긴 값으로 기동을 막는다. 옳은 규칙이라
     * 시험이 우회할 대상이 아니다. 재는 것은 정렬 집합의 순서뿐이다.
     */
    private static final Duration BULK_TIMEOUT = WAIT;

    /**
     * <b>이 시험만의 레디스다.</b> 재는 것은 정렬 집합의 순서지 내구성이 아니다.
     *
     * <p>공유 컨테이너는 운영 설정이라 매초 fsync 를 하고, 몰아치는 쓰기가 거기
     * 걸려 명령 하나가 30초를 넘겼다 — 세 번 연속으로.
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
    private static StringRedisTemplate redis;

    @BeforeAll
    static void 연결() {
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379)),
                LettuceClientConfiguration.builder().commandTimeout(BULK_TIMEOUT).build());
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
    }

    @AfterAll
    static void 정리() {
        factory.destroy();
    }

    private RedisScript<List> enqueueScript;

    @BeforeEach
    void 준비() {
        enqueueScript = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        redis.delete(List.of(QUEUE, MAX_SCORE));
    }

    private void enqueue(String memberId) {
        redis.execute(enqueueScript,
                List.of(QUEUE, MAX_SCORE, RedisKeys.alive(COUPON, 1, 0),
                        RedisKeys.admitted(COUPON, 1, 0), RedisKeys.grace(COUPON, 1, 0)),
                memberId, "86400", "30", "-1", NOW);
    }

    /** 큐 전체를 순서대로 한 번에 읽는다. 사람마다 왕복하면 10만 회가 안 끝난다. */
    private List<String> queueOrder() {
        return List.copyOf(Objects.requireNonNull(redis.opsForZSet().range(QUEUE, 0, -1)));
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
                redis.opsForZSet().remove(QUEUE, front);
                lastRank.remove(front);
            } else if (!expected.isEmpty()) {
                // 이탈 — 중간에서 빠진다
                String gone = expected.remove(rnd.nextInt(expected.size()));
                redis.opsForZSet().remove(QUEUE, gone);
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

package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 * 리더는 한 대다 (G3.2).
 *
 * <p>둘이 동시에 배분하면 <b>총합이 전역 크레딧을 넘는다.</b> 그리고 확인과
 * 삭제가 갈리면 그 사이 리스가 만료돼 <b>남의 락을 지운다.</b>
 */
@Tag("integration")
@SpringBootTest
class LeaderElectionTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(10);
    /**
     * 폴링 한 번의 상한. {@link #WAIT} 보다 짧아야 한다.
     *
     * <p>폴링 안에서 {@code WAIT} 를 쓰면 조건 한 번이 바깥 제한보다 오래 걸려
     * <b>상한이 상한 노릇을 못 한다.</b> 그러면 늦어졌다는 사실이 시험 실패가
     * 아니라 그냥 느린 시험으로 보인다.
     */
    private static final Duration POLL = Duration.ofSeconds(1);
    private static final String LEADER = RedisKeys.LEADER;
    private static final String LEASE = "2000";

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> acquire;
    private RedisScript<Long> release;

    @BeforeEach
    void 준비() {
        acquire = RedisScript.of(new ClassPathResource("redis/leader_acquire.lua"), List.class);
        release = RedisScript.of(new ClassPathResource("redis/leader_release.lua"), Long.class);
        redis.delete(LEADER).block(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> tryAcquire(String owner) {
        return tryAcquire(owner, LEASE);
    }

    @SuppressWarnings("unchecked")
    private List<Object> tryAcquire(String owner, String lease) {
        return (List<Object>) redis.execute(acquire, List.of(LEADER), List.of(owner, lease))
                .blockFirst(WAIT);
    }

    private long releaseBy(String owner) {
        return redis.execute(release, List.of(LEADER), List.of(owner)).blockFirst(WAIT);
    }

    private boolean acquired(List<Object> r) {
        return Long.parseLong(String.valueOf(r.get(0))) == 1;
    }

    private String owner(List<Object> r) {
        return String.valueOf(r.get(1));
    }

    @Test
    @DisplayName("아무도_안_잡았으면_획득한다")
    void 아무도_안_잡았으면_획득한다() {
        List<Object> result = tryAcquire("node-1");

        assertThat(acquired(result)).isTrue();
        assertThat(owner(result)).isEqualTo("node-1");
        assertThat(redis.opsForValue().get(LEADER).block(WAIT)).isEqualTo("node-1");
    }

    @Test
    @DisplayName("자기가_잡은_락은_연장된다")
    void 자기가_잡은_락은_연장된다() {
        tryAcquire("node-1");
        redis.expire(LEADER, Duration.ofMillis(300)).block(WAIT);

        assertThat(acquired(tryAcquire("node-1"))).isTrue();
        assertThat(redis.getExpire(LEADER).block(WAIT))
                .isGreaterThan(Duration.ofMillis(1000));
    }

    @Test
    @DisplayName("남이_잡고_있으면_획득하지_못한다")
    void 남이_잡고_있으면_획득하지_못한다() {
        tryAcquire("node-1");

        List<Object> result = tryAcquire("node-2");

        assertThat(acquired(result)).isFalse();
        assertThat(owner(result)).isEqualTo("node-1");
        assertThat(redis.opsForValue().get(LEADER).block(WAIT)).isEqualTo("node-1");
    }

    @Test
    @DisplayName("10노드가_동시에_시도하면_정확히_1대만_성공한다")
    void 노드_열이_동시에_시도하면_정확히_한_대만_성공한다() throws InterruptedException {
        // 둘이 동시에 배분하면 총합이 전역 크레딧을 넘는다.
        //
        // **리스를 길게 잡는다.** 운영값(2초)으로 재면 열 스레드의 경합이 그보다
        // 오래 걸릴 때 락이 만료돼 다음 노드도 이긴다 — 그건 경합이 아니라
        // 만료를 잰 것이고, 부하에 따라 결과가 갈린다.
        String 넉넉한_리스 = String.valueOf(Duration.ofMinutes(10).toMillis());
        int nodes = 10;
        AtomicInteger winners = new AtomicInteger();
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(nodes);

        try (ExecutorService pool = Executors.newFixedThreadPool(nodes)) {
            for (int i = 0; i < nodes; i++) {
                String owner = "node-" + i;
                pool.execute(() -> {
                    try {
                        start.await();
                        if (acquired(tryAcquire(owner, 넉넉한_리스))) {
                            winners.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failures.add(e);
                    } catch (RuntimeException e) {
                        failures.add(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(failures).isEmpty();
        assertThat(winners.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("자기_락만_지울_수_있다")
    void 자기_락만_지울_수_있다() {
        tryAcquire("node-1");

        assertThat(releaseBy("node-1")).isOne();
        assertThat(redis.hasKey(LEADER).block(WAIT)).isFalse();
    }

    @Test
    @DisplayName("남의_락은_지워지지_않는다")
    void 남의_락은_지워지지_않는다() {
        // 리스가 만료돼 다른 노드가 잡은 뒤 늦게 도착한 해제 요청이다.
        // 지워지면 새 리더의 락이 사라져 배분이 멎는다.
        tryAcquire("node-2");

        assertThat(releaseBy("node-1")).isZero();
        assertThat(redis.opsForValue().get(LEADER).block(WAIT)).isEqualTo("node-2");
    }

    @Test
    @DisplayName("리스가_만료되면_다른_노드가_잡는다")
    void 리스가_만료되면_다른_노드가_잡는다() {
        // 리더가 죽으면 이만큼 뒤 승계된다. 안 풀리면 배분이 영영 멎는다.
        redis.opsForValue().set(LEADER, "dead-node", Duration.ofMillis(200)).block(WAIT);

        리스_만료를_기다린다();

        assertThat(acquired(tryAcquire("node-2"))).isTrue();
    }

    /**
     * 리스가 풀릴 때까지 기다린다.
     *
     * <p>고정 대기를 쓰지 않는다 — 짧으면 흔들리고 길면 시험이 느려진다.
     * 만료를 판정하는 것은 Redis 시계라 폴링으로 확인한다.
     */
    private void 리스_만료를_기다린다() {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Boolean.FALSE.equals(redis.hasKey(LEADER).block(POLL)));
    }

    @Test
    @DisplayName("잘못된_인자는_락을_건드리지_않는다")
    void 잘못된_인자는_락을_건드리지_않는다() {
        tryAcquire("node-1");

        assertThatThrownBy(() ->
                redis.execute(acquire, List.of(LEADER), List.of("node-2", "0")).blockFirst(WAIT))
                .rootCause()
                .hasMessageContaining("리스");

        // 빈 ownerId 도 막는다. 빈 값으로 잡히면 해제 때 누구의 락인지
        // 가릴 수 없어 남의 락을 지운다.
        assertThatThrownBy(() ->
                redis.execute(acquire, List.of(LEADER), List.of("", LEASE)).blockFirst(WAIT))
                .rootCause()
                .hasMessageContaining("ownerId");

        assertThat(redis.opsForValue().get(LEADER).block(WAIT)).isEqualTo("node-1");
    }
}

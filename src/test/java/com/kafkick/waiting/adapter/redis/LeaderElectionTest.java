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
    /**
     * 스크립트에 넘기는 리스. <b>초 단위로 둔다</b> — 여기서 파생시키는 관측 창이
     * 서브초가 되면 레디스 왕복 한 번이 창보다 길어져, 결함이 아니라 부하에 진다.
     */
    private static final String LEASE = "8000";

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

    /**
     * 락에 적힌 소유자. <b>형식을 아는 곳을 한 군데로 모은다</b> — 값은
     * {@code <펜스 번호>|<ownerId>} 이고, 옛 형식에는 번호가 없다.
     */
    private String storedOwner() {
        String raw = redis.opsForValue().get(LEADER).block(WAIT);
        if (raw == null) {
            return null;
        }
        int sep = raw.indexOf('|');
        return sep < 0 ? raw : raw.substring(sep + 1);
    }

    private long fence(List<Object> r) {
        return Long.parseLong(String.valueOf(r.get(3)));
    }

    /**
     * <b>펜스 번호는 리더가 바뀔 때마다 커진다</b> (CY-766).
     *
     * <p>되돌릴 수 없는 쓰기는 이 번호를 들고 나가고, 줄 옆의 울타리가 그것으로
     * 옛 리더를 가려낸다. 안 커지면 옛 명령과 새 명령을 구분할 방법이 없다.
     */
    @Test
    @DisplayName("리더가_바뀌면_펜스_번호가_커진다")
    void 리더가_바뀌면_펜스_번호가_커진다() {
        long 첫_펜스 = fence(tryAcquire("node-1"));
        releaseBy("node-1");

        long 다음_펜스 = fence(tryAcquire("node-2"));

        assertThat(첫_펜스).as("잡았으면 번호가 있다").isPositive();
        assertThat(다음_펜스).as("다음 리더가 더 크다").isGreaterThan(첫_펜스);
    }

    /** 연장은 같은 임기다. 매 틱 새로 매기면 자기 자신을 옛 리더로 만든다. */
    @Test
    @DisplayName("연장은_펜스_번호를_안_바꾼다")
    void 연장은_펜스_번호를_안_바꾼다() {
        long 처음 = fence(tryAcquire("node-1"));

        assertThat(fence(tryAcquire("node-1"))).isEqualTo(처음);
    }

    /** 못 잡았으면 번호가 없다. 남의 번호를 들고 나가면 그 리더를 흉내 낸다. */
    @Test
    @DisplayName("못_잡으면_펜스_번호가_없다")
    void 못_잡으면_펜스_번호가_없다() {
        tryAcquire("node-1");

        assertThat(fence(tryAcquire("node-2"))).isZero();
    }

    /**
     * <b>옛 형식의 값도 알아보고, 번호를 그 자리에서 매긴다.</b>
     *
     * <p>롤아웃 구간에 옛 노드가 남긴 락은 펜스 번호가 없다. 못 읽으면 그 락을
     * 남의 것으로 보고 리더가 둘이 된다. 그렇다고 0 을 그대로 쓰면 울타리가
     * 전부 거절해 그 노드의 매진 큐 정리가 무기한 죽는다 — 연장은 번호를 안
     * 바꾸므로 리스가 끊길 때까지 스스로 못 빠져나온다.
     */
    @Test
    @DisplayName("펜스_번호_없는_옛_락은_번호를_받는다")
    void 펜스_번호_없는_옛_락은_번호를_받는다() {
        redis.opsForValue().set(LEADER, "node-1").block(WAIT);

        List<Object> r = tryAcquire("node-1");

        assertThat(acquired(r)).as("내 락으로 알아본다").isTrue();
        assertThat(owner(r)).isEqualTo("node-1");
        assertThat(fence(r)).as("0 으로 두면 정리가 무기한 죽는다").isPositive();
        assertThat(storedOwner()).as("주인은 그대로다").isEqualTo("node-1");
    }

    /** 옛 형식의 자기 락도 해제한다. 못 알아보면 안 지우고 나간다. */
    @Test
    @DisplayName("펜스_번호_없는_옛_락도_해제한다")
    void 펜스_번호_없는_옛_락도_해제한다() {
        redis.opsForValue().set(LEADER, "node-1").block(WAIT);

        assertThat(releaseBy("node-1")).isEqualTo(1);
        assertThat(redis.hasKey(LEADER).block(WAIT)).isFalse();
    }

    @Test
    @DisplayName("아무도_안_잡았으면_획득한다")
    void 아무도_안_잡았으면_획득한다() {
        List<Object> result = tryAcquire("node-1");

        assertThat(acquired(result)).isTrue();
        assertThat(owner(result)).isEqualTo("node-1");
        assertThat(storedOwner()).isEqualTo("node-1");
    }

    @Test
    @DisplayName("자기가_잡은_락은_연장된다")
    void 자기가_잡은_락은_연장된다() {
        // **리스보다 짧게 줄여 둔다.** 연장하면 리스만큼으로 되돌아가고, 안 하면
        // 줄여 둔 값이 그대로 남는다 — 그 차이가 이 시험이 재는 전부다.
        Duration 줄여_둔_리스 = 리스().dividedBy(4);
        tryAcquire("node-1");
        redis.expire(LEADER, 줄여_둔_리스).block(WAIT);

        // **연장 경로를 탔는지부터 본다.** 그 사이 리스가 끝나면 두 번째 획득이
        // 신규 분기를 타는데, 그쪽도 성공을 돌려주고 리스도 새로 걸어 준다 —
        // 연장이 통째로 사라져도 시험은 조용히 초록이다.
        assertThat(storedOwner())
                .as("연장하려면 아직 내 락이어야 한다").isEqualTo("node-1");

        assertThat(acquired(tryAcquire("node-1"))).isTrue();

        // 줄여 둔 값보다 커야 연장을 잰 것이다. 1초 같은 손으로 적은 하한은
        // 연장을 안 해도 넘으므로 아무것도 안 잰다.
        assertThat(redis.getExpire(LEADER).block(WAIT))
                .isGreaterThan(줄여_둔_리스)
                .isLessThanOrEqualTo(리스());
    }

    @Test
    @DisplayName("남이_잡고_있으면_획득하지_못한다")
    void 남이_잡고_있으면_획득하지_못한다() {
        tryAcquire("node-1");

        List<Object> result = tryAcquire("node-2");

        assertThat(acquired(result)).isFalse();
        assertThat(owner(result)).isEqualTo("node-1");
        assertThat(storedOwner()).isEqualTo("node-1");
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
        assertThat(storedOwner()).isEqualTo("node-2");
    }

    @Test
    @DisplayName("리스가_만료되면_다른_노드가_잡는다")
    void 리스가_만료되면_다른_노드가_잡는다() {
        // 리더가 죽으면 이만큼 뒤 승계된다. 안 풀리면 배분이 영영 멎는다.
        // 200밀리초로 두면 아래 확인 전에 끝나 버려, 걸었는데도 시험이 죽는다.
        Duration 짧은_리스 = Duration.ofSeconds(1);
        redis.opsForValue().set(LEADER, "dead-node", 짧은_리스).block(WAIT);
        // 애초에 안 걸렸으면 "만료돼서 잡았다" 가 아니라 "원래 없었다" 를 재게 된다.
        assertThat(storedOwner()).isEqualTo("dead-node");

        리스_만료를_기다린다(짧은_리스);

        assertThat(acquired(tryAcquire("node-2"))).isTrue();
    }

    /**
     * 리스가 풀릴 때까지 기다린다.
     *
     * <p>고정 대기를 쓰지 않는다 — 짧으면 흔들리고 길면 시험이 느려진다.
     * 만료를 판정하는 것은 Redis 시계라 폴링으로 확인한다.
     */
    /** 스크립트에 넘기는 리스. 시험이 그 값에서 파생시키므로 한 곳에서 읽는다. */
    private static Duration 리스() {
        return Duration.ofMillis(Long.parseLong(LEASE));
    }

    private void 리스_만료를_기다린다(Duration 걸어_둔_리스) {
        await().atMost(걸어_둔_리스.multipliedBy(5))
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

        assertThat(storedOwner()).isEqualTo("node-1");
    }
}

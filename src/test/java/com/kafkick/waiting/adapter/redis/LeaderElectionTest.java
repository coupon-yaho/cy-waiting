package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    /**
     * <b>운영 키를 안 쓴다.</b> 스케줄러를 켜는 시험 컨텍스트가 같은 JVM 에 캐시된 채
     * 남아 계속 {@link RedisKeys#LEADER} 를 잡으러 온다. 그 배경 획득이 하나만 끼어도
     * 임기 단언이 흔들린다. 포트가 운영 키를 넘기는 것은
     * {@link LeaderRedisPortTest} 가 따로 못 박는다.
     */
    private static final String LEADER = "test:scheduler:leader";
    private static final String GEN = "{test:scheduler:leader}:gen";
    /** 스크립트가 시계 바닥에 더하는 배포 창(마이크로초). 스크립트에서 읽어 못 박는다. */
    private static final long ROLLOUT_MARGIN_US = 86_400_000_000L;
    /**
     * 스크립트에 넘기는 리스. <b>초 단위로 둔다</b> — 여기서 파생시키는 관측 창이
     * 서브초가 되면 레디스 왕복 한 번이 창보다 길어져, 결함이 아니라 부하에 진다.
     */
    private static final String LEASE = "8000";

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private LeaderRedisPort port;

    private RedisScript<List> acquire;
    private RedisScript<Long> release;

    @BeforeEach
    void 준비() {
        acquire = RedisScript.of(new ClassPathResource("redis/leader_acquire.lua"), List.class);
        release = RedisScript.of(new ClassPathResource("redis/leader_release.lua"), Long.class);
        redis.delete(LEADER, GEN).block(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> tryAcquire(String owner) {
        return tryAcquire(owner, LEASE);
    }

    @SuppressWarnings("unchecked")
    private List<Object> tryAcquire(String owner, String lease) {
        return (List<Object>) redis.execute(acquire, List.of(LEADER, GEN),
                List.of(owner, lease)).blockFirst(WAIT);
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

    /** 레디스 서버의 마이크로초. 옛 형식의 펜스 번호가 이 크기였다. */
    private long serverMicros() {
        Long millis = redis.execute(connection -> connection.serverCommands().time())
                .blockFirst(WAIT);
        return millis * 1000L;
    }

    /**
     * <b>시계가 뒤로 가도 임기는 오른다</b> (CY-893).
     *
     * <p>시계를 되돌릴 수는 없지만 그 상태는 "세는 값이 지금 시계보다 앞서 있다" 와
     * 같다. 세는 값이 이기지 않으면 승계한 노드의 번호가 옛 리더보다 작아진다.
     */
    @Test
    @DisplayName("세는_값이_시계보다_앞서면_그것이_이긴다")
    void 세는_값이_시계보다_앞서면_그것이_이긴다() {
        long 앞선_값 = serverMicros() + Duration.ofDays(10).toMillis() * 1_000;
        redis.opsForValue().set(GEN, Long.toString(앞선_값)).block(WAIT);

        assertThat(fence(tryAcquire("node-1")))
                .as("시계로 바닥을 다시 깔면 임기가 되감긴다")
                .isEqualTo(앞선_값 + 1);
    }

    /**
     * <b>승계는 번호를 다시 안 준다</b> (CY-893).
     *
     * <p>세는 값이 되감기는 경로는 락 유실과 같이 온다 — 복제본이 {@code INCR} 을
     * 못 받았다면 락도 못 받았다. 그때 바닥이 없으면 방금 나간 번호가 그대로 다시
     * 나가고, 다른 슬롯에 남은 울타리 표가 그 번호를 같은 임기로 보고 통과시킨다.
     */
    @Test
    @DisplayName("세는_값이_되감겨도_옛_번호를_다시_안_준다")
    void 세는_값이_되감겨도_옛_번호를_다시_안_준다() {
        long 첫_임기 = fence(tryAcquire("node-1"));
        // 복제본 승격 흉내 — 락과 세는 값만 되감고 울타리 표는 그대로 둔다.
        redis.delete(LEADER).block(WAIT);
        redis.opsForValue().set(GEN, Long.toString(첫_임기 - 1)).block(WAIT);

        assertThat(fence(tryAcquire("node-2")))
                .as("같은 번호가 두 번 나가면 울타리가 유령을 못 가른다")
                .isGreaterThan(첫_임기);
    }

    /**
     * <b>씨앗은 시계에 롤아웃 창을 더한 값이다</b> (CY-893).
     *
     * <p>1 부터 세면 남아 있는 옛 울타리 표를 못 넘고, 시계 그대로면 배포 중에 아직
     * 안 바뀐 노드가 더 큰 번호를 매겨 새 리더가 표 수명 내내 거절된다.
     */
    @Test
    @DisplayName("씨앗은_롤아웃_창만큼_앞선다")
    void 씨앗은_롤아웃_창만큼_앞선다() {
        long 시작 = serverMicros();

        assertThat(fence(tryAcquire("node-1")))
                .as("배포 중 옛 노드가 매기는 시계 임기보다 커야 한다")
                .isGreaterThan(시작 + ROLLOUT_MARGIN_US);
    }

    /**
     * <b>세는 값이 성하지 않으면 새로 씨앗을 준다</b> (CY-893).
     *
     * <p>{@code INCR} 이 스크립트째 터지면 그 클러스터에서 리더가 영영 안 뽑힌다.
     * 운영자의 오타 하나가 배분을 통째로 멈추는 자리다.
     */
    @Test
    @DisplayName("세는_값이_정수가_아니면_다시_씨앗을_준다")
    void 세는_값이_정수가_아니면_다시_씨앗을_준다() {
        redis.opsForValue().set(GEN, "그냥 글자").block(WAIT);
        long 시작 = serverMicros();

        List<Object> r = tryAcquire("node-1");

        assertThat(acquired(r)).as("여기서 못 잡으면 배분이 통째로 멎는다").isTrue();
        assertThat(fence(r)).isGreaterThan(시작 + ROLLOUT_MARGIN_US);
    }

    /**
     * <b>레디스가 거절하는 모양을 {@code tonumber} 는 받는다.</b> 지수 표기와 int64
     * 밖의 값이 검증을 통과한 뒤 {@code INCR} 에서 터진다 — 검증만으로는 못 걷는다.
     */
    @Test
    @DisplayName("세는_값이_레디스가_못_읽는_모양이면_다시_씨앗을_준다")
    void 세는_값이_레디스가_못_읽는_모양이면_다시_씨앗을_준다() {
        for (String 못_읽는_값 : List.of("1.8e15", "99999999999999999999", "1e400")) {
            redis.delete(LEADER).block(WAIT);
            redis.opsForValue().set(GEN, 못_읽는_값).block(WAIT);
            long 시작 = serverMicros();

            List<Object> r = tryAcquire("node-1");

            assertThat(acquired(r)).as("%s 에서 리더가 안 뽑힌다", 못_읽는_값).isTrue();
            assertThat(fence(r)).as("%s", 못_읽는_값).isGreaterThan(시작 + ROLLOUT_MARGIN_US);
        }
    }

    /**
     * <b>상한은 덮지 않고 멈춘다.</b> 레디스가 읽는데 못 오르는 값은 상한뿐이고, 그
     * 번호는 직전 임기로 이미 나갔다. 바닥으로 낮춰 덮으면 그 번호를 든 유령이
     * 울타리를 통과한다 — 리더를 안 뽑는 쪽이 안전한 방향이다.
     */
    @Test
    @DisplayName("세는_값이_상한이면_낮춰_덮지_않는다")
    void 세는_값이_상한이면_낮춰_덮지_않는다() {
        redis.opsForValue().set(GEN, Long.toString(Long.MAX_VALUE)).block(WAIT);

        assertThatThrownBy(() -> tryAcquire("node-1"))
                .rootCause()
                .hasMessageContaining("상한");
        assertThat(redis.opsForValue().get(GEN).block(WAIT))
                .as("낮춰 덮으면 그 번호를 든 유령이 통과한다")
                .isEqualTo(Long.toString(Long.MAX_VALUE));
    }

    /** 타입이 어긋나도 같다. {@code GET} 부터 터져 스크립트가 못 돈다. */
    @Test
    @DisplayName("세는_값의_타입이_어긋나도_다시_씨앗을_준다")
    void 세는_값의_타입이_어긋나도_다시_씨앗을_준다() {
        redis.opsForList().leftPush(GEN, "목록이다").block(WAIT);
        long 시작 = serverMicros();

        List<Object> r = tryAcquire("node-1");

        assertThat(acquired(r)).isTrue();
        assertThat(fence(r)).isGreaterThan(시작 + ROLLOUT_MARGIN_US);
    }

    /**
     * <b>세는 값에 수명을 안 준다</b> (CY-893).
     *
     * <p>사라지면 다음 임기가 씨앗부터 다시 시작한다. 그 사이 시계가 뒤로 가 있으면
     * 새 임기가 옛 임기보다 작아져, 세대 번호로 바꾼 이유가 사라진다.
     */
    @Test
    @DisplayName("세는_값은_락이_풀려도_남는다")
    void 세는_값은_락이_풀려도_남는다() {
        tryAcquire("node-1");
        releaseBy("node-1");

        assertThat(redis.hasKey(GEN).block(WAIT)).as("지워지면 씨앗부터 다시 센다").isTrue();
        assertThat(redis.getExpire(GEN).block(WAIT))
                .as("수명이 붙으면 만료 뒤 임기가 되감긴다")
                .isEqualTo(Duration.ZERO);
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
        long 시작 = serverMicros();

        List<Object> r = tryAcquire("node-1");

        assertThat(acquired(r)).as("내 락으로 알아본다").isTrue();
        assertThat(owner(r)).isEqualTo("node-1");
        assertThat(fence(r)).as("0 으로 두면 정리가 무기한 죽는다").isPositive();
        assertThat(fence(r))
                .as("여기가 롤아웃 구간이다 — 옛 노드가 남긴 마이크로초 표를 넘어야 한다")
                .isGreaterThan(시작 + ROLLOUT_MARGIN_US);
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
                redis.execute(acquire, List.of(LEADER, GEN), List.of("node-2", "0"))
                        .blockFirst(WAIT))
                .rootCause()
                .hasMessageContaining("리스");

        // 빈 ownerId 도 막는다. 빈 값으로 잡히면 해제 때 누구의 락인지
        // 가릴 수 없어 남의 락을 지운다.
        assertThatThrownBy(() ->
                redis.execute(acquire, List.of(LEADER, GEN), List.of("", LEASE))
                        .blockFirst(WAIT))
                .rootCause()
                .hasMessageContaining("ownerId");

        assertThat(storedOwner()).isEqualTo("node-1");
    }

    /**
     * <b>키 하나로 부르면 그 자리에서 거절한다.</b> 안 막으면 세는 값을 nil 로 만져
     * 스크립트가 통째로 터지고, 그 오류는 갱신 경로가 삼켜 전 노드가 리더 없이 돈다.
     */
    @Test
    @DisplayName("키가_하나면_거절한다")
    void 키가_하나면_거절한다() {
        assertThatThrownBy(() ->
                redis.execute(acquire, List.of(LEADER), List.of("node-1", LEASE))
                        .blockFirst(WAIT))
                .rootCause()
                .hasMessageContaining("KEYS");
    }

    /**
     * <b>포트가 키를 둘 다 넘긴다.</b> 이 클래스는 KEYS 를 스스로 조립하므로, 포트가
     * 하나만 넘기도록 되돌아가도 나머지 시험은 전부 초록이다. 그때 운영에서는 리더가
     * 영영 안 뽑힌다 — 조용한 배선이라 여기서 한 번 태운다.
     */
    @Test
    @DisplayName("포트는_세는_값까지_넘긴다")
    void 포트는_세는_값까지_넘긴다() {
        assertThatCode(() -> port.acquire("wiring-probe").block(WAIT))
                .as("키를 하나만 넘기면 스크립트가 거절한다")
                .doesNotThrowAnyException();
    }

    /**
     * <b>배포 창을 스크립트에서 읽어 못 박는다.</b> 시험이 제 상수를 들면 스크립트
     * 쪽만 줄여도 단언이 {@code isGreaterThan} 이라 조용히 통과한다.
     */
    @Test
    @DisplayName("배포_창은_스크립트에_적힌_값이다")
    void 배포_창은_스크립트에_적힌_값이다() throws IOException {
        String body = new ClassPathResource("redis/leader_acquire.lua").getContentAsString(
                StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("ROLLOUT_MARGIN = (\\d+)").matcher(body);

        assertThat(m.find()).as("상수 이름이 바뀌면 이 시험부터 고친다").isTrue();
        assertThat(Long.parseLong(m.group(1))).isEqualTo(ROLLOUT_MARGIN_US);
    }
}

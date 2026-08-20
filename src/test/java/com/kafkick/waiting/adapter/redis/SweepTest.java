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
 * 이탈자 청소.
 *
 * <p><b>앞부분만 훑는다.</b> 2만 명 큐에서 전체를 보면 청소 자체가 부하다.
 * 뒤엣사람은 아직 폴링할 차례가 안 왔을 뿐 죽은 것이 아니다.
 */
@Tag("integration")
@SpringBootTest
class SweepTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String COUPON = "sweep";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String GRACE = RedisKeys.grace(COUPON, 1, 0);
    private static final String ALIVE = RedisKeys.alive(COUPON, 1, 0);

    /** 시각을 주입한다 — 실제 시계에 기대면 만료 시험이 흔들린다 (TS-4). */
    private static final long NOW = 1_800_000_000L;
    private static final String RETENTION = "300";
    private static final String BUDGET = "1000";

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> enqueueScript;
    private RedisScript<List> sweepScript;

    @BeforeEach
    void 준비() {
        enqueueScript = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        sweepScript = RedisScript.of(new ClassPathResource("redis/sweep.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE, GRACE, ALIVE).block(WAIT);
    }

    private void enqueue(String memberId) {
        redis.execute(enqueueScript,
                        List.of(QUEUE, MAX_SCORE, ALIVE),
                        List.of(memberId, "86400", "3600", "0", String.valueOf(NOW)))
                .blockFirst(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> sweep(String limit, String budget, String cursor) {
        return (List<Object>) redis.execute(
                        sweepScript,
                        List.of(QUEUE, GRACE, ALIVE),
                        List.of(limit, String.valueOf(NOW), RETENTION, budget, cursor))
                .blockFirst(WAIT);
    }

    private List<Object> sweep(String limit) {
        return sweep(limit, BUDGET, "0");
    }

    private String nextCursor(List<Object> r) {
        return String.valueOf(r.get(3));
    }

    private long swept(List<Object> r) {
        return Long.parseLong(String.valueOf(r.get(0)));
    }

    /** 유예 기록 정리 수. 반환값 두 번째는 생존 신호 정리 수다. */
    private long expired(List<Object> r) {
        return Long.parseLong(String.valueOf(r.get(2)));
    }

    @Test
    @DisplayName("생존_키가_없는_앞부분_항목이_제거된다")
    void 생존_키가_없는_앞부분_항목이_제거된다() {
        enqueue("m0");
        enqueue("m1");
        enqueue("m2");
        double keptScore = redis.opsForZSet().score(QUEUE, "m2").block(WAIT);
        redis.opsForZSet().remove(ALIVE, "m0", "m1").block(WAIT);

        assertThat(swept(sweep("10"))).isEqualTo(2);

        // 크기만 보면 엉뚱한 사람을 지운 구현도 통과한다. 누가 빠지고
        // 누가 남았는지를 직접 본다.
        assertThat(redis.opsForZSet().score(QUEUE, "m0").block(WAIT)).isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "m1").block(WAIT)).isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "m2").block(WAIT)).isEqualTo(keptScore);
    }

    @Test
    @DisplayName("생존_키가_있으면_건드리지_않는다")
    void 생존_키가_있으면_건드리지_않는다() {
        enqueue("m0");
        enqueue("m1");

        assertThat(swept(sweep("10"))).isZero();
        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(2);
    }

    @Test
    @DisplayName("검사_범위_밖은_보지_않는다")
    void 검사_범위_밖은_보지_않는다() {
        for (int i = 0; i < 5; i++) {
            enqueue("m" + i);
            redis.opsForZSet().remove(ALIVE, "m" + i).block(WAIT);
        }

        double kept2 = redis.opsForZSet().score(QUEUE, "m2").block(WAIT);
        double kept4 = redis.opsForZSet().score(QUEUE, "m4").block(WAIT);

        assertThat(swept(sweep("2"))).isEqualTo(2);

        // 앞 둘만 빠지고 범위 밖은 **순번까지 그대로** 남는다
        assertThat(redis.opsForZSet().score(QUEUE, "m0").block(WAIT)).isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "m1").block(WAIT)).isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "m2").block(WAIT)).isEqualTo(kept2);
        assertThat(redis.opsForZSet().score(QUEUE, "m4").block(WAIT)).isEqualTo(kept4);
    }

    @Test
    @DisplayName("검사_범위가_인자로_주어진다")
    void 검사_범위가_인자로_주어진다() {
        // 부하와 정확도의 맞바꿈이라 배포 없이 조절할 수 있어야 한다 (P-1).
        for (int i = 0; i < 5; i++) {
            enqueue("m" + i);
            redis.opsForZSet().remove(ALIVE, "m" + i).block(WAIT);
        }

        assertThat(swept(sweep("1"))).isOne();
        assertThat(swept(sweep("4"))).isEqualTo(4);
    }

    @Test
    @DisplayName("제거된_사람이_유예_기록에_남는다")
    void 제거된_사람이_유예_기록에_남는다() {
        // 제거와 기록이 갈리면 자리도 잃고 재방문자로도 식별 안 되는
        // 사람이 생긴다. 같은 스크립트 안에서 한다.
        enqueue("m0");
        redis.opsForZSet().remove(ALIVE, "m0").block(WAIT);

        sweep("10");

        assertThat(redis.opsForHash().get(GRACE, "m0").block(WAIT))
                .isEqualTo(String.valueOf(NOW));
    }

    @Test
    @DisplayName("만료된_유예_기록이_정리된다")
    void 만료된_유예_기록이_정리된다() {
        redis.opsForHash().put(GRACE, "old", String.valueOf(NOW - 400)).block(WAIT);
        redis.opsForHash().put(GRACE, "fresh", String.valueOf(NOW - 100)).block(WAIT);

        assertThat(expired(sweep("10"))).isOne();
        assertThat(redis.opsForHash().hasKey(GRACE, "old").block(WAIT)).isFalse();
        assertThat(redis.opsForHash().hasKey(GRACE, "fresh").block(WAIT)).isTrue();
    }

    @Test
    @DisplayName("유예_기록이_무한히_쌓이지_않는다")
    void 유예_기록이_무한히_쌓이지_않는다() {
        // 만료가 없으면 이 해시가 영원히 자란다 (RD-7).
        for (int i = 0; i < 50; i++) {
            redis.opsForHash().put(GRACE, "old" + i, String.valueOf(NOW - 1000)).block(WAIT);
        }

        sweep("10");

        assertThat(redis.opsForHash().size(GRACE).block(WAIT)).isZero();
    }

    @Test
    @DisplayName("값이_깨진_유예_기록도_정리된다")
    void 값이_깨진_유예_기록도_정리된다() {
        // 숫자가 아니면 언제 것인지 알 수 없다. 남겨 두면 영원히 안 지워진다.
        redis.opsForHash().put(GRACE, "broken", "언제인지모름").block(WAIT);

        assertThat(expired(sweep("10"))).isOne();
        assertThat(redis.opsForHash().hasKey(GRACE, "broken").block(WAIT)).isFalse();
    }

    @Test
    @DisplayName("잘못된_인자는_아무것도_바꾸지_않는다")
    void 잘못된_인자는_아무것도_바꾸지_않는다() {
        enqueue("m0");
        double before = redis.opsForZSet().score(QUEUE, "m0").block(WAIT);
        redis.opsForZSet().remove(ALIVE, "m0").block(WAIT);

        assertThatThrownBy(() -> sweep("0")).rootCause().hasMessageContaining("검사 범위");

        assertThat(redis.opsForZSet().score(QUEUE, "m0").block(WAIT)).isEqualTo(before);
        assertThat(redis.opsForHash().size(GRACE).block(WAIT)).isZero();
    }

    @Test
    @DisplayName("정리_예산이_한_번의_실행을_묶는다")
    void 정리_예산이_한_번의_실행을_묶는다() {
        // COUNT 는 힌트지 상한이 아니다. 받은 것 중 예산만큼만 지워야
        // 한 번의 실행이 유계다.
        for (int i = 0; i < 40; i++) {
            redis.opsForHash().put(GRACE, "old" + i, String.valueOf(NOW - 1000)).block(WAIT);
        }

        assertThat(expired(sweep("10", "5", "0"))).isEqualTo(5);
        assertThat(redis.opsForHash().size(GRACE).block(WAIT)).isEqualTo(35);
    }

    @Test
    @DisplayName("커서를_이어_넘기면_전부_정리된다")
    void 커서를_이어_넘기면_전부_정리된다() {
        // 한 번에 다 안 지우는 대신 다음 틱이 이어받는다. 커서가 돌지
        // 않으면 같은 앞부분만 계속 보고 뒤는 영영 안 지워진다.
        for (int i = 0; i < 40; i++) {
            redis.opsForHash().put(GRACE, "old" + i, String.valueOf(NOW - 1000)).block(WAIT);
        }

        String cursor = "0";
        for (int round = 0; round < 20; round++) {
            cursor = nextCursor(sweep("10", "5", cursor));
            if (Boolean.TRUE.equals(redis.opsForHash().size(GRACE).block(WAIT) == 0L)) {
                break;
            }
        }

        assertThat(redis.opsForHash().size(GRACE).block(WAIT)).isZero();
    }

    @Test
    @DisplayName("잘못된_커서는_아무것도_바꾸지_않는다")
    void 잘못된_커서는_아무것도_바꾸지_않는다() {
        // 커서 검증이 쓰기 뒤에 있으면 앞의 쓰기가 남는다 — Lua 는
        // 롤백하지 않는다.
        enqueue("m0");
        double before = redis.opsForZSet().score(QUEUE, "m0").block(WAIT);
        redis.opsForZSet().remove(ALIVE, "m0").block(WAIT);
        redis.opsForHash().put(GRACE, "keep", String.valueOf(NOW)).block(WAIT);

        assertThatThrownBy(() -> sweep("10", "5", "abc")).rootCause()
                .hasMessageContaining("커서");

        assertThat(redis.opsForZSet().score(QUEUE, "m0").block(WAIT)).isEqualTo(before);
        assertThat(redis.opsForHash().hasKey(GRACE, "keep").block(WAIT)).isTrue();
    }

    @Test
    @DisplayName("만료된_생존_신호도_예산_안에서_걷는다")
    void 만료된_생존_신호도_예산_안에서_걷는다() {
        // 한 번에 다 지우려 하면 그 자체가 오래 걸린다.
        for (int i = 0; i < 20; i++) {
            redis.opsForZSet().add(ALIVE, "gone" + i, NOW - 100).block(WAIT);
        }

        List<Object> result = sweep("1", "5", "0");

        assertThat(Long.parseLong(String.valueOf(result.get(1)))).isEqualTo(5);
        assertThat(redis.opsForZSet().size(ALIVE).block(WAIT)).isEqualTo(15);
    }
}

package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

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
 * 시계가 뒤로 가도 줄 선 사람을 추월시키지 않는다 (F2 · 불변식 4).
 *
 * <p>순번이 카운터가 아니라 <b>벽시계</b>라(A-9) NTP 보정이나 복제본 승격으로
 * 뒤로 갈 수 있다. 실제로 시계를 돌리지 않고 시험한다 — 바닥값을 미래로 두는
 * 것이 <b>시계가 그만큼 뒤처진 것과 같다.</b>
 */
@Tag("integration")
@SpringBootTest
class ClockMonotonicTest extends RedisContainerSupport {

    private static final String NOW = "1800000000";

    private static final long TTL_SECONDS = 86_400;
    private static final Duration WAIT = Duration.ofSeconds(5);

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
        for (int i = 0; i < 200; i++) {
            redis.delete(alive("m" + i)).block(WAIT);
        }
        redis.delete(alive("m1"), alive("A"), alive("B")).block(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> enqueue(String memberId) {
        return (List<Object>) redis.execute(
                        script,
                        List.of(QUEUE, MAX_SCORE, alive(memberId)),
                        List.of(memberId, String.valueOf(TTL_SECONDS), ALIVE_TTL, NO_CAP, NOW))
                .blockFirst(WAIT);
    }

    private String alive(String memberId) {
        return RedisKeys.alive(COUPON, 1, 0);
    }

    private long scoreOf(String memberId) {
        return redis.opsForZSet().score(QUEUE, memberId).block(WAIT).longValue();
    }

    private long appliedFlag(List<Object> result) {
        return Long.parseLong(String.valueOf(result.get(1)));
    }

    private long alreadyQueued(List<Object> result) {
        return Long.parseLong(String.valueOf(result.get(2)));
    }

    @Test
    @DisplayName("정상_등록은_바닥값을_적용하지_않는다")
    void 정상_등록은_바닥값을_적용하지_않는다() {
        List<Object> result = enqueue("m1");

        // score · floorApplied · alreadyQueued · rank
        assertThat(result).hasSize(4);
        assertThat(appliedFlag(result)).isZero();
        assertThat(alreadyQueued(result)).isZero();
        assertThat(scoreOf("m1")).isPositive();
    }

    @Test
    @DisplayName("시계를_되돌려도_뒤에_온_사람이_앞서지_않는다")
    void 시계를_되돌려도_뒤에_온_사람이_앞서지_않는다() {
        enqueue("A");
        long scoreA = scoreOf("A");

        // 바닥값을 A 보다 한참 앞에 둔다 — 시계가 그만큼 뒤처진 상태와 같다.
        redis.opsForValue().set(MAX_SCORE, String.valueOf(scoreA + 10_000_000)).block(WAIT);
        enqueue("B");

        assertThat(scoreOf("B")).isGreaterThan(scoreA);
    }

    @Test
    @DisplayName("큐가_빈_동안_시계가_되돌아가도_막힌다")
    void 큐가_빈_동안_시계가_되돌아가도_막힌다() {
        enqueue("A");
        long scoreA = scoreOf("A");

        // 전원 입장 — ZSET 은 비지만 바닥값은 남는다. ZSET 의 마지막 원소를
        // 읽는 방식으로는 이 경우를 못 막는다. 그게 이 키의 존재 이유다.
        redis.delete(QUEUE).block(WAIT);
        redis.opsForValue().set(MAX_SCORE, String.valueOf(scoreA + 10_000_000)).block(WAIT);
        enqueue("B");

        assertThat(scoreOf("B")).isGreaterThan(scoreA);
    }

    @Test
    @DisplayName("바닥값이_적용되면_그_사실이_반환된다")
    void 바닥값이_적용되면_그_사실이_반환된다() {
        // 조용히 보정하지 않는다. 시계가 뒤로 간 사실을 알 수 있어야
        // "순서는 맞는데 왜 다 같은 score 인가" 를 나중에 밝힐 수 있다.
        enqueue("A");
        redis.opsForValue().set(MAX_SCORE, String.valueOf(scoreOf("A") + 60_000_000)).block(WAIT);

        assertThat(appliedFlag(enqueue("B"))).isOne();
    }

    @Test
    @DisplayName("연속_등록에서_순번이_단조_증가한다")
    void 연속_등록에서_순번이_단조_증가한다() {
        // 같은 마이크로초에 둘이 들어와도 뒤엣것이 앞서면 안 된다.
        long previous = 0;
        for (int i = 0; i < 200; i++) {
            enqueue("m" + i);
            long score = scoreOf("m" + i);
            assertThat(score).isGreaterThan(previous);
            previous = score;
        }
    }

    @Test
    @DisplayName("바닥값에_TTL이_걸린다")
    void 바닥값에_TTL이_걸린다() {
        // 오픈 중에는 등록마다 밀려나 안 사라지고, 끝난 쿠폰은 하루 뒤 사라진다.
        // 쿠폰 일정을 몰라도 성립한다.
        enqueue("m1");

        assertThat(redis.getExpire(MAX_SCORE).block(WAIT))
                .isBetween(Duration.ofSeconds(TTL_SECONDS - 10), Duration.ofSeconds(TTL_SECONDS));
    }

    @Test
    @DisplayName("이미_줄에_있으면_원래_순번을_지킨다")
    void 이미_줄에_있으면_원래_순번을_지킨다() {
        // 덮어쓰면 새로고침 연타가 자기 자신을 뒤로 민다 — 사용자는
        // 기다릴수록 손해라고 배운다.
        enqueue("m1");
        long first = scoreOf("m1");

        for (int i = 0; i < 10; i++) {
            List<Object> again = enqueue("m1");
            assertThat(alreadyQueued(again)).isOne();
            assertThat(scoreOf("m1")).isEqualTo(first);
        }
    }

    @Test
    @DisplayName("재등록은_뒤에_선_사람을_밀어내지_않는다")
    void 재등록은_뒤에_선_사람을_밀어내지_않는다() {
        enqueue("A");
        enqueue("B");
        long scoreB = scoreOf("B");

        enqueue("A");

        assertThat(scoreOf("A")).isLessThan(scoreB);
        assertThat(scoreOf("B")).isEqualTo(scoreB);
    }

    @Test
    @DisplayName("TTL이_양의_정수가_아니면_아무것도_쓰지_않는다")
    void TTL이_양의_정수가_아니면_아무것도_쓰지_않는다() {
        // Lua 는 중간 오류를 되돌리지 않는다. 쓰기 전에 막지 않으면
        // maxscore 없는 ZSET 이 남아 "같이 남거나 같이 사라진다" 가 깨진다.
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                redis.execute(script, List.of(QUEUE, MAX_SCORE, alive("m1")),
                                List.of("m1", "0", ALIVE_TTL, NO_CAP, NOW))
                        .blockFirst(WAIT))
                .rootCause()
                .hasMessageContaining("TTL");

        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isZero();
        assertThat(redis.hasKey(MAX_SCORE).block(WAIT)).isFalse();
        // 셋 중 하나만 생기는 회귀를 잡는다 — 검증이 첫 쓰기 앞에 있어야
        // 한다는 계약은 세 키 전부에 걸린다.
        assertThat(redis.hasKey(alive("m1")).block(WAIT)).isFalse();
    }
}

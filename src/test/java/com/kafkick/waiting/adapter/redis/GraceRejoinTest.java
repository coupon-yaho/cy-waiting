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
 * 자리를 비웠다 돌아온 사람 (7.5).
 *
 * <p><b>순번은 안 돌려줍니다</b> (D-11). 비운 사이에 온 사람을 뒤로 밀면
 * 그건 추월입니다 — 돌아온 것은 새로 서는 것과 같습니다. 다만 <b>돌아왔다는
 * 사실은 알려</b> 클라이언트가 "줄이 사라졌다" 와 구분할 수 있게 합니다.
 */
@Tag("integration")
@SpringBootTest
class GraceRejoinTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final String COUPON = "c1";
    private static final long NOW = 1_800_000_000L;

    private static final RedisScript<List> ENQUEUE =
            RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);

    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String ALIVE = RedisKeys.alive(COUPON, 1, 0);
    private static final String ADMITTED = RedisKeys.admitted(COUPON, 1, 0);
    private static final String GRACE = RedisKeys.grace(COUPON, 1, 0);

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @BeforeEach
    void 준비() {
        redis.delete(QUEUE, MAX_SCORE, ALIVE, ADMITTED, GRACE).block(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> 등록(String memberId) {
        return (List<Object>) redis.execute(ENQUEUE,
                        List.of(QUEUE, MAX_SCORE, ALIVE, ADMITTED, GRACE),
                        List.of(memberId, "86400", "3600", "-1", String.valueOf(NOW)))
                .next().block(WAIT);
    }

    private long 재방문(List<Object> raw) {
        return Long.parseLong(String.valueOf(raw.get(4)));
    }

    /** 처음 온 사람은 재방문이 아닙니다. 안 가르면 모두가 재방문이 됩니다. */
    @Test
    @DisplayName("처음_온_사람은_재방문이_아니다")
    void 처음_온_사람은_재방문이_아니다() {
        assertThat(재방문(등록("m1"))).isZero();
    }

    /**
     * <b>이탈 기록이 있으면 재방문입니다</b> (7.5.1).
     *
     * <p>클라이언트가 "내 줄이 사라졌다" 와 "내가 자리를 비웠다" 를 구분할 수
     * 있어야 합니다. 같은 응답이면 사용자에게 할 말이 없습니다.
     */
    @Test
    @DisplayName("이탈_기록이_있으면_재방문이다")
    void 이탈_기록이_있으면_재방문이다() {
        redis.opsForHash().put(GRACE, "m1", "d:" + (NOW - 60)).block(WAIT);

        assertThat(재방문(등록("m1"))).isOne();
    }

    /**
     * <b>이탈 기록을 소비합니다</b> (7.5.2).
     *
     * <p>안 지우면 다음에 또 재방문으로 나오고, 유예 만료를 기다려야 정상으로
     * 돌아옵니다 — 그동안 그 사람은 매번 자리를 비웠던 것이 됩니다.
     */
    @Test
    @DisplayName("재등록하면_이탈_기록이_사라진다")
    void 재등록하면_이탈_기록이_사라진다() {
        redis.opsForHash().put(GRACE, "m1", "d:" + (NOW - 60)).block(WAIT);

        등록("m1");

        assertThat(redis.opsForHash().hasKey(GRACE, "m1").block(WAIT)).isFalse();
    }

    /**
     * <b>순번은 안 돌려줍니다</b> (7.5.3 · D-11).
     *
     * <p>비운 사이에 온 사람을 뒤로 밀면 그건 추월입니다. 돌아온 사람은 그
     * 뒤에 섭니다 — 그것이 불변식 4 가 뜻하는 바입니다.
     */
    @Test
    @DisplayName("재방문자도_그_사이_온_사람_뒤에_선다")
    void 재방문자도_그_사이_온_사람_뒤에_선다() {
        redis.opsForHash().put(GRACE, "m1", "d:" + (NOW - 60)).block(WAIT);
        등록("m2");

        List<Object> 결과 = 등록("m1");

        // 앞에 한 명 있다. 순번을 돌려줬으면 0 이었을 것이다.
        assertThat(Long.parseLong(String.valueOf(결과.get(3)))).isOne();
    }

    /**
     * <b>입장 표시는 재방문이 아닙니다.</b>
     *
     * <p>같은 해시에 종류가 둘입니다. 안 가르면 차례가 왔던 사람이 재방문으로
     * 나오고, 그 표시가 지워져 <b>입장 복구가 통째로 없어집니다.</b>
     */
    @Test
    @DisplayName("입장_표시는_재방문으로_안_읽는다")
    void 입장_표시는_재방문으로_안_읽는다() {
        redis.opsForHash().put(GRACE, "m1", "a:" + (NOW - 60)).block(WAIT);

        assertThat(재방문(등록("m1"))).isZero();
        assertThat(redis.opsForHash().get(GRACE, "m1").block(WAIT))
                .as("입장 표시가 살아남는다").isEqualTo("a:" + (NOW - 60));
    }

    /** 이미 줄에 선 사람은 재방문이 아닙니다. 그 분기는 순번을 그대로 돌려줍니다. */
    @Test
    @DisplayName("이미_줄에_선_사람은_재방문이_아니다")
    void 이미_줄에_선_사람은_재방문이_아니다() {
        등록("m1");
        redis.opsForHash().put(GRACE, "m1", "d:" + (NOW - 60)).block(WAIT);

        assertThat(재방문(등록("m1"))).isZero();
    }
}

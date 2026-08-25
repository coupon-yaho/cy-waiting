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
 * 이탈 기록 해시에 <b>두 writer 가 있다</b>. 청소는 이탈 시각을, 조회는 입장
 * 표시를 같은 자리에 쓴다.
 *
 * <p>둘이 형식을 달리 쓰면 한쪽이 다른 쪽 기록을 못 알아본다. 그건 값 하나가
 * 아니라 <b>사람의 상태</b>가 조용히 뒤집히는 일이다.
 */
@Tag("integration")
@SpringBootTest
class GraceRecordTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String COUPON = "grace-record";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String GRACE = RedisKeys.grace(COUPON, 1, 0);
    private static final String ALIVE = RedisKeys.alive(COUPON, 1, 0);
    private static final String ADMITTED = RedisKeys.admitted(COUPON, 1, 0);

    private static final long NOW = 1_800_000_000L;
    private static final String RETENTION = "300";

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> enqueueScript;
    private RedisScript<List> statusScript;
    private RedisScript<List> sweepScript;

    @BeforeEach
    void 준비() {
        enqueueScript = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        statusScript = RedisScript.of(new ClassPathResource("redis/queue_status.lua"), List.class);
        sweepScript = RedisScript.of(new ClassPathResource("redis/sweep.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE, GRACE, ALIVE, ADMITTED).block(WAIT);
    }

    private void 등록한다(String memberId) {
        redis.execute(enqueueScript,
                        List.of(QUEUE, MAX_SCORE, ALIVE, ADMITTED),
                        List.of(memberId, "86400", "3600", "-1", String.valueOf(NOW)))
                .blockFirst(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> 조회한다(String memberId, long now) {
        return (List<Object>) redis.execute(statusScript,
                        List.of(QUEUE, ADMITTED, ALIVE, GRACE),
                        List.of(memberId, "30", String.valueOf(now)))
                .blockFirst(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> 청소한다(long now) {
        return (List<Object>) redis.execute(sweepScript,
                        List.of(QUEUE, GRACE, ALIVE),
                        List.of("100", String.valueOf(now), RETENTION, "1000", "0"))
                .blockFirst(WAIT);
    }

    /**
     * <b>입장 표시가 한 틱만 살면 안 된다.</b> 청소가 그것을 이탈 기록으로 읽고
     * 낡았다고 판정해 지우면, 그 뒤 폴링이 다시 종료를 받는다 — 같은 사람이
     * 두 번 물었을 때 다른 답을 준다.
     */
    @Test
    @DisplayName("입장_표시가_청소를_넘긴다")
    void 입장_표시가_청소를_넘긴다() {
        등록한다("m1");
        // 임계를 올려 차례가 오게 한다. 조회가 큐에서 빼고 입장 표시를 남긴다.
        redis.opsForValue().set(ADMITTED, "9999999999999999").block(WAIT);
        assertThat(조회한다("m1", NOW).get(0)).isEqualTo("ADMITTED");

        청소한다(NOW + 1);

        assertThat(조회한다("m1", NOW + 2).get(0)).isEqualTo("ADMITTED");
    }

    /**
     * 그렇다고 영원히 남기지도 않는다. 보관 기간이 지나면 이탈 기록과 같이 걷힌다 —
     * 안 걷으면 쿠폰당 해시가 발급 인원만큼 자란다.
     */
    @Test
    @DisplayName("입장_표시도_보관_기간이_지나면_걷힌다")
    void 입장_표시도_보관_기간이_지나면_걷힌다() {
        등록한다("m1");
        redis.opsForValue().set(ADMITTED, "9999999999999999").block(WAIT);
        조회한다("m1", NOW);

        청소한다(NOW + Long.parseLong(RETENTION) + 1);

        assertThat(조회한다("m1", NOW + Long.parseLong(RETENTION) + 2).get(0))
                .isEqualTo("NOT_QUEUED");
    }

    /**
     * 이탈 기록은 원래 하던 대로 걷힌다. 종류를 실으면서 옛 판정이 흔들리면
     * 이탈자가 영영 안 지워지고 해시만 자란다.
     */
    @Test
    @DisplayName("이탈_기록은_보관_기간이_지나면_걷힌다")
    void 이탈_기록은_보관_기간이_지나면_걷힌다() {
        등록한다("m1");
        // 생존 신호를 지우면 다음 청소가 이탈로 본다.
        redis.opsForZSet().remove(ALIVE, "m1").block(WAIT);
        청소한다(NOW + 1);
        assertThat(redis.opsForHash().hasKey(GRACE, "m1").block(WAIT)).isTrue();

        청소한다(NOW + Long.parseLong(RETENTION) + 2);

        assertThat(redis.opsForHash().hasKey(GRACE, "m1").block(WAIT)).isFalse();
    }
}

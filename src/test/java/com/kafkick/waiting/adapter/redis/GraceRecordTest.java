package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.queue.GraceRetention;
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
    /** 보관 기간. <b>손으로 안 적는다</b> — 토큰 수명과의 관계가 도메인에 있다. */
    private static final String RETENTION = String.valueOf(GraceRetention.SECONDS);

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
                        List.of(QUEUE, MAX_SCORE, ALIVE, ADMITTED, GRACE),
                        List.of(memberId, "86400", "3600", "-1", String.valueOf(NOW), "300"))
                .blockFirst(WAIT);
    }

    private List<Object> 조회한다(String memberId, long now) {
        return 조회한다(memberId, String.valueOf(now));
    }

    @SuppressWarnings("unchecked")
    private List<Object> 조회한다(String memberId, String now) {
        return (List<Object>) redis.execute(statusScript,
                        List.of(QUEUE, ADMITTED, ALIVE, GRACE),
                        List.of(memberId, "30", now))
                .blockFirst(WAIT);
    }

    private List<Object> 청소한다(long now) {
        return 청소한다(String.valueOf(now));
    }

    @SuppressWarnings("unchecked")
    private List<Object> 청소한다(String now) {
        return (List<Object>) redis.execute(sweepScript,
                        List.of(QUEUE, GRACE, ALIVE, ADMITTED),
                        List.of("100", now, RETENTION, "1000", "0"))
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
     * <b>배포 창에서 두 형식이 섞인다.</b> 게이트웨이가 여러 대라 옛 인스턴스가
     * 남긴 표시를 새 인스턴스가 읽는 구간이 반드시 생긴다. 그때 못 알아보면
     * 차례를 받은 사람이 종료를 받고, 다시 서면 그 사이 온 사람들 뒤로 간다.
     */
    @Test
    @DisplayName("옛_형식의_입장_표시도_알아본다")
    void 옛_형식의_입장_표시도_알아본다() {
        등록한다("m1");
        redis.opsForValue().set(ADMITTED, "9999999999999999").block(WAIT);
        조회한다("m1", NOW);
        // 옛 인스턴스가 남긴 모양으로 되돌린다.
        redis.opsForHash().put(GRACE, "m1", "admitted").block(WAIT);

        assertThat(조회한다("m1", NOW + 1).get(0)).isEqualTo("ADMITTED");
    }

    /**
     * 청소도 마찬가지다. 못 알아보면 보관 기간을 기다리지 않고 첫 판에 지운다.
     *
     * <p><b>지금 시각을 못 박아 둔다.</b> 시각 없는 값을 매 판 "지금" 으로 쳐 주면
     * 다시 젊어져 영영 안 걷힌다. 못 박으면 다음 판부터 늙는다.
     */
    @Test
    @DisplayName("옛_형식의_입장_표시에_시각을_박는다")
    void 옛_형식의_입장_표시에_시각을_박는다() {
        등록한다("m1");
        redis.opsForValue().set(ADMITTED, "9999999999999999").block(WAIT);
        조회한다("m1", NOW);
        redis.opsForHash().put(GRACE, "m1", "admitted").block(WAIT);

        청소한다(NOW + 1);

        assertThat(redis.opsForHash().get(GRACE, "m1").block(WAIT)).isEqualTo("a:" + (NOW + 1));
        assertThat(조회한다("m1", NOW + 2).get(0)).isEqualTo("ADMITTED");
    }

    /** 못 박았으니 늙는다. 옛 값이 영영 남으면 해시가 발급 인원만큼 자란다. */
    @Test
    @DisplayName("시각을_박은_옛_표시도_결국_걷힌다")
    void 시각을_박은_옛_표시도_결국_걷힌다() {
        // **실제 입장 경로를 지난다.** 큐에 남은 채로 입장 표시만 얹으면 그건
        // 도달 불가능한 상태다 — 시험이 그 상태에서만 참인 것을 증명하게 된다.
        등록한다("m1");
        redis.opsForValue().set(ADMITTED, "9999999999999999").block(WAIT);
        조회한다("m1", NOW);
        redis.opsForHash().put(GRACE, "m1", "admitted").block(WAIT);
        청소한다(NOW + 1);

        청소한다(NOW + 1 + Long.parseLong(RETENTION) + 1);

        assertThat(redis.opsForHash().hasKey(GRACE, "m1").block(WAIT)).isFalse();
    }

    /**
     * <b>기록된 시각이 유한하지 않으면 그 항목이 불멸이 된다.</b> nan 은 어떤
     * 비교도 참으로 안 만들고 무한은 어떤 기준보다도 크다. 형식이 깨진 값은
     * 남겨 둘 근거가 없으므로 낡음으로 본다.
     */
    @Test
    @DisplayName("깨진_시각의_기록은_걷힌다")
    void 깨진_시각의_기록은_걷힌다() {
        for (String 깨진_값 : List.of("a:nan", "d:nan", "a:1e400", "d:-1e400", "a:", "d:-1")) {
            redis.opsForHash().put(GRACE, 깨진_값, 깨진_값).block(WAIT);
        }

        청소한다(NOW + 1);

        assertThat(redis.opsForHash().keys(GRACE).collectList().block(WAIT)).isEmpty();
    }

    /**
     * <b>시각이 nan 이나 무한이면 그 항목이 불멸이 된다.</b> 비교가 늘 거짓이라
     * 보관 기간이 아무리 지나도 안 걷힌다. 값으로 굳는 자리라 첫 쓰기 앞에서 막는다.
     */
    @Test
    @DisplayName("시각이_유한하지_않으면_거부한다")
    void 시각이_유한하지_않으면_거부한다() {
        등록한다("m1");

        // 스크립트의 거절은 실행 예외로 감싸여 온다. 원인 쪽에 사유가 있다.
        assertThatThrownBy(() -> 청소한다("nan")).hasRootCauseMessage("시각은 유한해야 한다: nan");
        assertThatThrownBy(() -> 청소한다("1e400")).hasRootCauseMessage("시각은 유한해야 한다: 1e400");
        assertThatThrownBy(() -> 조회한다("m1", "nan"))
                .hasRootCauseMessage("시각은 유한해야 한다: nan");
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
        // **살아 있는 사람을 줄 안에 먼저 세운다.** 검사 창 안이 통째로
        // 조용하면 청소가 아무것도 안 한다 — 그건 전원 이탈이 아니라 저장소
        // 유실이기 때문이다. 줄 밖에 세우면 그 가드를 못 지난다.
        등록한다("keeper");
        등록한다("m1");
        // 생존 신호를 지우면 다음 청소가 이탈로 본다.
        redis.opsForZSet().remove(ALIVE, "m1").block(WAIT);
        청소한다(NOW + 1);
        assertThat(redis.opsForHash().hasKey(GRACE, "m1").block(WAIT)).isTrue();

        청소한다(NOW + Long.parseLong(RETENTION) + 2);

        assertThat(redis.opsForHash().hasKey(GRACE, "m1").block(WAIT)).isFalse();
    }
}

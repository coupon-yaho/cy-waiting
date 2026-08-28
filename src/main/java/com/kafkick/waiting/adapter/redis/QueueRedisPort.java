package com.kafkick.waiting.adapter.redis;

import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.domain.queue.QueueEntry;
import com.kafkick.waiting.domain.queue.RankEstimator;
import com.kafkick.waiting.domain.queue.QueueState;
import com.kafkick.waiting.gateway.QueuePort;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * 큐 등록과 순번 조회.
 *
 * <p><b>요청 경로가 레디스를 치는 유일한 자리다</b> (RD-4). 판정은 스냅샷이
 * 하고 여기는 판정이 끝난 뒤에만 돈다 — 통과하는 사람은 여기 안 온다.
 *
 * <p>샤드 수는 스케줄러와 <b>같은 값</b>이어야 한다. 갈리면 배분이 올린 임계와
 * 조회가 보는 줄이 다른 키가 되어, 차례가 와도 아무도 못 들어간다.
 */
@Component
public final class QueueRedisPort implements QueuePort {

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> ENQUEUE =
            RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> STATUS =
            RedisScript.of(new ClassPathResource("redis/queue_status.lua"), List.class);

    /**
     * 바닥값이 사는 시간.
     *
     * <p>큐가 빈 동안의 시계 역행을 막는 값이라 <b>큐보다 오래 살아야 한다.</b>
     * 하루로 둔다 — 선착순 한 판이 그보다 길면 그건 다른 문제다.
     */
    private static final String MAX_SCORE_TTL_SEC = "86400";

    /**
     * 생존 신호가 사는 시간.
     *
     * <p>폴링 간격에서 나오는 값이지만, 등록 시점에는 그 사람의 ETA 를 아직
     * 모른다. 가장 긴 폴링 간격(30초)의 세 배를 쓴다 — 백그라운드 탭이
     * 스로틀돼도 이탈자로 안 지워지는 값이다.
     */
    private static final String ALIVE_TTL_SEC = "90";

    private final ReactiveStringRedisTemplate redis;
    private final int shards;

    /**
     * <b>값은 설정에서 받는다.</b> 상수로 복제하면 검증기가 안 보는 값이
     * 실제로 쓰이는 값이 된다.
     */
    @Autowired
    QueueRedisPort(ReactiveStringRedisTemplate redis, ControlPlaneProperties properties) {
        this(redis, properties.scheduler().shards());
    }

    private QueueRedisPort(ReactiveStringRedisTemplate redis, int shards) {
        if (shards < 1) {
            throw new IllegalArgumentException("shards 는 1 이상이어야 한다: %d".formatted(shards));
        }
        this.redis = Objects.requireNonNull(redis, "redis 는 필수다");
        this.shards = shards;
    }

    public static QueueRedisPort of(ReactiveStringRedisTemplate redis, int shards) {
        return new QueueRedisPort(redis, shards);
    }

    /**
     * 줄에 세운다. <b>조회와 등록을 나누지 않는다</b> — 나누면 새로고침 연타에
     * 항목이 둘 생긴다.
     *
     * @param maxLen 큐 길이 상한. <b>{@link QueuePort#NO_LIMIT} 만 상한 없음이고 0 은
     *               전원 거절이다</b> — 배수할 수 없는 쿠폰은 한 명도 안 받는다
     */
    @Override
    public Mono<QueueEntry> enqueue(String couponId, String memberId, long maxLen, Instant now) {
        int shard = ShardHash.shardOf(memberId, shards);
        return redis.execute(ENQUEUE,
                        List.of(RedisKeys.queue(couponId, shards, shard),
                                RedisKeys.maxScore(couponId, shards, shard),
                                RedisKeys.alive(couponId, shards, shard),
                                RedisKeys.admitted(couponId, shards, shard),
                                RedisKeys.grace(couponId, shards, shard)),
                        List.of(memberId, MAX_SCORE_TTL_SEC, ALIVE_TTL_SEC,
                                Long.toString(maxLen), Long.toString(now.getEpochSecond())))
                .next()
                // **빈 결과를 성공으로 안 본다.** 그대로 두면 등록도 거절도 아닌
                // 채로 200 이 나가고, 실패 경로가 통째로 안 돈다.
                .switchIfEmpty(Mono.error(new IllegalStateException("등록 결과가 비었다")))
                .map(this::toEntry);
    }

    /**
     * 지금 어디쯤인가. 조회·하트비트·배수 판정이 한 번에 일어난다 — 나누면
     * 성실히 새로고침하는 사람이 이탈자로 지워진다.
     */
    @Override
    public Mono<QueueEntry> status(String couponId, String memberId, Instant now) {
        int shard = ShardHash.shardOf(memberId, shards);
        return redis.execute(STATUS,
                        List.of(RedisKeys.queue(couponId, shards, shard),
                                RedisKeys.admitted(couponId, shards, shard),
                                RedisKeys.alive(couponId, shards, shard),
                                RedisKeys.grace(couponId, shards, shard)),
                        List.of(memberId, ALIVE_TTL_SEC, Long.toString(now.getEpochSecond())))
                .next()
                .switchIfEmpty(Mono.error(new IllegalStateException("조회 결과가 비었다")))
                .map(this::toStatus);
    }

    /** 등록 결과. {@code score} 가 {@code -1} 이면 상한에 걸린 것이다. */
    private QueueEntry toEntry(List<?> raw) {
        long score = number(raw.get(0));
        if (score < 0) {
            return QueueEntry.rejected();
        }
        return new QueueEntry(QueueState.WAITING, globalRank(number(raw.get(3))), score,
                number(raw.get(2)) == 1, number(raw.get(1)) == 1, number(raw.get(4)) == 1);
    }

    private QueueEntry toStatus(List<?> raw) {
        QueueState state = QueueState.valueOf(String.valueOf(raw.get(0)));
        if (state == QueueState.NOT_QUEUED) {
            return QueueEntry.notQueued();
        }
        // 조회는 재방문을 안 말한다 — 그것은 등록 결과에만 있는 사실이다.
        return new QueueEntry(state, globalRank(number(raw.get(1))),
                number(raw.get(2)), true, false, false);
    }

    /**
     * <b>샤드 안 등수를 그대로 내보내지 않는다.</b> 샤드가 넷이면 실제보다 네 배
     * 작게 나가고, 사용자는 자기 앞이 실제보다 적다고 본다.
     *
     * <p>환산은 도메인이 한다. 샤드 수를 아는 것은 여기뿐이다.
     */
    private long globalRank(long localRank) {
        return RankEstimator.globalRank(localRank, shards);
    }

    /**
     * 루아는 정수와 문자열을 섞어 돌려준다.
     *
     * <p>순번은 16자리라 문자열로 온다 — 수로 받으면 접힌다. 그래서 둘 다 받는다.
     */
    private long number(Object value) {
        return value instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(value));
    }
}

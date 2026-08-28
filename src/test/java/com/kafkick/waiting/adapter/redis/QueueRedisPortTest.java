package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.queue.QueueEntry;
import com.kafkick.waiting.gateway.QueuePort;
import com.kafkick.waiting.domain.queue.QueueState;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 큐 등록과 순번 조회. <b>요청 경로에서 레디스를 치는 유일한 자리다</b> (RD-4) —
 * 판정은 스냅샷이 하고 여기는 판정이 끝난 뒤에만 돈다.
 */
@Tag("integration")
class QueueRedisPortTest extends RedisContainerSupport {

    private static final String COUPON = "qp";
    private static final int SHARDS = 1;
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final Instant 지금 = Instant.parse("2026-08-24T00:00:00Z");

    private static LettuceConnectionFactory factory;
    private static ReactiveStringRedisTemplate redis;
    private static QueueRedisPort port;

    @BeforeAll
    static void 연결() {
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379)),
                LettuceClientConfiguration.builder().commandTimeout(WAIT).build());
        factory.afterPropertiesSet();
        redis = new ReactiveStringRedisTemplate(factory);
        port = QueueRedisPort.of(redis, SHARDS);
    }

    @BeforeEach
    void 비운다() {
        redis.delete(RedisKeys.queue(COUPON, SHARDS, 0),
                RedisKeys.maxScore(COUPON, SHARDS, 0),
                RedisKeys.alive(COUPON, SHARDS, 0),
                RedisKeys.admitted(COUPON, SHARDS, 0),
                RedisKeys.grace(COUPON, SHARDS, 0)).block(WAIT);
    }

    /**
     * <b>어댑터가 재방문 자리를 제대로 읽습니다.</b>
     *
     * <p>스크립트를 직접 치는 시험만 있으면 <b>반환 배열의 자리를 바꿔도</b>
     * 전부 통과합니다. 이 패키지는 뮤테이션 범위 밖이라 여기가 유일한 방어이고,
     * 자리를 바꾸면 처음 온 사람이 "돌아오신 걸 환영합니다" 를 받습니다.
     */
    @Test
    @DisplayName("재방문_여부를_자리에서_제대로_읽는다")
    void 재방문_여부를_자리에서_제대로_읽는다() {
        redis.opsForHash().put(RedisKeys.grace(COUPON, SHARDS, 0), "m1",
                "d:" + 지금.getEpochSecond()).block(WAIT);
        // **앞에 둘을 세운다.** 한 명이면 rank 가 1 이라 자리를 바꿔도 같은
        // 답이 나온다 — 두 값이 실제로 달라야 자리가 못 박힌다.
        등록("m0");
        등록("m9");

        QueueEntry 돌아온_사람 = port.enqueue(COUPON, "m1", QueuePort.NO_LIMIT, 지금).block(WAIT);
        QueueEntry 처음_온_사람 = port.enqueue(COUPON, "m2", QueuePort.NO_LIMIT, 지금).block(WAIT);

        assertThat(돌아온_사람.rejoined()).as("재방문").isTrue();
        assertThat(돌아온_사람.rank()).as("앞 인원").isEqualTo(2);
        assertThat(처음_온_사람.rejoined()).as("처음 온 사람").isFalse();
    }

    /** 조회가 보는 키를 다 비운다. 하나라도 남으면 앞선 시험의 상태가 답을 바꾼다. */
    private void 비운다(int 샤드수, int 샤드) {
        redis.delete(RedisKeys.queue(COUPON, 샤드수, 샤드),
                RedisKeys.maxScore(COUPON, 샤드수, 샤드),
                RedisKeys.alive(COUPON, 샤드수, 샤드),
                RedisKeys.admitted(COUPON, 샤드수, 샤드),
                RedisKeys.grace(COUPON, 샤드수, 샤드)).block(WAIT);
    }

    private QueueEntry 등록(String memberId) {
        return port.enqueue(COUPON, memberId, QueuePort.NO_LIMIT, 지금).block(WAIT);
    }

    @Test
    @DisplayName("등록하면_순번을_받는다")
    void 등록하면_순번을_받는다() {
        QueueEntry entry = 등록("m1");

        assertThat(entry.accepted()).isTrue();
        assertThat(entry.rank()).isZero();
        assertThat(entry.score()).isPositive();
    }

    @Test
    @DisplayName("먼저_온_사람이_앞이다")
    void 먼저_온_사람이_앞이다() {
        QueueEntry 첫째 = 등록("m1");
        QueueEntry 둘째 = 등록("m2");

        assertThat(첫째.rank()).isZero();
        assertThat(둘째.rank()).isEqualTo(1);
        assertThat(둘째.score()).isGreaterThan(첫째.score());
    }

    /** 덮어쓰면 새로고침 연타가 자기 자신을 뒤로 민다 — 기다릴수록 손해가 된다. */
    @Test
    @DisplayName("다시_등록해도_순번이_안_바뀐다")
    void 다시_등록해도_순번이_안_바뀐다() {
        QueueEntry 처음 = 등록("m1");
        등록("m2");
        QueueEntry 다시 = 등록("m1");

        assertThat(다시.score()).isEqualTo(처음.score());
        assertThat(다시.rank()).isZero();
        assertThat(다시.alreadyQueued()).isTrue();
    }

    /**
     * 도메인은 상한 0 을 "배수할 수 없으니 받지 않는다" 로 읽는다. 여기서 0 을
     * 상한 없음으로 읽으면 뜻이 정반대가 되고, 배수가 멎은 쿠폰의 줄이 무한히 자란다.
     */
    @Test
    @DisplayName("상한_0_은_한_명도_안_받는다")
    void 상한_0_은_한_명도_안_받는다() {
        assertThat(port.enqueue(COUPON, "m1", 0, 지금).block(WAIT).accepted()).isFalse();
        assertThat(redis.opsForZSet().size(RedisKeys.queue(COUPON, SHARDS, 0)).block(WAIT))
                .isZero();
    }

    @Test
    @DisplayName("상한을_넘으면_거절한다")
    void 상한을_넘으면_거절한다() {
        등록("m1");
        등록("m2");

        QueueEntry 셋째 = port.enqueue(COUPON, "m3", 2, 지금).block(WAIT);

        assertThat(셋째.accepted()).isFalse();
    }

    /** 이미 선 사람을 상한으로 쫓아내면 줄이 길어진 것이 그 사람 잘못이 아닌데 자리를 잃는다. */
    @Test
    @DisplayName("이미_선_사람은_상한에_안_걸린다")
    void 이미_선_사람은_상한에_안_걸린다() {
        등록("m1");
        등록("m2");

        QueueEntry 다시 = port.enqueue(COUPON, "m1", 1, 지금).block(WAIT);

        assertThat(다시.accepted()).isTrue();
        assertThat(다시.rank()).isZero();
    }

    @Test
    @DisplayName("조회하면_기다리는_중이다")
    void 조회하면_기다리는_중이다() {
        등록("m1");
        등록("m2");

        QueueEntry 조회 = port.status(COUPON, "m2", 지금).block(WAIT);

        assertThat(조회.state()).isEqualTo(QueueState.WAITING);
        assertThat(조회.rank()).isEqualTo(1);
    }

    @Test
    @DisplayName("줄에_없으면_그렇게_말한다")
    void 줄에_없으면_그렇게_말한다() {
        QueueEntry 조회 = port.status(COUPON, "없는사람", 지금).block(WAIT);

        assertThat(조회.state()).isEqualTo(QueueState.NOT_QUEUED);
        assertThat(조회.rank()).isNegative();
    }

    @Test
    @DisplayName("차례가_오면_입장이다")
    void 차례가_오면_입장이다() {
        QueueEntry 첫째 = 등록("m1");
        등록("m2");
        // 배분이 임계를 올린다. 개수가 아니라 score 값이다 (D-8).
        redis.opsForValue().set(RedisKeys.admitted(COUPON, SHARDS, 0),
                Long.toString(첫째.score())).block(WAIT);

        assertThat(port.status(COUPON, "m1", 지금).block(WAIT).state())
                .isEqualTo(QueueState.ADMITTED);
        assertThat(port.status(COUPON, "m2", 지금).block(WAIT).state())
                .isEqualTo(QueueState.WAITING);
    }

    /**
     * <b>같은 답을 몇 번이고 준다.</b> 입장하면 큐에서 빼므로 다음 폴링은 줄에
     * 없는 것으로 보인다. 그대로 두면 자기 차례를 받은 사람이 1초 뒤에 "매진" 을
     * 보고, 다시 서면 그동안 온 사람들 뒤로 간다.
     */
    @Test
    @DisplayName("입장은_다시_물어도_입장이다")
    void 입장은_다시_물어도_입장이다() {
        QueueEntry 첫째 = 등록("m1");
        redis.opsForValue().set(RedisKeys.admitted(COUPON, SHARDS, 0),
                Long.toString(첫째.score())).block(WAIT);

        assertThat(port.status(COUPON, "m1", 지금).block(WAIT).state())
                .isEqualTo(QueueState.ADMITTED);
        // 응답을 놓친 클라이언트가 다시 묻는다. 여기서 매진이 나오면 복구 수단이 없다.
        assertThat(port.status(COUPON, "m1", 지금).block(WAIT).state())
                .isEqualTo(QueueState.ADMITTED);
        assertThat(port.status(COUPON, "m1", 지금).block(WAIT).state())
                .isEqualTo(QueueState.ADMITTED);
    }

    /** 줄에 선 적 없는 사람까지 입장으로 만들면 그게 곧 무제한 발급이다. */
    @Test
    @DisplayName("줄에_선_적_없으면_입장이_아니다")
    void 줄에_선_적_없으면_입장이_아니다() {
        등록("m1");
        redis.opsForValue().set(RedisKeys.admitted(COUPON, SHARDS, 0), "99999999999999")
                .block(WAIT);

        assertThat(port.status(COUPON, "온적없음", 지금).block(WAIT).state())
                .isEqualTo(QueueState.NOT_QUEUED);
    }

    /**
     * <b>등록과 조회가 같은 샤드를 봐야 한다.</b> 갈리면 방금 선 사람이 조회에서
     * 줄에 없는 것으로 나오고, 그는 영영 자기 순번을 못 본다.
     */
    @Test
    @DisplayName("등록한_샤드에서_조회한다")
    void 등록한_샤드에서_조회한다() {
        int 샤드수 = 8;
        QueueRedisPort 쪼갠_것 = QueueRedisPort.of(redis, 샤드수);
        String member = "m-shard";
        int 내_샤드 = ShardHash.shardOf(member, 샤드수);
        비운다(샤드수, 내_샤드);

        쪼갠_것.enqueue(COUPON, member, QueuePort.NO_LIMIT, 지금).block(WAIT);

        // 자기 샤드에만 들어갔는지 실물로 본다. 0번 샤드로 굳으면 자기 샤드가
        // 0 이 아닌 사람에게서 드러나므로, 그 사람을 골랐다는 것도 못 박는다.
        assertThat(내_샤드).isEqualTo(ShardHash.shardOf(member, 샤드수)).isPositive();
        assertThat(redis.opsForZSet()
                .score(RedisKeys.queue(COUPON, 샤드수, 내_샤드), member).block(WAIT))
                .isPositive();
        assertThat(쪼갠_것.status(COUPON, member, 지금).block(WAIT).state())
                .isEqualTo(QueueState.WAITING);
    }

    /**
     * 샤드 안 등수를 그대로 내보내면 샤드가 넷일 때 실제보다 네 배 작게 나간다.
     * 사용자는 자기 앞이 실제보다 적다고 보고, 그만큼 기다림이 길게 느껴진다.
     */
    @Test
    @DisplayName("순번은_샤드_안_등수가_아니라_전체_등수다")
    void 순번은_샤드_안_등수가_아니라_전체_등수다() {
        int 샤드수 = 8;
        QueueRedisPort 쪼갠_것 = QueueRedisPort.of(redis, 샤드수);
        String 앞사람 = "rank-a";
        String 뒷사람 = "rank-b";
        // 같은 샤드에 둘을 넣어야 등수가 1 이 된다.
        while (ShardHash.shardOf(앞사람, 샤드수) != ShardHash.shardOf(뒷사람, 샤드수)) {
            뒷사람 = 뒷사람 + "b";
        }
        int 샤드 = ShardHash.shardOf(앞사람, 샤드수);
        // 조회가 보는 키를 다 비운다. 남으면 앞선 시험의 상태가 답을 바꾼다.
        비운다(샤드수, 샤드);

        쪼갠_것.enqueue(COUPON, 앞사람, QueuePort.NO_LIMIT, 지금).block(WAIT);
        QueueEntry 뒤 = 쪼갠_것.enqueue(COUPON, 뒷사람, QueuePort.NO_LIMIT, 지금).block(WAIT);

        // 샤드 안 등수는 1 이지만 여덟으로 쪼갰으니 앞에 여덟 명이 있다고 본다.
        assertThat(뒤.rank()).isEqualTo(8);
        assertThat(쪼갠_것.status(COUPON, 뒷사람, 지금).block(WAIT).rank()).isEqualTo(8);
    }

    /**
     * 같은 사람이 동시에 여러 번 눌러도 자리는 하나다. 둘이 생기면 순번이
     * 갈리고, 뒤엣것이 앞엣것을 밀어낸다.
     */
    @Test
    @DisplayName("동시에_눌러도_자리는_하나다")
    void 동시에_눌러도_자리는_하나다() {
        List<QueueEntry> 결과 = reactor.core.publisher.Flux
                .merge(IntStream.range(0, 32)
                        .mapToObj(i -> port.enqueue(COUPON, "m1", QueuePort.NO_LIMIT, 지금))
                        .toList())
                .collectList()
                .block(WAIT);

        assertThat(결과).hasSize(32);
        assertThat(결과).extracting(QueueEntry::score).containsOnly(결과.get(0).score());
        assertThat(redis.opsForZSet().size(RedisKeys.queue(COUPON, SHARDS, 0)).block(WAIT))
                .isEqualTo(1);
    }
}

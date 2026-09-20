package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.adapter.redis.QueueRedisPort;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.queue.QueueState;
import io.lettuce.core.RedisURI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * C13d — 되살리기 전 창 (CY-944).
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C13 절이 든다. 막는 시나리오가 아니라 <b>재는</b>
 * 시나리오다 — 창의 조건과 크기를 수치로 남기는 것이 이 자리의 목적이다.
 */
@Tag("chaos")
class CursorHealWindowScenarioTest {

    private static final String COUPON = "c13d-window";

    private static final int SHARDS = 1;

    private static final int SHARD = 0;

    private static final Duration 기다림 = Duration.ofSeconds(10);

    /** 임기. 이 시나리오는 울타리를 재지 않으므로 한 번호로 고정한다. */
    private static final long 임기 = 1_770_000_000_123_456L;

    private static final Instant 지금 = Instant.ofEpochSecond(1_770_000_000L);

    /** 등록 스크립트 인자. 수명과 상한은 이 시나리오의 초점이 아니라 넉넉히 준다. */
    private static final String MAXSCORE_TTL = "3600";
    private static final String ALIVE_TTL = "250";
    private static final String 큐_상한 = "-1";
    private static final String 이탈_보관 = "600";

    /**
     * 옛 마스터의 시계가 앞선 폭(μs). 승격한 복제본이 그만큼 뒤처졌다는 뜻이다.
     *
     * <p>관측 가능한 조건은 {@code TIME < maxscore} 하나다. 실제 시각에서 유도해 회차가 길어져도 안 지나간다.
     */
    private static final long 앞선_시계 = 3600L * 1_000_000L;

    /** 창의 폭(μs). 참 커서와 살아남은 바닥값의 차이다. */
    private static final long 창_폭 = 3;

    private static RedisFaults faults;

    private static LettuceConnectionFactory factory;

    private static ReactiveStringRedisTemplate redis;

    @SuppressWarnings("rawtypes")
    private static RedisScript<List> 등록;

    private static RedisScript<String> 레디스_시각;

    private AllocationRedisPort port;

    private QueueRedisPort 큐;

    @BeforeAll
    static void 띄운다() {
        faults = RedisFaults.시작한다();
        RedisURI uri = RedisURI.create(faults.주소());
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(uri.getHost(), uri.getPort()),
                LettuceClientConfiguration.builder().commandTimeout(기다림).build());
        factory.afterPropertiesSet();
        redis = new ReactiveStringRedisTemplate(factory);
        등록 = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        레디스_시각 = RedisScript.of(
                "local t = redis.call('TIME') "
                        + "return string.format('%.0f', t[1] * 1000000 + t[2])", String.class);
    }

    @AfterAll
    static void 내린다() {
        factory.destroy();
        faults.close();
    }

    @BeforeEach
    void 비운다() {
        redis.delete(RedisKeys.queue(COUPON, SHARDS, SHARD),
                RedisKeys.maxScore(COUPON, SHARDS, SHARD),
                RedisKeys.alive(COUPON, SHARDS, SHARD),
                RedisKeys.admitted(COUPON, SHARDS, SHARD),
                RedisKeys.grace(COUPON, SHARDS, SHARD),
                RedisKeys.applyFence(COUPON, SHARDS, SHARD)).block(기다림);
        // 되살림의 기억이 포트에 있다. 공유하면 앞 회차의 기억이 다음 시험의 되살림을 만든다.
        port = AllocationRedisPort.of(redis, SHARDS);
        큐 = QueueRedisPort.of(redis, SHARDS);
    }

    /** 한 사람을 줄에 세우고 {score, floorApplied, alreadyQueued, rank, rejoined} 를 돌려준다. */
    @SuppressWarnings("unchecked")
    private List<Object> 세운다(String member) {
        return (List<Object>) redis.execute(등록,
                List.of(RedisKeys.queue(COUPON, SHARDS, SHARD),
                        RedisKeys.maxScore(COUPON, SHARDS, SHARD),
                        RedisKeys.alive(COUPON, SHARDS, SHARD),
                        RedisKeys.admitted(COUPON, SHARDS, SHARD),
                        RedisKeys.grace(COUPON, SHARDS, SHARD)),
                List.of(member, MAXSCORE_TTL, ALIVE_TTL, 큐_상한,
                        Long.toString(지금.getEpochSecond()), 이탈_보관))
                .blockLast(기다림);
    }

    private long 점수(List<Object> 등록_결과) {
        return Long.parseLong(String.valueOf(등록_결과.get(0)));
    }

    /** 바닥값이나 커서가 점수를 밀어 올렸는가. 1 이면 뒤처진 시계를 재현했다는 뜻이다. */
    private boolean 밀려_올라갔나(List<Object> 등록_결과) {
        return Long.parseLong(String.valueOf(등록_결과.get(1))) == 1;
    }

    private long 커서() {
        String raw = redis.opsForValue().get(RedisKeys.admitted(COUPON, SHARDS, SHARD)).block(기다림);
        return raw == null ? -1 : Long.parseLong(raw);
    }

    private QueueState 상태(String member) {
        return 큐.status(COUPON, member, 지금).block(기다림).state();
    }

    private long 레디스_시각() {
        return Long.parseLong(redis.execute(레디스_시각, List.of()).blockLast(기다림));
    }

    /** 앞선 시계를 쓰던 옛 마스터의 등록. 복제되지 않은 쓰기라 스크립트를 안 거친다. */
    private void 옛_마스터가_세운다(String member, long score) {
        redis.opsForZSet().add(RedisKeys.queue(COUPON, SHARDS, SHARD), member, score).block(기다림);
        redis.opsForZSet().add(RedisKeys.alive(COUPON, SHARDS, SHARD), member,
                지금.getEpochSecond() + Long.parseLong(ALIVE_TTL)).block(기다림);
        redis.opsForValue().set(RedisKeys.maxScore(COUPON, SHARDS, SHARD),
                String.valueOf(score), Duration.ofSeconds(Long.parseLong(MAXSCORE_TTL)))
                .block(기다림);
    }

    /**
     * 창이 열린 상태를 만들고 참 커서를 돌려준다.
     *
     * <p>커서를 만든 등록까지 꼬리에 들어가야 열린다. 살아남은 바닥값은 앞선 시계가 매긴 값이라 새 마스터의
     * {@code TIME} 보다 앞선다.
     *
     * @param 커서_되감김 커서가 남되 옛 값으로 돌아간 모양. 0 이면 커서 자체가 사라진 모양이다
     */
    private long 창을_연다(long 커서_되감김) {
        long 바닥 = 레디스_시각() + 앞선_시계;
        옛_마스터가_세운다("m1", 바닥 - 1);
        옛_마스터가_세운다("m2", 바닥);
        long 참_커서 = 바닥 + 창_폭;
        옛_마스터가_세운다("m3", 참_커서);
        port.apply(new Grant(COUPON, 3), 임기).block(기다림);
        assertThat(커서()).as("전제 — 커서는 앞선 시계가 매긴 점수다").isEqualTo(참_커서);

        // 접미 잘림이라 커서 쓰기와 그것을 만든 등록이 같이 빠진다. 바닥값은 살아남은 등록의 것으로 돌아간다.
        redis.opsForZSet().remove(RedisKeys.queue(COUPON, SHARDS, SHARD), "m3").block(기다림);
        redis.opsForZSet().remove(RedisKeys.alive(COUPON, SHARDS, SHARD), "m3").block(기다림);
        redis.opsForValue().set(RedisKeys.maxScore(COUPON, SHARDS, SHARD),
                String.valueOf(바닥)).block(기다림);
        if (커서_되감김 > 0) {
            assertThat(커서_되감김).as("살아남은 커서는 살아남은 등록의 것이다")
                    .isLessThanOrEqualTo(바닥);
            redis.opsForValue().set(RedisKeys.admitted(COUPON, SHARDS, SHARD),
                    String.valueOf(커서_되감김)).block(기다림);
        } else {
            redis.delete(RedisKeys.admitted(COUPON, SHARDS, SHARD)).block(기다림);
        }
        return 참_커서;
    }

    /**
     * <b>진입 — 창 안에 선 사람이 되살림 순간 크레딧 없이 들어간다.</b>
     *
     * <p>바닥값은 앞선 시계가 매긴 값이라 새 점수를 밀어 올리지만, 참 커서까지는 못 민다. 그 사이에 선 사람은
     * 되살리는 순간 아무도 들이지 않은 회차에 들어간 사람이 된다.
     */
    @Test
    @DisplayName("진입_창_안의_등록이_되살림에_크레딧_없이_들어간다")
    void 진입_창_안의_등록이_되살림에_크레딧_없이_들어간다() {
        long 참_커서 = 창을_연다(0);

        List<Object> 창_안 = 세운다("late");
        port.apply(new Grant(COUPON, 0), 임기).block(기다림);

        assertThat(밀려_올라갔나(창_안)).as("전제 — 뒤처진 시계를 재현했다").isTrue();
        assertThat(점수(창_안)).as("살아남은 바닥값 바로 위에 선다").isEqualTo(참_커서 - 창_폭 + 1);
        assertThat(커서()).as("되살림이 참 커서를 돌려놓는다").isEqualTo(참_커서);
        assertThat(상태("late")).as("크레딧 없이 입장으로 보인다").isEqualTo(QueueState.ADMITTED);
    }

    /**
     * <b>유지 — 창에 들어갈 수 있는 인원은 폭까지다.</b>
     *
     * <p>등록이 바닥값을 1 씩 밀어 올린다. 폭을 넘긴 사람은 참 커서 위로 나와, 창은 저절로 닫힌다.
     */
    @Test
    @DisplayName("유지_폭을_넘긴_사람은_커서_위로_나온다")
    void 유지_폭을_넘긴_사람은_커서_위로_나온다() {
        long 참_커서 = 창을_연다(0);

        List<Long> 창_안 = new ArrayList<>();
        for (int i = 0; i <= 창_폭; i++) {
            List<Object> 결과 = 세운다("w" + i);
            assertThat(밀려_올라갔나(결과)).as("전제 — 전원이 바닥값에 밀린다").isTrue();
            창_안.add(점수(결과));
        }
        port.apply(new Grant(COUPON, 0), 임기).block(기다림);

        assertThat(창_안.stream().filter(점수 -> 점수 <= 참_커서).count())
                .as("폭만큼만 커서 아래에 선다").isEqualTo(창_폭);
        assertThat(창_안.get((int) 창_폭)).as("폭을 넘긴 사람은 커서 위다").isGreaterThan(참_커서);
        assertThat(상태("w" + 창_폭)).as("그 사람은 여전히 기다린다").isEqualTo(QueueState.WAITING);
    }

    /**
     * <b>커서를 만든 등록이 살아남아도 바닥값이 만료되면 창이 열린다</b> (CY-959).
     *
     * <p>폴링은 생존 신호만 갱신하고 바닥값은 등록만 쓴다. 그래서 기존 대기자가 계속 폴링하는 동안
     * 신규 등록이 하루 없으면 바닥값만 만료된다 — 큐도 생존 신호도 멀쩡한 채로.
     */
    @Test
    @DisplayName("유지_바닥값이_만료되면_창이_인원으로_안_닫힌다")
    void 유지_바닥값이_만료되면_창이_인원으로_안_닫힌다() {
        long 바닥 = 레디스_시각() + 앞선_시계;
        옛_마스터가_세운다("m1", 바닥 - 1);
        옛_마스터가_세운다("m2", 바닥);
        long 참_커서 = 바닥 + 창_폭;
        // 커서를 만든 등록은 살아남는다. 사라지는 것은 커서와 바닥값뿐이다.
        옛_마스터가_세운다("m3", 참_커서);
        port.apply(new Grant(COUPON, 3), 임기).block(기다림);
        redis.delete(RedisKeys.admitted(COUPON, SHARDS, SHARD),
                RedisKeys.maxScore(COUPON, SHARDS, SHARD)).block(기다림);
        assertThat(커서()).as("전제 — 커서도 같이 유실됐다").isEqualTo(-1);

        long 등록_전 = 레디스_시각();
        List<Object> 첫째 = 세운다("late");
        long 등록_후 = 레디스_시각();
        // **다시 깔린 바닥 위에 선다.** 그래도 커서 아래라, 이 창은 인원으로 안 닫힌다.
        List<Object> 둘째 = 세운다("late2");
        port.apply(new Grant(COUPON, 0), 임기).block(기다림);

        assertThat(밀려_올라갔나(첫째)).as("밀어 올릴 바닥이 없다").isFalse();
        assertThat(점수(첫째)).as("밀린 데 없이 실시각 그대로 선다").isBetween(등록_전, 등록_후);
        assertThat(커서()).as("되살림이 참 커서를 돌려놓는다").isEqualTo(참_커서);
        assertThat(상태("late")).as("크레딧 없이 입장으로 읽힌다").isEqualTo(QueueState.ADMITTED);
        // 바닥이 남았다면 폭이 창_폭 이었다. 만료되니 앞선 시계만큼 벌어진다.
        // 아래를 1초 여유로 두는 것은 앞선 시계를 재는 자리와 등록 사이에 셋업이 끼어서다.
        assertThat(참_커서 - 점수(첫째)).as("폭이 앞선 시계만큼이다")
                .isBetween(앞선_시계 - 1_000_000, 앞선_시계 + 창_폭);
        // **둘째도 실시각에 선다.** 첫째가 바닥을 다시 깔았지만 시계가 이미 그 위라 안 밀린다.
        // 점수만 보면 밀린 경로(바닥+1)에서도 커진다. 안 밀린 것을 따로 봐야 기제가 갈린다.
        assertThat(밀려_올라갔나(둘째)).as("다시 깔린 바닥을 시계가 이미 지났다").isFalse();
        assertThat(점수(둘째)).as("뒤에 서지만 여전히 커서 아래다 — 인원으로 안 닫힌다")
                .isGreaterThan(점수(첫째)).isLessThanOrEqualTo(참_커서);
        assertThat(상태("late2")).as("둘째도 크레딧 없이 들어간다").isEqualTo(QueueState.ADMITTED);
    }

    /**
     * <b>회복 — 되살린 뒤의 등록은 커서 위에 선다.</b>
     *
     * <p>창을 닫는 것은 되살림 자체다. 커서가 돌아온 순간부터 등록이 그 위에 세우므로, 창은 유실부터 다음
     * 적용까지로 끝난다.
     */
    @Test
    @DisplayName("회복_되살린_뒤의_등록은_커서_위에_선다")
    void 회복_되살린_뒤의_등록은_커서_위에_선다() {
        long 참_커서 = 창을_연다(0);
        세운다("late");

        port.apply(new Grant(COUPON, 0), 임기).block(기다림);
        long 회복_뒤_점수 = 점수(세운다("after"));

        assertThat(커서()).as("전제 — 되살렸다").isEqualTo(참_커서);
        assertThat(회복_뒤_점수).as("커서 위 한 칸에 선다").isEqualTo(참_커서 + 1);
        assertThat(상태("after")).as("창이 닫혔다").isEqualTo(QueueState.WAITING);
    }
}

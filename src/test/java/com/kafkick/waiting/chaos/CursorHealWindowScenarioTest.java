package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.domain.allocation.Grant;
import io.lettuce.core.RedisURI;
import java.time.Duration;
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

    /** 등록이 쓰는 지금 시각(초). 생존 신호 만료에만 쓰여 점수와 무관하다. */
    private static final String 지금 = "1770000000";

    private static final String MAXSCORE_TTL = "3600";
    private static final String ALIVE_TTL = "250";
    private static final String 큐_상한 = "-1";
    private static final String 이탈_보관 = "600";

    /**
     * 옛 마스터의 시계가 앞선 폭(μs). 승격한 복제본이 그만큼 뒤처졌다는 뜻이다.
     *
     * <p>점수를 직접 얹어 만든다 — 레디스 TIME 은 시험이 못 되돌린다.
     */
    private static final long 앞선_시계 = 5_000_000L;

    /** 창 안에서 줄을 서는 인원. 전원이 참 커서 아래에 서는지가 이 시나리오의 핵심이다. */
    private static final int 창_안_인원 = 5;

    private static RedisFaults faults;

    private static LettuceConnectionFactory factory;

    private static ReactiveStringRedisTemplate redis;

    @SuppressWarnings("rawtypes")
    private static RedisScript<List> 등록;

    private AllocationRedisPort port;

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
    }

    @SuppressWarnings("unchecked")
    private List<Object> 세운다(String member) {
        return (List<Object>) redis.execute(등록,
                List.of(RedisKeys.queue(COUPON, SHARDS, SHARD),
                        RedisKeys.maxScore(COUPON, SHARDS, SHARD),
                        RedisKeys.alive(COUPON, SHARDS, SHARD),
                        RedisKeys.admitted(COUPON, SHARDS, SHARD),
                        RedisKeys.grace(COUPON, SHARDS, SHARD)),
                List.of(member, MAXSCORE_TTL, ALIVE_TTL, 큐_상한, 지금, 이탈_보관))
                .blockLast(기다림);
    }

    private long 점수(List<Object> 등록_결과) {
        return Long.parseLong(String.valueOf(등록_결과.get(0)));
    }

    private long 커서() {
        String raw = redis.opsForValue().get(RedisKeys.admitted(COUPON, SHARDS, SHARD)).block(기다림);
        return raw == null ? -1 : Long.parseLong(raw);
    }

    /** 앞선 시계가 매긴 점수로 한 사람을 얹는다. 등록 스크립트가 쓰는 두 자리를 같이 채운다. */
    private void 앞선_시계로_세운다(String member, long score) {
        redis.opsForZSet().add(RedisKeys.queue(COUPON, SHARDS, SHARD), member, score).block(기다림);
        redis.opsForValue().set(RedisKeys.maxScore(COUPON, SHARDS, SHARD),
                String.valueOf(score)).block(기다림);
    }

    /**
     * 꼬리를 자른다. 커서를 만든 등록까지 같이 빠져야 이 창이 열린다.
     *
     * @param 커서_되감김 커서가 남되 옛 값으로 돌아간 모양. 0 이면 커서 자체가 사라진 모양이다
     */
    private void 꼬리를_자른다(String 마지막, long 앞사람_점수, long 커서_되감김) {
        redis.opsForZSet().remove(RedisKeys.queue(COUPON, SHARDS, SHARD), 마지막).block(기다림);
        redis.opsForZSet().remove(RedisKeys.alive(COUPON, SHARDS, SHARD), 마지막).block(기다림);
        redis.opsForValue().set(RedisKeys.maxScore(COUPON, SHARDS, SHARD),
                String.valueOf(앞사람_점수)).block(기다림);
        if (커서_되감김 > 0) {
            redis.opsForValue().set(RedisKeys.admitted(COUPON, SHARDS, SHARD),
                    String.valueOf(커서_되감김)).block(기다림);
        } else {
            redis.delete(RedisKeys.admitted(COUPON, SHARDS, SHARD)).block(기다림);
        }
    }

    /**
     * <b>진입 — 창 안에 선 사람이 되살림 순간 크레딧 없이 들어간다.</b>
     *
     * <p>커서가 사라진 동안에는 등록이 점수를 밀어 올릴 기준이 없다. 뒤처진 시계가 매긴 점수는 참 커서 아래라,
     * 되살리는 순간 그 사람은 아무도 들이지 않은 회차에 들어간 사람이 된다.
     */
    @Test
    @DisplayName("진입_창_안의_등록이_되살림에_크레딧_없이_들어간다")
    void 진입_창_안의_등록이_되살림에_크레딧_없이_들어간다() {
        세운다("m1");
        long m2 = 점수(세운다("m2"));
        long 앞선_점수 = m2 + 앞선_시계;
        앞선_시계로_세운다("m3", 앞선_점수);
        port.apply(new Grant(COUPON, 3), 임기).block(기다림);
        long 잃기_전_커서 = 커서();

        꼬리를_자른다("m3", m2, 0);
        long 창_안_점수 = 점수(세운다("late"));
        port.apply(new Grant(COUPON, 0), 임기).block(기다림);

        assertThat(잃기_전_커서).as("전제 — 커서는 앞선 시계가 매긴 점수다").isEqualTo(앞선_점수);
        assertThat(창_안_점수).as("밀어 올릴 커서가 없어 뒤처진 시계 그대로 선다")
                .isGreaterThan(m2).isLessThan(잃기_전_커서);
        assertThat(커서()).as("되살림이 참 커서를 돌려놓는다").isEqualTo(잃기_전_커서);
        assertThat(창_안_점수).as("크레딧 없이 들어간다 — 이 창이 CY-944 다")
                .isLessThanOrEqualTo(커서());
    }

    /**
     * <b>유지 — 창이 열려 있는 동안 선 사람은 전원이 들어간다.</b>
     *
     * <p>창의 크기는 점수 영역에서 유계다. 살아남은 바닥값과 참 커서 사이에만 설 수 있고, 등록이 바닥값을 1 씩
     * 밀어 올리므로 그 폭보다 많은 사람이 들어갈 수는 없다.
     */
    @Test
    @DisplayName("유지_창_안_인원은_되살린_폭_안에_전원_들어간다")
    void 유지_창_안_인원은_되살린_폭_안에_전원_들어간다() {
        long m1 = 점수(세운다("m1"));
        long m2 = 점수(세운다("m2"));
        long 앞선_점수 = m2 + 앞선_시계;
        앞선_시계로_세운다("m3", 앞선_점수);
        port.apply(new Grant(COUPON, 3), 임기).block(기다림);

        // 되감긴 커서로 자른다. 커서가 아예 없으면 폭을 모르므로 크기를 수치로 못 남긴다.
        꼬리를_자른다("m3", m2, m1);
        List<Long> 창_안 = new ArrayList<>();
        for (int i = 0; i < 창_안_인원; i++) {
            창_안.add(점수(세운다("w" + i)));
        }
        port.apply(new Grant(COUPON, 0), 임기).block(기다림);

        long 커서 = 커서();
        assertThat(커서).as("되살린 커서는 잃기 전 값이다").isEqualTo(앞선_점수);
        assertThat(창_안).as("전원이 살아남은 바닥값과 참 커서 사이에 선다")
                .allMatch(점수 -> 점수 > m2 && 점수 <= 커서);
        assertThat(port.healedSpan()).as("되감긴 값에서 참 커서까지").isEqualTo(앞선_점수 - m1);
        assertThat(창_안.size()).as("창에 들어갈 수 있는 인원은 폭을 못 넘는다")
                .isLessThanOrEqualTo(Math.toIntExact(앞선_점수 - m2));
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
        세운다("m1");
        long m2 = 점수(세운다("m2"));
        long 앞선_점수 = m2 + 앞선_시계;
        앞선_시계로_세운다("m3", 앞선_점수);
        port.apply(new Grant(COUPON, 3), 임기).block(기다림);

        꼬리를_자른다("m3", m2, 0);
        점수(세운다("late"));
        port.apply(new Grant(COUPON, 0), 임기).block(기다림);
        long 회복_뒤_점수 = 점수(세운다("after"));

        assertThat(커서()).as("전제 — 되살렸다").isEqualTo(앞선_점수);
        assertThat(회복_뒤_점수).as("커서 위 한 칸에 선다").isEqualTo(앞선_점수 + 1);
    }
}

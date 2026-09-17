package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.adapter.redis.RedisKeys;
import com.kafkick.waiting.domain.allocation.Grant;
import io.lettuce.core.RedisURI;
import java.time.Duration;
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
 * C13c — 입장 커서의 꼬리가 유실된다 (CY-946).
 *
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C13 절이 든다. 여기는 그것을 어떻게 판정하는가만
 * 든다. 복제본 승격과 AOF 잘림은 마지막 쓰기 몇 개를 뺀다 — 커서와 그 커서를 만든 등록이 같이 빠지는 것이
 * 이 시나리오다.
 */
@Tag("chaos")
class CursorTailLossScenarioTest {

    private static final String COUPON = "c13c-tail";

    private static final int SHARDS = 1;

    private static final int SHARD = 0;

    private static final Duration 기다림 = Duration.ofSeconds(10);

    /** 임기. 이 시나리오는 울타리를 재지 않으므로 한 번호로 고정한다. */
    private static final long 임기 = 1_770_000_000_123_456L;

    /** 등록 스크립트 인자. 수명과 상한은 이 시나리오의 초점이 아니라 넉넉히 준다. */
    private static final String MAXSCORE_TTL = "3600";
    private static final String ALIVE_TTL = "250";
    private static final String 큐_상한 = "-1";
    private static final String 이탈_보관 = "600";

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
                        String.valueOf(System.currentTimeMillis() / 1000), 이탈_보관))
                .blockLast(기다림);
    }

    private long 점수(List<Object> 등록_결과) {
        return Long.parseLong(String.valueOf(등록_결과.get(0)));
    }

    private long 순번(List<Object> 등록_결과) {
        return Long.parseLong(String.valueOf(등록_결과.get(3)));
    }

    private long 커서() {
        String raw = redis.opsForValue().get(RedisKeys.admitted(COUPON, SHARDS, SHARD)).block(기다림);
        return raw == null ? -1 : Long.parseLong(raw);
    }

    /** 꼬리 유실. <b>커서와 그 커서를 만든 등록이 같이 빠진다</b> — 등록이 살아남으면 바닥값이 새 점수를 이미 민다. */
    private void 꼬리를_잃는다(String... members) {
        redis.delete(RedisKeys.admitted(COUPON, SHARDS, SHARD)).block(기다림);
        for (String member : members) {
            redis.opsForZSet().remove(RedisKeys.queue(COUPON, SHARDS, SHARD), member).block(기다림);
        }
    }

    /**
     * <b>커서를 잃어도 순번이 뛰지 않는다.</b>
     *
     * <p>순번과 총원은 커서 위에서 센다. 커서가 사라지면 줄 머리부터 다시 세어 뒤에 선 사람의 순번이 입장자
     * 수만큼 뛴다 — 사용자가 보기엔 줄이 뒤로 간 것이고, 그것이 불변식 3 이다.
     */
    @Test
    @DisplayName("진입_커서를_잃으면_순번이_뛴다")
    void 진입_커서를_잃으면_순번이_뛴다() {
        세운다("m1");
        세운다("m2");
        long 유실_전_순번 = 순번(세운다("m3"));
        port.apply(new Grant(COUPON, 2), 임기).block(기다림);
        long 들이기_뒤_순번 = 순번(세운다("m3"));

        꼬리를_잃는다("m2");
        long 유실_중_순번 = 순번(세운다("m3"));

        assertThat(들이기_뒤_순번).as("둘을 들였으니 앞의 둘이 빠진다").isZero();
        assertThat(유실_전_순번).as("들이기 전에는 앞에 둘").isEqualTo(2);
        assertThat(유실_중_순번).as("커서가 없으면 줄 머리부터 다시 센다").isGreaterThan(들이기_뒤_순번);
    }

    /**
     * <b>유지 — 되살림이 순번을 돌려놓고, 그 사이 선 사람은 커서 위에 선다.</b>
     *
     * <p>되살림이 커서를 원래대로 올리는데, 그 사이 등록한 사람의 점수가 커서 아래에 있으면 되살림 순간
     * 크레딧 없이 들어간다. 등록 스크립트가 점수를 커서 위로 미는 것이 그 방어다.
     */
    @Test
    @DisplayName("유지_되살림이_순번을_돌려놓고_사이의_등록은_커서_위에_선다")
    void 유지_되살림이_순번을_돌려놓고_사이의_등록은_커서_위에_선다() {
        세운다("m1");
        세운다("m2");
        세운다("m3");
        port.apply(new Grant(COUPON, 2), 임기).block(기다림);
        long 유실_전_커서 = 커서();
        long 유실_전_순번 = 순번(세운다("m3"));

        꼬리를_잃는다("m2");
        long 사이_점수 = 점수(세운다("m4"));
        // 크레딧이 0 이어도 되살린다. 들이는 일이 아니라 들인 기록을 돌려놓는 일이다.
        port.apply(new Grant(COUPON, 0), 임기).block(기다림);

        assertThat(커서()).as("되살린 커서는 잃기 전 값 이상이다").isGreaterThanOrEqualTo(유실_전_커서);
        assertThat(사이_점수).as("되살림 전에 선 사람도 커서 위에 선다").isGreaterThan(커서());
        assertThat(순번(세운다("m3"))).as("되살림이 순번을 돌려놓는다").isEqualTo(유실_전_순번);
        assertThat(port.healed()).as("되살림을 센다").isEqualTo(1);
    }

    /**
     * <b>회복 — 되살린 커서 위에서만 들인다.</b>
     *
     * <p>커서를 못 되살리면 줄 머리부터 다시 세어 이미 들인 사람에게 크레딧을 또 쓴다. 그만큼 실제로 들어오는
     * 사람이 줄고, 뒷단이 받는 수는 예산을 넘는다.
     */
    @Test
    @DisplayName("회복_되살린_커서_위에서만_들인다")
    void 회복_되살린_커서_위에서만_들인다() {
        세운다("m1");
        세운다("m2");
        long m3 = 점수(세운다("m3"));
        세운다("m4");
        port.apply(new Grant(COUPON, 2), 임기).block(기다림);

        꼬리를_잃는다("m2");
        long 들인_수 = port.apply(new Grant(COUPON, 1), 임기).block(기다림);

        assertThat(들인_수).as("몫만큼만 들인다").isEqualTo(1);
        assertThat(커서()).as("되살린 커서 바로 위의 한 사람까지만 올린다").isEqualTo(m3);
        assertThat(port.healed()).as("되살림을 센다").isEqualTo(1);
        // 커서가 통째로 사라진 회차는 폭을 모른다 — 임계값 자체를 폭으로 더하면 마이크로초 시각이 들어간다.
        assertThat(port.healedSpan()).as("폭을 모르는 되살림은 합에 안 넣는다").isZero();
    }
}

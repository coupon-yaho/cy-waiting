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
 * <p>무엇을 왜 재는지는 {@code plan/08-resilience.md} 의 C13 절이 든다. 유실은 마지막 등록의 세 쓰기와 그 뒤의
 * 커서 쓰기로 만든다 — 잘림이 빼는 것은 끝에서부터다. 되살리기 전 창(CY-944)은 여기서 안 닫힌다.
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

    /** 이 회차가 들이는 인원. 순번이 뛰는 폭이 이 값과 같아야 한다. */
    private static final int 들이는_인원 = 2;

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

    /**
     * 꼬리를 자른다. 마지막 등록의 세 쓰기와 그 뒤의 커서 쓰기가 같이 빠진다.
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
     * <b>진입 — 커서를 잃으면 순번이 입장자 수만큼 뛴다.</b>
     *
     * <p>순번과 총원은 커서 위에서 센다. 커서가 사라지면 줄 머리부터 다시 세어, 뒤에 선 사람에게는 줄이 뒤로
     * 간 것으로 보인다. 그것이 불변식 3 이다.
     */
    @Test
    @DisplayName("진입_커서를_잃으면_순번이_입장자_수만큼_뛴다")
    void 진입_커서를_잃으면_순번이_입장자_수만큼_뛴다() {
        세운다("m1");
        세운다("m2");
        long m3 = 점수(세운다("m3"));
        세운다("m4");
        port.apply(new Grant(COUPON, 들이는_인원), 임기).block(기다림);
        long 유실_전_순번 = 순번(세운다("m3"));

        꼬리를_자른다("m4", m3, 0);
        long 유실_중_순번 = 순번(세운다("m3"));

        assertThat(유실_전_순번).as("커서 바로 위라 앞에 아무도 없다").isZero();
        assertThat(유실_중_순번 - 유실_전_순번).as("들인 인원만큼 뛴다").isEqualTo(들이는_인원);
    }

    /**
     * <b>유지 — 되살림이 커서와 순번을 돌려놓는다.</b>
     *
     * <p>크레딧이 0 이어도 되살린다. 들이는 일이 아니라 들인 기록을 돌려놓는 일이다.
     */
    @Test
    @DisplayName("유지_되살림이_커서와_순번을_돌려놓는다")
    void 유지_되살림이_커서와_순번을_돌려놓는다() {
        세운다("m1");
        long m2 = 점수(세운다("m2"));
        long m3 = 점수(세운다("m3"));
        세운다("m4");
        port.apply(new Grant(COUPON, 들이는_인원), 임기).block(기다림);
        long 유실_전_커서 = 커서();
        // 커서 위에 한 사람을 더 세워 기대 순번이 0 이 아니게 만든다 — 0 == 0 은 아무것도 안 재는 단언이다.
        세운다("m5");
        long 유실_전_순번 = 순번(세운다("m5"));

        꼬리를_자른다("m4", m3, 0);
        long 유실_중_순번 = 순번(세운다("m5"));
        port.apply(new Grant(COUPON, 0), 임기).block(기다림);
        long 되살린_뒤_순번 = 순번(세운다("m5"));

        assertThat(유실_전_커서).as("둘째 사람까지 들였다").isEqualTo(m2);
        assertThat(유실_전_순번).as("커서 위에 m3·m4 가 있다").isEqualTo(2);
        assertThat(커서()).as("되살린 커서는 잃기 전 값이다").isEqualTo(유실_전_커서);
        assertThat(유실_중_순번 - 되살린_뒤_순번).as("커서를 잃어 뛰었던 만큼 되돌아온다")
                .isEqualTo(들이는_인원);
        // 꼬리에 m4 의 등록도 같이 빠졌다. 그 한 자리는 되살림이 돌려놓는 것이 아니다.
        assertThat(되살린_뒤_순번).as("남는 차이는 잃은 등록 한 자리뿐").isEqualTo(유실_전_순번 - 1);
        assertThat(port.healed()).as("되살림을 센다").isEqualTo(1);
    }

    /**
     * <b>유지 변종 — 커서가 남되 옛 값으로 돌아간다.</b>
     *
     * <p>복제본이 몇 회차 뒤진 채 승격하면 커서가 사라지는 대신 옛 값으로 되돌아간다. 이때만 되살린 폭을 잴 수
     * 있다 — 커서가 아예 없으면 폭을 모른다.
     */
    @Test
    @DisplayName("유지_되감긴_커서는_폭까지_센다")
    void 유지_되감긴_커서는_폭까지_센다() {
        long m1 = 점수(세운다("m1"));
        long m2 = 점수(세운다("m2"));
        long m3 = 점수(세운다("m3"));
        세운다("m4");
        port.apply(new Grant(COUPON, 들이는_인원), 임기).block(기다림);

        꼬리를_자른다("m4", m3, m1);
        port.apply(new Grant(COUPON, 0), 임기).block(기다림);

        assertThat(커서()).as("되살린 커서는 잃기 전 값이다").isEqualTo(m2);
        assertThat(port.healed()).isEqualTo(1);
        assertThat(port.healedSpan()).as("되감긴 값에서 잃기 전 값까지").isEqualTo(m2 - m1);
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
        port.apply(new Grant(COUPON, 들이는_인원), 임기).block(기다림);

        꼬리를_자른다("m4", m3, 0);
        long 들인_수 = port.apply(new Grant(COUPON, 1), 임기).block(기다림);

        assertThat(들인_수).as("몫만큼만 들인다").isEqualTo(1);
        assertThat(커서()).as("되살린 커서 바로 위의 한 사람까지만 올린다").isEqualTo(m3);
        assertThat(port.healed()).as("되살림을 센다").isEqualTo(1);
        // 커서가 통째로 사라진 회차는 폭을 모른다 — 임계값 자체를 폭으로 더하면 마이크로초 시각이 들어간다.
        assertThat(port.healedSpan()).as("폭을 모르는 되살림은 합에 안 넣는다").isZero();
    }
}

package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * 게이트웨이 하트비트와 죽은 항목 정리.
 *
 * <p><b>시각을 레디스가 찍는다.</b> 노드마다 제 벽시계를 쓰면 시계가 앞선 노드는
 * 영영 신선하고 뒤진 노드는 즉시 만료된다 — 한 시계로 재야 비교가 성립한다.
 */
@Tag("integration")
@SpringBootTest
class GatewayHeartbeatTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String INSTANCES = RedisKeys.INSTANCES;
    private static final String REAP_AFTER = "30";

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> heartbeat;
    private RedisScript<Long> leave;

    @BeforeEach
    void 준비() {
        heartbeat = RedisScript.of(new ClassPathResource("redis/gateway_heartbeat.lua"), List.class);
        leave = RedisScript.of(new ClassPathResource("redis/gateway_leave.lua"), Long.class);
        redis.delete(INSTANCES).block(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> beat(String instanceId, String reapAfter) {
        return (List<Object>) redis.execute(heartbeat, List.of(INSTANCES), List.of(instanceId, reapAfter))
                .blockFirst(WAIT);
    }

    private List<Object> beat(String instanceId) {
        return beat(instanceId, REAP_AFTER);
    }

    private long stamped(List<Object> r) {
        return Long.parseLong(String.valueOf(r.get(1)));
    }

    private long alive(List<Object> r) {
        return Long.parseLong(String.valueOf(r.get(0)));
    }

    @Test
    @DisplayName("하트비트를_남기면_자기_자신이_살아있는_것으로_센다")
    void 하트비트를_남기면_자기_자신이_살아있는_것으로_센다() {
        assertThat(alive(beat("a"))).isEqualTo(1);
    }

    @Test
    @DisplayName("노드가_늘면_즉시_분모에_들어온다")
    void 노드가_늘면_즉시_분모에_들어온다() {
        // 늦으면 기존 노드가 작은 분모로 나눠 총합이 전역 크레딧을 넘는다.
        beat("a");

        assertThat(alive(beat("b"))).isEqualTo(2);
    }

    @Test
    @DisplayName("같은_노드가_반복해도_한_번_센다")
    void 같은_노드가_반복해도_한_번_센다() {
        // 하트비트는 주기적이라 같은 노드가 계속 온다. 세면 분모가 부푼다.
        beat("a");

        assertThat(alive(beat("a"))).isEqualTo(1);
    }

    @Test
    @DisplayName("시각은_레디스가_찍는다")
    void 시각은_레디스가_찍는다() {
        // 노드가 값을 못 넣게 한다. 넣을 수 있으면 시계가 갈린 노드가
        // 영영 신선하거나 즉시 만료된다.
        List<Object> r = beat("a");
        long stamped = Long.parseLong(String.valueOf(r.get(1)));
        String stored = redis.<String, String>opsForHash().get(INSTANCES, "a").block(WAIT);

        assertThat(Long.parseLong(stored)).isEqualTo(stamped);
        assertThat(stamped).isGreaterThan(1_700_000_000L);
    }

    @Test
    @DisplayName("임계를_넘긴_항목은_지워지고_안_센다")
    void 임계를_넘긴_항목은_지워지고_안_센다() {
        // 해시가 배포 이력만큼 자라면 매 틱 그걸 다 읽는다.
        beat("old");
        redis.<String, String>opsForHash().put(INSTANCES, "old", "1").block(WAIT);

        assertThat(alive(beat("a"))).isEqualTo(1);
        assertThat(redis.<String, String>opsForHash().hasKey(INSTANCES, "old").block(WAIT)).isFalse();
    }

    @Test
    @DisplayName("값이_숫자가_아니면_죽은_것으로_본다")
    void 값이_숫자가_아니면_죽은_것으로_본다() {
        // 판이 갈리거나 손으로 건드린 값이다. 살아 있는 것으로 세면
        // 분모가 부풀어 전 노드가 몫을 덜 쓴다.
        redis.<String, String>opsForHash().put(INSTANCES, "broken", "어제쯤").block(WAIT);

        assertThat(alive(beat("a"))).isEqualTo(1);
        assertThat(redis.<String, String>opsForHash().hasKey(INSTANCES, "broken").block(WAIT)).isFalse();
    }

    @Test
    @DisplayName("자발적_종료는_임계를_안_기다린다")
    void 자발적_종료는_임계를_안_기다린다() {
        // 배포마다 임계 시간 동안 분모가 부풀면 그동안 전 노드가 몫을 덜 쓴다.
        beat("a");
        beat("b");

        Long removed = redis.execute(leave, List.of(INSTANCES), List.of("b")).blockFirst(WAIT);

        assertThat(removed).isEqualTo(1);
        assertThat(alive(beat("a"))).isEqualTo(1);
    }

    @Test
    @DisplayName("없는_노드를_빼도_남의_것을_지우지_않는다")
    void 없는_노드를_빼도_남의_것을_지우지_않는다() {
        beat("a");

        Long removed = redis.execute(leave, List.of(INSTANCES), List.of("ghost")).blockFirst(WAIT);

        assertThat(removed).isZero();
        assertThat(alive(beat("a"))).isEqualTo(1);
    }

    @Test
    @DisplayName("하트비트는_매번_갱신된다")
    void 하트비트는_매번_갱신된다() {
        // **HSETNX 로 바꿔도 통과하던 구멍이다.** 심장이 계속 뛰는지가 하트비트의
        // 전부인데, 첫 등록만 되고 갱신이 씹히면 모든 노드가 임계 뒤 죽은 것으로
        // 분류되어 분모가 무너진다.
        long first = stamped(beat("a"));
        redis.<String, String>opsForHash().put(INSTANCES, "a", String.valueOf(first - 10))
                .block(WAIT);

        beat("a");

        long stored = Long.parseLong(redis.<String, String>opsForHash().get(INSTANCES, "a").block(WAIT));
        assertThat(stored).isGreaterThanOrEqualTo(first);
    }

    @Test
    @DisplayName("임계와_같은_나이는_살고_한_칸_넘으면_죽는다")
    void 임계와_같은_나이는_살고_한_칸_넘으면_죽는다() {
        // **1970년 값을 넣으면 임계가 30이든 30억이든 결과가 같다.** 그러면
        // 이 시험은 "나이 판정이 있는가" 까지만 재고 임계 자체는 못 잰다.
        // 서버 시각을 받아 경계 양쪽을 박는다.
        // **기준 시각을 마지막에 받는다.** 먼저 받아 두면 그 뒤 왕복 사이에
        // 서버 초가 넘어가 경계에 걸친 항목의 나이가 한 칸 밀린다. 임계를 넉넉히
        // 잡아 왕복 지연을 흡수한다 — 재는 것은 경계지 지연이 아니다.
        String reapAfter = "60";
        long now = stamped(beat("a"));
        redis.<String, String>opsForHash().put(INSTANCES, "edge", String.valueOf(now - 60)).block(WAIT);
        redis.<String, String>opsForHash().put(INSTANCES, "over", String.valueOf(now - 61)).block(WAIT);

        assertThat(alive(beat("a", reapAfter))).isEqualTo(2);
        assertThat(redis.<String, String>opsForHash().hasKey(INSTANCES, "edge").block(WAIT)).isTrue();
        assertThat(redis.<String, String>opsForHash().hasKey(INSTANCES, "over").block(WAIT)).isFalse();
    }

    @Test
    @DisplayName("미래_시각은_죽은_것으로_본다")
    void 미래_시각은_죽은_것으로_본다() {
        // **복제본이 승격하면서 시계가 뒤로 가면 기존 항목이 전부 미래가 된다.**
        // 그때 now - seen 이 음수라 영영 안 지워지고, 죽은 노드가 계속 세어져
        // 분모가 부푼다. 스스로 회복되지도 않는다.
        redis.<String, String>opsForHash().put(INSTANCES, "future", "99999999999").block(WAIT);

        assertThat(alive(beat("a"))).isEqualTo(1);
        assertThat(redis.<String, String>opsForHash().hasKey(INSTANCES, "future").block(WAIT))
                .isFalse();
    }

    @Test
    @DisplayName("무한대_임계는_거절한다")
    void 무한대_임계는_거절한다() {
        // tonumber 는 1e400 을 inf 로 주고 math.floor(inf) == inf 라 정수
        // 검사를 그냥 통과한다. 그러면 아무것도 영영 안 지워진다.
        assertThatThrownBy(() -> beat("a", "1e400"))
                .hasRootCauseMessage("임계는 1..86400 의 정수여야 한다: 1e400");
    }

    @Test
    @DisplayName("상한을_넘는_임계는_거절한다")
    void 상한을_넘는_임계는_거절한다() {
        assertThatThrownBy(() -> beat("a", "86401"))
                .hasRootCauseMessage("임계는 1..86400 의 정수여야 한다: 86401");
    }

    @Test
    @DisplayName("빈_노드_이름으로_해제하면_거절한다")
    void 빈_노드_이름으로_해제하면_거절한다() {
        // 그냥 받으면 아무 일도 안 하고 0 을 돌려줘서, 부른 쪽은 지웠다고 믿는다.
        assertThatThrownBy(
                () -> redis.execute(leave, List.of(INSTANCES), List.of("")).blockFirst(WAIT))
                .hasRootCauseMessage("instanceId 는 필수다");
    }

    @Test
    @DisplayName("임계가_잘못되면_거절한다")
    void 임계가_잘못되면_거절한다() {
        // 0 이나 소수를 그냥 받으면 모든 항목이 즉시 죽거나 영영 안 죽는다.
        assertThatThrownBy(() -> beat("a", "0"))
                .hasRootCauseMessage("임계는 1..86400 의 정수여야 한다: 0");
        assertThatThrownBy(() -> beat("a", "1.5"))
                .hasRootCauseMessage("임계는 1..86400 의 정수여야 한다: 1.5");
    }
}

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
    /**
     * <b>운영 키를 안 쓴다.</b> 이 컨텍스트에는 살아 있는 하트비트 루프가 있어
     * 같은 키에 자기를 등록한다 — 그러면 이 시험이 센 수에 남이 섞여 간헐로
     * 깨진다. 스크립트는 키를 인자로 받으므로 자리를 갈라 주면 된다.
     */
    // 접두사를 그대로 두는 것은 이 키가 무엇인지 이름으로 알아보기 위해서다.
    // 슬롯은 어차피 갈린다 — 해시태그가 없는 키는 이름 전체로 슬롯이 정해지므로
    // 운영 키와 이 키는 같은 슬롯일 수 없다. 스크립트가 키를 하나만 쓰니 무방하다.
    private static final String INSTANCES = RedisKeys.INSTANCES + ":heartbeat-test";
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
    /** 표를 인정하는 신선도. 분모의 임계보다 짧다. */
    private static final String VOTE_FRESH = "5";

    private List<Object> beat(String instanceId, String reapAfter, String circuit) {
        return beat(instanceId, reapAfter, circuit, VOTE_FRESH);
    }

    private List<Object> beat(String instanceId, String reapAfter, String circuit,
            String voteFresh) {
        return (List<Object>) redis.execute(heartbeat, List.of(INSTANCES),
                        List.of(instanceId, reapAfter, circuit, voteFresh))
                .blockFirst(WAIT);
    }

    private List<Object> beat(String instanceId, String reapAfter) {
        return beat(instanceId, reapAfter, "CLOSED");
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
        assertThat(storedSeen("a")).isEqualTo(stamped);
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
        // 버전이 갈리거나 손으로 건드린 값이다. 살아 있는 것으로 세면
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

        assertThat(storedSeen("a")).isGreaterThanOrEqualTo(first);
    }

    @Test
    @DisplayName("임계와_같은_나이는_살고_한_칸_넘으면_죽는다")
    void 임계와_같은_나이는_살고_한_칸_넘으면_죽는다() {
        // **1970년 값을 넣으면 임계가 30이든 30억이든 결과가 같다.** 그러면
        // 이 시험은 "나이 판정이 있는가" 까지만 재고 임계 자체는 못 잰다.
        // 서버 시각을 받아 경계 양쪽을 박는다.
        // **왕복 사이에 서버 초가 넘어가면 경계가 한 칸 밀린다.** 그러면 나이
        // 60 으로 심은 항목이 61 이 되어 죽고, 시험이 간헐적으로 깨진다.
        // 넘어가지 않은 표본만 골라서 잰다 — 재는 것은 경계지 왕복 지연이 아니다.
        //
        // **시각을 인자로 받게 하지 않는다.** 그러면 운영에서도 노드가 제 시계를
        // 넣을 수 있고, 이 스크립트가 존재하는 이유가 바로 그걸 막는 것이다.
        // 한 회차는 명령 네 번이라 밀리초 단위고, 그 사이 초가 넘어갈 확률은 1%
        // 아래다. 스무 번이 모두 걸릴 일은 사실상 없고, 그래도 걸리면 조용히
        // 통과하는 대신 이유를 적어 실패한다.
        long reapAfter = 60;
        long alive = 0;
        boolean measured = false;
        for (int attempt = 0; attempt < 20; attempt++) {
            redis.delete(INSTANCES).block(WAIT);
            long anchor = stamped(beat("a"));
            redis.<String, String>opsForHash()
                    .put(INSTANCES, "edge", String.valueOf(anchor - reapAfter)).block(WAIT);
            redis.<String, String>opsForHash()
                    .put(INSTANCES, "over", String.valueOf(anchor - reapAfter - 1)).block(WAIT);

            List<Object> r = beat("a", String.valueOf(reapAfter));
            if (stamped(r) == anchor) {
                alive = alive(r);
                measured = true;
                break;
            }
        }
        assertThat(measured).withFailMessage("초 경계를 안 넘긴 표본이 없었다").isTrue();

        // 나이가 임계와 같으면 살고, 한 칸 넘으면 죽는다.
        assertThat(alive).isEqualTo(2);
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
    /**
     * <b>노드마다 자기 서킷을 싣고, 갈래를 나눠 센다</b> (CY-791).
     *
     * <p>안 실으면 배분이 리더 한 대의 관측으로 전 클러스터의 크레딧을 정한다.
     */
    // 열린 것과 반쯤 열린 것을 합치면 전 노드가 동시에 반쯤 열린 순간이 과반으로
    // 접혀 배분이 0 이 되고, 그러면 서킷이 시험할 호출이 없어 영영 안 닫힌다.
    @Test
    @DisplayName("표를_갈래별로_세어_돌려준다")
    void 표를_갈래별로_세어_돌려준다() {
        beat("a", "30", "CLOSED");
        beat("b", "30", "OPEN");
        List<Object> r = beat("c", "30", "HALF_OPEN");

        assertThat(alive(r)).isEqualTo(3);
        assertThat(at(r, 2)).as("열린 수").isEqualTo(1);
        assertThat(at(r, 3)).as("반쯤 열린 수").isEqualTo(1);
        assertThat(at(r, 4)).as("표를 낸 수").isEqualTo(3);
    }

    /**
     * <b>생존 값에 서킷을 붙이지 않는다.</b>
     *
     * <p>붙이면 롤아웃 중 옛 노드의 {@code tonumber} 가 nil 을 내고 새 노드를
     * 죽은 것으로 판정해 <b>지운다.</b> 옛 리더가 보는 분모가 줄어 남은 노드가
     * 각자 큰 몫을 쓰므로 총 통과가 전역 크레딧을 넘는다 — 초과 발급 방향이다.
     * 롤백은 더 나쁘다. 전 노드가 옛 파서로 돌아가 서로를 계속 지운다.
     */
    @Test
    @DisplayName("생존_값은_초만_담아_옛_노드가_읽는다")
    void 생존_값은_초만_담아_옛_노드가_읽는다() {
        beat("a", "30", "OPEN");

        String raw = redis.<String, String>opsForHash().get(INSTANCES, "a").block(WAIT);

        assertThat(raw).containsOnlyDigits();
        assertThat(Long.parseLong(raw)).isPositive();
    }

    /**
     * <b>옛 노드의 항목도 센다.</b> 롤아웃 구간에는 표를 안 싣는 노드가 섞인다.
     * 못 읽고 죽은 것으로 치면 그 노드들이 분모에서 빠져 남은 노드가 큰 몫을 쓴다.
     */
    @Test
    @DisplayName("표를_안_낸_옛_항목도_산다")
    void 표를_안_낸_옛_항목도_산다() {
        long now = stamped(beat("a", "30", "OPEN"));
        redis.<String, String>opsForHash()
                .put(INSTANCES, "old", String.valueOf(now)).block(WAIT);

        List<Object> r = beat("a", "30", "OPEN");

        assertThat(alive(r)).as("옛 노드도 센다").isEqualTo(2);
        assertThat(at(r, 2)).as("모르는 것은 닫힌 것으로 본다").isEqualTo(1);
        assertThat(at(r, 4)).as("표를 낸 것은 하나뿐이다").isEqualTo(1);
    }

    /** 죽은 항목의 표는 안 센다. 세면 이미 없는 노드가 배분을 계속 멈춘다. */
    @Test
    @DisplayName("죽은_항목의_표는_안_센다")
    void 죽은_항목의_표는_안_센다() {
        redis.<String, String>opsForHash().put(INSTANCES, "dead", "1").block(WAIT);
        redis.<String, String>opsForHash().put(INSTANCES, "#c:dead", "OPEN").block(WAIT);

        List<Object> r = beat("a", "30", "CLOSED");

        assertThat(alive(r)).isEqualTo(1);
        assertThat(at(r, 2)).isZero();
    }

    /**
     * <b>낡은 표는 산 노드의 것이어도 안 센다.</b>
     *
     * <p>표의 신선도를 분모의 임계와 같이 두면, 죽어 가는 노드의 마지막 표가
     * 그 임계(운영 기본 60초)만큼 살아 있다. 시체 하나가 멀쩡한 클러스터를
     * 그 시간 내내 조인다 — 30초 안에 회복해야 한다는 기준을 못 지킨다.
     */
    @Test
    @DisplayName("낡은_표는_안_센다")
    void 낡은_표는_안_센다() {
        long now = stamped(beat("a", "30", "CLOSED"));
        // 아직 산 노드다 — 임계 30초 안이다. 그러나 표로는 낡았다.
        redis.<String, String>opsForHash()
                .put(INSTANCES, "stale", String.valueOf(now - 10)).block(WAIT);
        redis.<String, String>opsForHash().put(INSTANCES, "#c:stale", "OPEN").block(WAIT);

        List<Object> r = beat("a", "30", "CLOSED", "5");

        assertThat(alive(r)).as("분모에는 들어간다").isEqualTo(2);
        assertThat(at(r, 2)).as("표로는 안 센다").isZero();
        assertThat(at(r, 4)).as("표를 낸 것은 자기뿐이다").isEqualTo(1);
    }

    /** 항목이 사라진 표는 지운다. 안 지우면 해시가 배포 이력만큼 자란다. */
    @Test
    @DisplayName("주인_없는_표는_지운다")
    void 주인_없는_표는_지운다() {
        redis.<String, String>opsForHash().put(INSTANCES, "#c:gone", "OPEN").block(WAIT);

        beat("a", "30", "CLOSED");

        assertThat(redis.opsForHash().hasKey(INSTANCES, "#c:gone").block(WAIT)).isFalse();
    }

    /** 모르는 상태는 거절한다. 받아 두면 표로 세어져 전 클러스터의 배분이 멎는다. */
    @Test
    @DisplayName("모르는_서킷_상태는_거절한다")
    void 모르는_서킷_상태는_거절한다() {
        assertThatThrownBy(() -> beat("a", "30", "BANANA"))
                .hasRootCauseMessage("모르는 서킷 상태다: BANANA");
    }

    /** 표 신선도도 임계와 같은 자로 검사한다. 분모의 임계보다 길 수 없다. */
    @Test
    @DisplayName("표_신선도가_임계보다_길_수_없다")
    void 표_신선도가_임계보다_길_수_없다() {
        assertThatThrownBy(() -> beat("a", "30", "CLOSED", "31"))
                .hasRootCauseMessage("표 신선도는 1..30 의 정수여야 한다: 31");
    }

    /** 저장된 값에서 시각만. <b>형식을 아는 곳을 한 군데로 모은다</b> — 초만 담는다. */
    private long storedSeen(String instanceId) {
        return Long.parseLong(
                redis.<String, String>opsForHash().get(INSTANCES, instanceId).block(WAIT));
    }

    private int at(List<Object> r, int index) {
        return Integer.parseInt(String.valueOf(r.get(index)));
    }
}

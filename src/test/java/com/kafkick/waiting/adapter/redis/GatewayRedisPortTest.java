package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.control.GatewayHeartbeatLoop;
import com.kafkick.waiting.domain.admission.CircuitState;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

/**
 * 하트비트 어댑터.
 *
 * <p>스크립트는 {@code GatewayHeartbeatTest} 가 잰다. 여기서 재는 것은 <b>자바가
 * 그 반환을 분모로 옳게 읽는가</b> 다 — 스크립트가 맞아도 파싱이 틀리면 분모가
 * 조용히 어긋나고, 그건 어느 쪽 시험도 안 잡는다.
 */
@Tag("integration")
@SpringBootTest
class GatewayRedisPortTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);

    private static final long REAP_AFTER_SEC = 30;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private GatewayRedisPort port;

    /**
     * <b>살아 있는 하트비트 루프를 멈춘다.</b>
     *
     * <p>이 컨텍스트에는 매 틱 자기를 등록하는 루프가 있고, 그 노드가 이
     * 시험이 세는 수에 섞인다 — 지운 뒤 단언 사이에 한 번만 찍혀도 깨진다.
     * 운영 키를 쓰는 시험이라 자리를 가를 수 없으니 원을 멈춘다.
     */
    @Autowired
    private GatewayHeartbeatLoop 하트비트;

    @BeforeEach
    void 준비() {
        하트비트.stop();
        port = GatewayRedisPort.of(redis);
        redis.delete(RedisKeys.INSTANCES).block(WAIT);
    }

    /** 표를 인정하는 신선도. 분모의 임계보다 짧다. */
    private static final long VOTE_FRESH_SEC = 5;

    /** 한 노드가 평시 서킷으로 찍는다. 분모만 본다. */
    private Mono<Integer> 노드(String id) {
        return 찍는다(id, CircuitState.CLOSED).map(GatewayRedisPort.Presence::alive);
    }

    private Mono<GatewayRedisPort.Presence> 찍는다(String id, CircuitState circuit) {
        return port.beat(id, REAP_AFTER_SEC, VOTE_FRESH_SEC, circuit);
    }

    @Test
    @DisplayName("자기를_세고_남도_센다")
    void 자기를_세고_남도_센다() {
        assertThat(노드("gw-a").block(WAIT)).isEqualTo(1);

        assertThat(노드("gw-b").block(WAIT)).isEqualTo(2);
        // 같은 노드가 다시 찍어도 늘지 않는다. 늘면 배포마다 분모가 부푼다.
        assertThat(노드("gw-a").block(WAIT)).isEqualTo(2);
    }

    @Test
    @DisplayName("나간_노드는_즉시_빠진다")
    void 나간_노드는_즉시_빠진다() {
        노드("gw-a").block(WAIT);
        노드("gw-b").block(WAIT);

        port.leave("gw-b").block(WAIT);

        // 임계를 안 기다린다. 기다리면 배포마다 그 시간 동안 전 노드가 몫을 덜 쓴다.
        assertThat(노드("gw-a").block(WAIT)).isEqualTo(1);
    }

    /**
     * <b>표를 갈래별로 세어 돌려준다</b> (CY-791).
     *
     * <p>분모와 표가 <b>같은 왕복</b>에서 나와야 한다. 나눠 읽으면 그 사이에
     * 노드가 드나들어 서로 다른 회차의 값이 섞인다.
     */
    // 열린 것과 반쯤 열린 것을 합치지 않는다. 합치면 전 노드가 동시에 반쯤 열린
    // 순간이 과반으로 접혀 배분이 0 이 되고, 그러면 서킷이 영영 안 닫힌다.
    @Test
    @DisplayName("표를_갈래별로_세어_돌려준다")
    void 표를_갈래별로_세어_돌려준다() {
        찍는다("gw-a", CircuitState.OPEN).block(WAIT);
        찍는다("gw-b", CircuitState.CLOSED).block(WAIT);

        GatewayRedisPort.Presence seen = 찍는다("gw-c", CircuitState.HALF_OPEN).block(WAIT);

        assertThat(seen).isEqualTo(new GatewayRedisPort.Presence(3, 1, 1, 3));
    }

    /**
     * <b>값은 옛 형식 그대로 둔다</b> — 표는 별도 field 에 실린다.
     *
     * <p>값에 붙이면 롤아웃 중 옛 노드의 {@code tonumber} 가 nil 을 내고 새
     * 노드를 죽은 것으로 판정해 지운다. 옛 리더가 보는 분모가 줄어 남은 노드가
     * 각자 큰 몫을 쓰고, 그건 초과 발급 방향이다.
     */
    @Test
    @DisplayName("생존_값은_초만_담는다")
    void 생존_값은_초만_담는다() {
        찍는다("gw-a", CircuitState.OPEN).block(WAIT);

        String stored = redis.<String, String>opsForHash()
                .get(RedisKeys.INSTANCES, "gw-a").block(WAIT);

        assertThat(stored).containsOnlyDigits();
    }

    /** 나간 노드는 표도 같이 빠진다. 안 그러면 해시가 배포 이력만큼 자란다. */
    @Test
    @DisplayName("나간_노드의_표도_같이_빠진다")
    void 나간_노드의_표도_같이_빠진다() {
        찍는다("gw-a", CircuitState.OPEN).block(WAIT);
        찍는다("gw-b", CircuitState.CLOSED).block(WAIT);

        port.leave("gw-a").block(WAIT);

        GatewayRedisPort.Presence seen = 찍는다("gw-b", CircuitState.CLOSED).block(WAIT);
        assertThat(seen).isEqualTo(new GatewayRedisPort.Presence(1, 0, 0, 1));
    }

    /**
     * <b>칸 수가 다르면 터뜨린다</b> (RD-11).
     *
     * <p>모자란 칸을 0 으로 메우면 표가 영영 0 이고 클러스터는 항상 닫힌 것으로
     * 보인다 — 기능이 조용히 꺼진 채 돌다가 다음 장애 때에야 드러난다. 롤백
     * 구간(새 코드 + 옛 스크립트)이 실제로 그 자리다.
     */
    @Test
    @DisplayName("칸_수가_다른_응답은_거절한다")
    void 칸_수가_다른_응답은_거절한다() {
        // 롤백 구간에서 옛 스크립트가 돌려주는 모양이다.
        assertThatThrownBy(() -> GatewayRedisPort.presence(List.of(3L, 1_700_000_000L, 1L)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("5 칸");
    }

    /** 칸 수가 맞으면 자리대로 읽는다. 순서를 바꾸면 여기가 빨개진다. */
    @Test
    @DisplayName("칸_수가_맞으면_자리대로_읽는다")
    void 칸_수가_맞으면_자리대로_읽는다() {
        GatewayRedisPort.Presence seen =
                GatewayRedisPort.presence(List.of(9L, 1_700_000_000L, 3L, 2L, 7L));

        assertThat(seen).isEqualTo(new GatewayRedisPort.Presence(9, 3, 2, 7));
    }
}

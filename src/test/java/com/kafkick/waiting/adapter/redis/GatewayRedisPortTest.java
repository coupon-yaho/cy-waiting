package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.GatewayHeartbeatLoop;
import com.kafkick.waiting.domain.admission.CircuitState;
import java.time.Duration;
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

    /** 한 노드가 평시 서킷으로 찍는다. 분모만 본다. */
    private Mono<Integer> 노드(String id) {
        return port.beat(id, REAP_AFTER_SEC, CircuitState.CLOSED).map(GatewayRedisPort.Presence::alive);
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
     * <b>서킷을 같이 세어 돌려준다</b> (CY-791).
     *
     * <p>배분이 리더 한 대의 로컬 관측으로 전 클러스터의 크레딧을 정하지
     * 않으려면, 분모와 열린 수가 <b>같은 왕복</b>에서 나와야 한다. 나눠 읽으면
     * 그 사이에 노드가 드나들어 서로 다른 판의 값이 섞인다.
     */
    @Test
    @DisplayName("살아있는_수와_열린_수를_같이_돌려준다")
    void 살아있는_수와_열린_수를_같이_돌려준다() {
        port.beat("gw-a", REAP_AFTER_SEC, CircuitState.OPEN).block(WAIT);
        port.beat("gw-b", REAP_AFTER_SEC, CircuitState.CLOSED).block(WAIT);

        GatewayRedisPort.Presence seen = port.beat("gw-c", REAP_AFTER_SEC, CircuitState.HALF_OPEN).block(WAIT);

        // 열린 것도 반쯤 열린 것도 닫히지 않은 것이다. 반만 세면 회복 구간에서
        // 다시 전속력으로 밀어 넣는다.
        assertThat(seen).isEqualTo(new GatewayRedisPort.Presence(3, 2));
    }
}

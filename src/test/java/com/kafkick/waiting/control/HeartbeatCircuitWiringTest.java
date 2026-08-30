package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.GatewayRedisPort.Presence;
import com.kafkick.waiting.domain.admission.CircuitState;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * 하트비트가 <b>서킷을 실어 보내고 클러스터 판정을 받아 적는지</b> 본다 (CY-791).
 *
 * <p>안 실으면 배분이 리더 한 대의 로컬 관측으로 전 클러스터의 크레딧을 정한다.
 * 리더만 멀쩡하면 나머지가 전부 열려 있어도 평소 속도로 돌고, 그 몫은 이미
 * 넘어진 뒷단으로 간다.
 */
class HeartbeatCircuitWiringTest {

    private GatewayRegistry 등록부() {
        return GatewayRegistry.of(1, 1);
    }

    /** 이 노드가 본 서킷이 스크립트 인자로 나가야 한다. 안 나가면 아무도 못 센다. */
    @Test
    @DisplayName("이_노드의_서킷을_실어_보낸다")
    void 이_노드의_서킷을_실어_보낸다() {
        List<CircuitState> 보낸_것 = new ArrayList<>();

        GatewayPresenceConfig.beatStep(circuit -> {
            보낸_것.add(circuit);
            return Mono.just(new Presence(1, 0));
        }, () -> CircuitState.HALF_OPEN, 등록부()).get().block();

        assertThat(보낸_것).containsExactly(CircuitState.HALF_OPEN);
    }

    /**
     * 스크립트가 센 것을 <b>등록부가 받아 적는다.</b>
     *
     * <p>과반이 열렸으면 클러스터는 열린 것이다 — 셋 중 둘이면 OPEN 이다.
     */
    @Test
    @DisplayName("클러스터_판정을_등록부에_적는다")
    void 클러스터_판정을_등록부에_적는다() {
        GatewayRegistry registry = 등록부();

        GatewayPresenceConfig.beatStep(circuit -> Mono.just(new Presence(3, 2)),
                () -> CircuitState.CLOSED, registry).get().block();

        assertThat(registry.circuit()).isEqualTo(CircuitState.OPEN);
    }

    /** 분모는 그대로 첫 칸이다. 서킷을 실었다고 세던 것이 바뀌면 안 된다. */
    @Test
    @DisplayName("분모는_첫_칸_그대로다")
    void 분모는_첫_칸_그대로다() {
        AtomicReference<Integer> 관측 = new AtomicReference<>();

        Integer alive = GatewayPresenceConfig.beatStep(circuit -> Mono.just(new Presence(4, 1)),
                () -> CircuitState.CLOSED, 등록부()).get().block();
        관측.set(alive);

        assertThat(관측.get()).isEqualTo(4);
    }

    /** 관측이 오기 전에는 닫힌 것으로 본다. 모른다고 배분을 멈추면 평시가 죽는다. */
    @Test
    @DisplayName("관측_전에는_닫힌_것으로_본다")
    void 관측_전에는_닫힌_것으로_본다() {
        assertThat(등록부().circuit()).isEqualTo(CircuitState.CLOSED);
    }
}

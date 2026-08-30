package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.GatewayRedisPort.Presence;
import com.kafkick.waiting.domain.admission.CircuitState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 하트비트가 <b>서킷을 실어 보내고 클러스터 판정을 받아 적는지</b> 본다 (CY-791).
 *
 * <p>안 실으면 배분이 리더 한 대의 로컬 관측으로 전 클러스터의 크레딧을 정한다.
 * 리더만 멀쩡하면 나머지가 전부 열려 있어도 평소 속도로 돌고, 그 몫은 이미
 * 넘어진 뒷단으로 간다.
 */
class HeartbeatCircuitWiringTest {

    /** 감소를 확정하기까지의 연속 관측 수. 서킷을 푸는 방향도 같은 수를 쓴다. */
    private static final int 등록부_감소_틱 = 3;

    private static final Duration 간격 = Duration.ofSeconds(1);

    private GatewayRegistry 등록부() {
        return GatewayRegistry.of(등록부_감소_틱, 1);
    }

    /** 이 노드가 본 서킷이 스크립트 인자로 나가야 한다. 안 나가면 아무도 못 센다. */
    @Test
    @DisplayName("이_노드의_서킷을_실어_보낸다")
    void 이_노드의_서킷을_실어_보낸다() {
        List<CircuitState> 보낸_것 = new ArrayList<>();

        GatewayPresenceConfig.beatStep(circuit -> {
            보낸_것.add(circuit);
            return Mono.just(new Presence(1, 0, 0, 1));
        }, () -> CircuitState.HALF_OPEN, 등록부()).get().block();

        assertThat(보낸_것).containsExactly(CircuitState.HALF_OPEN);
    }

    /**
     * 스크립트가 센 것을 <b>등록부가 받아 적는다.</b>
     *
     * <p>픽스처를 <b>비대칭으로 고른다.</b> 셋 중 둘 같은 조합은 인자를 뒤바꿔도
     * 같은 답이 나와 단언이 아무 일도 안 한다. 셋 중 하나면 뒤바꾼 순간 OPEN 이
     * 되어 죽는다 — 그 뒤바뀜은 부분 장애를 전면 정지로 읽는 실제 결함이다.
     */
    @Test
    @DisplayName("클러스터_판정을_등록부에_적는다")
    void 클러스터_판정을_등록부에_적는다() {
        GatewayRegistry registry = 등록부();

        GatewayPresenceConfig.beatStep(circuit -> Mono.just(new Presence(3, 1, 0, 3)),
                () -> CircuitState.CLOSED, registry).get().block();

        assertThat(registry.circuit()).as("소수만 열린 것은 부분 장애다")
                .isEqualTo(CircuitState.HALF_OPEN);
    }

    /** 반쯤 열린 표도 갈래를 지켜 전해져야 한다. 열린 것과 섞이면 교착이 난다. */
    @Test
    @DisplayName("반쯤_열린_표는_전면_정지가_안_된다")
    void 반쯤_열린_표는_전면_정지가_안_된다() {
        GatewayRegistry registry = 등록부();

        GatewayPresenceConfig.beatStep(circuit -> Mono.just(new Presence(3, 0, 3, 3)),
                () -> CircuitState.CLOSED, registry).get().block();

        assertThat(registry.circuit()).isEqualTo(CircuitState.HALF_OPEN);
    }

    /**
     * <b>무응답도 놓친 것이다</b> — 오류만 세면 절반만 지킨다.
     *
     * <p>레디스 장애에서 더 흔한 쪽은 오류가 아니라 무응답이다. 상한이 걸리면
     * 리액터는 상류를 취소하지 오류를 흘리지 않으므로, 놓침을 세는 자리가 상한
     * 안쪽에 있으면 그 구간을 통째로 못 본다.
     */
    @Test
    @DisplayName("무응답도_놓친_것으로_센다")
    void 무응답도_놓친_것으로_센다() {
        GatewayRegistry registry = 등록부();

        루프를_돌린다(Mono::never, registry);

        assertThat(registry.circuit()).isEqualTo(CircuitState.OPEN);
    }

    /** 오류도 마찬가지다. 둘 다 같은 자리에서 세어야 한 쪽만 지켜지지 않는다. */
    @Test
    @DisplayName("오류도_놓친_것으로_센다")
    void 오류도_놓친_것으로_센다() {
        GatewayRegistry registry = 등록부();

        루프를_돌린다(() -> Mono.error(new IllegalStateException("레디스가 죽었다")), registry);

        assertThat(registry.circuit()).isEqualTo(CircuitState.OPEN);
    }

    /**
     * 배선된 것과 같은 모양으로 루프를 돌린다. 놓침을 세는 자리가 상한 바깥인지를
     * 재는 것이므로, 루프를 빼고 재면 그 자리를 못 본다.
     */
    private void 루프를_돌린다(Supplier<Mono<Integer>> beat, GatewayRegistry registry) {
        VirtualTimeScheduler 가상 = VirtualTimeScheduler.create();
        GatewayHeartbeatLoop loop = GatewayHeartbeatLoop.of(beat, Mono::empty,
                registry::observed, () -> registry.circuitMissed(CircuitState.OPEN),
                간격, Duration.ofSeconds(1), 가상);

        loop.start(가상);
        try {
            가상.advanceTimeBy(간격.multipliedBy(등록부_감소_틱 * 2L + 1));
        } finally {
            loop.stop();
        }
    }

    /** 관측이 오기 전에는 닫힌 것으로 본다. 모른다고 배분을 멈추면 평시가 죽는다. */
    @Test
    @DisplayName("관측_전에는_닫힌_것으로_본다")
    void 관측_전에는_닫힌_것으로_본다() {
        assertThat(등록부().circuit()).isEqualTo(CircuitState.CLOSED);
    }
}

package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.GatewayRedisPort;
import com.kafkick.waiting.adapter.redis.GatewayRedisPort.Presence;
import com.kafkick.waiting.domain.admission.CircuitState;
import com.kafkick.waiting.gateway.CircuitStateReader;
import java.util.function.Function;
import java.util.function.Supplier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * 노드가 자기 존재를 알리는 배선. <b>배분 토글 밖이다</b> — 요청만 받는 노드도
 * 분모에 들어가야 한다. 토글 뒤에 두면 리더가 그 노드를 못 세고, 남은 노드가
 * 각자 큰 몫을 써서 총 통과가 전역 크레딧을 넘는다.
 */
@Configuration
public class GatewayPresenceConfig {

    /**
     * 관측한 노드 수. 하트비트가 여기에 관측을 넣는데 토글을 끈 노드에도
     * 하트비트가 돌아야 하므로, 둘이 같은 자리에 있어야 한다.
     */
    @Bean
    GatewayRegistry gatewayRegistry(ControlPlaneProperties properties) {
        return GatewayRegistry.of(properties.capacity().rampDownTicks(),
                properties.capacity().expectedNodes());
    }

    /**
     * 하트비트 루프. 주기는 틱과 같다 — 배분이 한 틱마다 분모를 읽으므로 그보다
     * 드물게 찍으면 멀쩡한 노드가 관측 사이에서 사라진다.
     */
    @Bean
    GatewayHeartbeatLoop gatewayHeartbeatLoop(GatewayRedisPort port,
            GatewayRegistry registry, ControlPlaneProperties properties,
            CircuitStateReader circuit) {
        String instanceId = Leadership.newOwnerId();
        long reapAfterSec = properties.capacity().freshness().toSeconds();
        return GatewayHeartbeatLoop.of(
                beatStep(state -> port.beat(instanceId, reapAfterSec, state), circuit::now,
                        registry),
                () -> port.leave(instanceId),
                registry::observed,
                properties.scheduler().tick(),
                properties.leader().attempt());
    }

    /**
     * 한 번의 하트비트. <b>서킷을 싣고, 클러스터 판정을 받아 적는다</b> (CY-791).
     *
     * <p>배선을 여기 따로 뺀 것은, 이 두 줄이 빠져도 하트비트가 초록으로 돌기
     * 때문이다. 그러면 배분은 리더 한 대의 로컬 서킷으로 전 클러스터의 크레딧을
     * 정한다 — 리더만 멀쩡하면 나머지가 다 열려 있어도 평소 속도로 돈다.
     */
    static Supplier<Mono<Integer>> beatStep(Function<CircuitState, Mono<Presence>> beat,
            Supplier<CircuitState> local, GatewayRegistry registry) {
        return () -> beat.apply(local.get())
                .doOnNext(seen -> registry.circuitObserved(seen.alive(), seen.notClosed()))
                .map(Presence::alive);
    }
}

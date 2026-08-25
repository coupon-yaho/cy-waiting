package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.GatewayRedisPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 노드가 자기 존재를 알리는 배선. <b>배분 토글 밖이다.</b>
 *
 * <p>요청만 받는 노드도 분모에 들어가야 한다. 토글 뒤에 두면 리더가 그 노드를
 * 못 세고, 남은 노드가 각자 큰 몫을 써서 총 통과가 전역 크레딧을 넘는다.
 */
@Configuration
public class GatewayPresenceConfig {

    /**
     * 관측한 노드 수.
     *
     * <p><b>배분 토글 밖에 둔다.</b> 하트비트가 여기에 관측을 넣는데, 토글을 끈
     * 노드에도 하트비트가 돌아야 하므로 둘이 같은 자리에 있어야 한다.
     */
    @Bean
    GatewayRegistry gatewayRegistry(ControlPlaneProperties properties) {
        return GatewayRegistry.of(properties.capacity().rampDownTicks(),
                properties.capacity().expectedNodes());
    }

    /**
     * 하트비트 루프.
     *
     * <p>주기는 틱과 같다 — 배분이 한 틱마다 분모를 읽으므로 그보다 드물게 찍으면
     * 멀쩡한 노드가 관측 사이에서 사라진다. 죽은 항목 임계는 신선도와 같다.
     */
    @Bean
    GatewayHeartbeatLoop gatewayHeartbeatLoop(GatewayRedisPort port,
            GatewayRegistry registry, ControlPlaneProperties properties) {
        String instanceId = Leadership.newOwnerId();
        long reapAfterSec = properties.capacity().freshness().toSeconds();
        return GatewayHeartbeatLoop.of(
                () -> port.beat(instanceId, reapAfterSec),
                () -> port.leave(instanceId),
                registry::observed,
                properties.scheduler().tick(),
                properties.leader().attempt());
    }
}

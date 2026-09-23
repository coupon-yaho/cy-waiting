package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.GatewayRedisPort;
import com.kafkick.waiting.domain.admission.CircuitState;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import com.kafkick.waiting.gateway.CircuitStateReader;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.ReactiveRedisClusterConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * 하트비트를 놓친 회차의 배선 (CY-838).
 *
 * <p>레지스트리는 관측 실패를 받으면 감소 연속 수를 지운다. <b>그 호출이 배선에서 빠지면</b> 레디스가
 * 흔들리는 동안 늦은 하트비트로 적게 센 관측이 실패를 사이에 두고도 연속으로 쌓여 멀쩡한 노드가 빠진다.
 */
class GatewayPresenceMissTest {

    private static final int 감소_확정_수 = 3;
    private static final int 노드 = 3;

    private final GatewayPresenceConfig config = new GatewayPresenceConfig();

    /** 연속을 지웠는지 한 칸 줄였는지 가르려고, 놓침 뒤로 감소 확정 수만큼 다시 관측한다. */
    @Test
    @DisplayName("놓친_회차가_감소_연속을_처음부터_다시_세게_한다")
    void 놓친_회차가_감소_연속을_처음부터_다시_세게_한다() {
        GatewayRegistry registry = GatewayRegistry.of(감소_확정_수, 노드);
        Runnable 놓침 = config.missStep(() -> CircuitState.CLOSED, registry);

        registry.observed(노드 - 1);
        registry.observed(노드 - 1);
        놓침.run();

        registry.observed(노드 - 1);
        registry.observed(노드 - 1);
        assertThat(registry.count()).as("놓침 뒤 두 번으로는 확정되지 않는다").isEqualTo(노드);
        registry.observed(노드 - 1);
        assertThat(registry.count()).as("놓침 뒤 세 번째에 확정된다").isEqualTo(노드 - 1);
    }

    @Test
    @DisplayName("놓친_회차가_통과_수를_모름으로_둔다")
    void 놓친_회차가_통과_수를_모름으로_둔다() {
        GatewayRegistry registry = GatewayRegistry.of(감소_확정_수, 노드);
        registry.passObserved(100, 노드, 노드);
        assertThat(registry.passRate()).as("전제").isEqualTo(100);

        config.missStep(() -> CircuitState.CLOSED, registry).run();

        assertThat(registry.passRate()).isEqualTo(-1);
    }

    /**
     * <b>빈이 만든 루프로 잰다.</b> 떼어 낸 메서드만 부르면 빈이 다시 람다로 돌아가도 초록이다 — 이번
     * 결함이 그 모양이었다. 연결을 곧바로 던지게 해 하트비트를 동기로 실패시키고, 가상 시간으로 한 회차만 돈다.
     */
    @Test
    @DisplayName("빈이_만든_루프가_놓침에서_감소_연속을_끊는다")
    void 빈이_만든_루프가_놓침에서_감소_연속을_끊는다() {
        GatewayRegistry registry = GatewayRegistry.of(감소_확정_수, 노드);
        registry.observed(노드 - 1);
        registry.observed(노드 - 1);
        registry.passObserved(100, 노드, 노드);

        GatewayHeartbeatLoop loop = config.gatewayHeartbeatLoop(
                GatewayRedisPort.of(new ReactiveStringRedisTemplate(끊긴_연결())), registry,
                ControlPlaneProperties.defaults(),
                CircuitStateReader.of(CircuitBreakerRegistry.ofDefaults(), "backend"),
                new StaticListableBeanFactory().getBeanProvider(PassRateSource.class),
                new StaticListableBeanFactory().getBeanProvider(InstanceOutliers.class),
                PollRejections.create());
        VirtualTimeScheduler 시계 = VirtualTimeScheduler.create();
        try {
            loop.start(시계);
            시계.advanceTime();
        } finally {
            loop.stop();
        }

        assertThat(registry.passRate()).as("전제 — 놓침 한 회차가 돌았다").isEqualTo(-1);
        registry.observed(노드 - 1);
        assertThat(registry.count()).as("놓침을 사이에 둔 관측은 연속이 아니다").isEqualTo(노드);
    }

    /** 연결을 달라는 순간 던진다. 레디스 없이 하트비트 실패를 만든다. */
    private static ReactiveRedisConnectionFactory 끊긴_연결() {
        return new ReactiveRedisConnectionFactory() {
            @Override
            public ReactiveRedisConnection getReactiveConnection() {
                throw new RedisConnectionFailureException("끊겼다");
            }

            @Override
            public ReactiveRedisClusterConnection getReactiveClusterConnection() {
                throw new RedisConnectionFailureException("끊겼다");
            }

            @Override
            public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
                return null;
            }
        };
    }
}

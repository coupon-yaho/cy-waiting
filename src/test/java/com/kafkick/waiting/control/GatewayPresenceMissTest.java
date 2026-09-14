package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.adapter.redis.GatewayRedisPort;
import com.kafkick.waiting.domain.admission.CircuitState;
import com.kafkick.waiting.gateway.CircuitStateReader;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.scheduler.Schedulers;

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
     * <b>빈이 만든 루프로 잰다.</b> 놓침 경로를 떼어 낸 메서드만 부르면, 빈이 다시 람다로 돌아가도
     * 초록이다 — 이번 결함이 바로 그 모양이었다. 아무도 안 듣는 포트로 하트비트를 실패시킨다.
     */
    @Test
    @DisplayName("빈이_만든_루프가_놓침에서_감소_연속을_끊는다")
    void 빈이_만든_루프가_놓침에서_감소_연속을_끊는다() throws IOException {
        GatewayRegistry registry = GatewayRegistry.of(감소_확정_수, 노드);
        registry.observed(노드 - 1);
        registry.observed(노드 - 1);
        registry.passObserved(100, 노드, 노드);

        LettuceConnectionFactory factory = new LettuceConnectionFactory("localhost", 빈_포트());
        factory.afterPropertiesSet();
        factory.start();
        GatewayHeartbeatLoop loop = config.gatewayHeartbeatLoop(
                GatewayRedisPort.of(new ReactiveStringRedisTemplate(factory)), registry,
                ControlPlaneProperties.defaults(),
                CircuitStateReader.of(CircuitBreakerRegistry.ofDefaults(), "backend"),
                new StaticListableBeanFactory().getBeanProvider(PassRateSource.class));
        try {
            loop.start(Schedulers.newSingle("miss-wiring"));
            // 통과 수가 모름이 되면 놓침 한 회차가 끝났다.
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> registry.passRate() == -1);
        } finally {
            loop.stop();
            factory.destroy();
        }

        registry.observed(노드 - 1);
        assertThat(registry.count()).as("놓침을 사이에 둔 관측은 연속이 아니다").isEqualTo(노드);
    }

    private static int 빈_포트() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}

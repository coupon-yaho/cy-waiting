package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.GatewayRedisPort;
import com.kafkick.waiting.adapter.redis.GatewayRedisPort.Presence;
import com.kafkick.waiting.domain.admission.CircuitState;
import com.kafkick.waiting.gateway.CircuitStateReader;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;
import org.springframework.beans.factory.ObjectProvider;
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
     * 표를 인정하는 신선도. <b>분모의 임계와 분리한다.</b>
     *
     * <p>같이 두면 죽은 노드의 마지막 표가 분모의 임계만큼 살아 있고, 시체
     * 하나가 멀쩡한 클러스터를 그 시간 내내 조인다. <b>기본값에서는 둘이 같다</b> —
     * 임계가 여유 신선도(3초)라 이 값이 거기로 잘린다. 임계를 늘리는 순간 분리가
     * 살아나고, 그때 통과 수의 "모름" 판정도 같이 넓어진다.
     */
    private static final int VOTE_FRESH_TICKS = 5;

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
            CircuitStateReader circuit, ObjectProvider<PassRateSource> passRate) {
        String instanceId = Leadership.newOwnerId();
        long reapAfterSec = properties.capacity().freshness().toSeconds();
        long voteFreshSec = voteFreshSec(properties.scheduler().tick(), reapAfterSec);
        return GatewayHeartbeatLoop.of(
                // **판정 필터가 없어도 돈다.** 이 루프는 그 빈보다 먼저 서고,
                // 판정 필터가 아직 없으면 "모름" 을 싣는다 — 0 으로 실으면 안 잰
                // 노드가 잰 노드로 세어져 합이 모자란 것을 못 안다.
                beatStep(state -> port.beat(instanceId, reapAfterSec, voteFreshSec, state,
                                passed(passRate)),
                        circuit::now, registry),
                () -> port.leave(instanceId),
                registry::observed,
                // 놓침은 상한 바깥에서 센다 — 무응답이 오류로 안 오기 때문이다.
                () -> {
                    registry.circuitMissed(circuit.now());
                    // 통과 수에는 분모 같은 유지 근거가 없다. 낡은 값을 "지금" 이라는
                    // 이름으로 내보내면 장애 내내 지나간 부하를 보고한다.
                    registry.passUnknown();
                },
                properties.scheduler().tick(),
                properties.leader().attempt());
    }

    /**
     * 표를 인정할 초. <b>곱한 뒤에 초로 바꾼다.</b>
     *
     * <p>먼저 초로 바꾸면 1초 미만 틱이 0 으로 잘려 하한 1초가 나간다. 그러면
     * 하트비트 한 회차가 오는 사이에 남의 표가 낡아, 리더가 자기 표만 들고
     * 판단한다 — 클러스터 다수결이 이름만 남는다.
     */
    // 올림한다. 내리면 간격과 같아져 왕복 지연만큼 늘 모자란다.
    static long voteFreshSec(Duration tick, long reapAfterSec) {
        long millis = tick.multipliedBy(VOTE_FRESH_TICKS).toMillis();
        return Math.clamp(Math.ceilDiv(millis, 1000L), 1, reapAfterSec);
    }

    /** 이 노드가 최근에 뒷단으로 보낸 초당 수. 아직 안 붙었으면 음수("모름")다. */
    static long passed(ObjectProvider<PassRateSource> passRate) {
        PassRateSource source = passRate.getIfAvailable();
        return source == null ? -1 : source.passRatePerSec();
    }

    /**
     * 한 번의 하트비트. <b>서킷을 싣고, 클러스터 판정을 받아 적는다</b> (CY-791).
     *
     * <p>배선을 따로 뺀 것은, 이 두 줄이 빠져도 하트비트가 초록으로 돌기
     * 때문이다. 그러면 배분은 리더 한 대의 로컬 서킷으로 크레딧을 정한다.
     */
    // **성공만 여기서 적는다.** 실패는 루프가 상한 바깥에서 센다 — 무응답은
    // 오류로 안 오고 취소로 오기 때문에, 여기서는 볼 수 없다.
    static Supplier<Mono<Integer>> beatStep(Function<CircuitState, Mono<Presence>> beat,
            Supplier<CircuitState> local, GatewayRegistry registry) {
        return () -> beat.apply(local.get())
                .doOnNext(seen -> registry.circuitObserved(seen.alive(), seen.open(),
                        seen.halfOpen()))
                .doOnNext(seen -> registry.passObserved(seen.passed(), seen.passReported(),
                        seen.alive()))
                .map(Presence::alive);
    }
}

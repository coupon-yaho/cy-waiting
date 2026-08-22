package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.adapter.redis.LeaderRedisPort;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import java.time.Instant;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * 제어 평면 배선.
 *
 * <p>조각이 다 있어도 <b>안 엮이면 아무것도 안 돈다.</b> 각자 초록인데 사이가
 * 비어 있으면 배분이 영영 안 돌고, 그 상태로도 프로세스는 멀쩡히 뜬다.
 *
 * <p>토글로 끌 수 있다 — 배분을 도는 무리와 요청만 받는 무리를 나눈다.
 */
@Configuration
@ConditionalOnProperty(name = "waiting.scheduler.enabled", havingValue = "true",
        matchIfMissing = true)
public class ControlPlaneConfig {

    /** 평활화 계수. 클수록 최근 값을 빨리 따라간다. */
    private static final double SMOOTHING_ALPHA = 0.3;

    @Bean
    ControlPlaneProperties controlPlaneProperties() {
        return ControlPlaneProperties.defaults();
    }

    /**
     * 소유자 ID 는 <b>기동마다</b> 새로 만든다.
     *
     * <p>고정하면 재기동한 자신을 이전 소유자로 오인해, 죽기 전에 잡아 둔 락을
     * 새 프로세스가 자기 것으로 알고 연장한다.
     */
    @Bean
    Leadership leadership(LeaderRedisPort port, ControlPlaneProperties properties) {
        String ownerId = Leadership.newOwnerId();
        return Leadership.of(ownerId, properties.leader().lease(), properties.leader().attempt(),
                () -> port.acquire(ownerId), () -> port.release(ownerId));
    }

    @Bean
    GatewayRegistry gatewayRegistry(ControlPlaneProperties properties) {
        return GatewayRegistry.of(properties.capacity().rampDownTicks(),
                properties.capacity().expectedNodes());
    }

    @Bean
    CapacityCollector capacityCollector(ControlPlaneProperties properties) {
        return CapacityCollector.of(properties.capacity().rampUp(),
                properties.capacity().freshness(), properties.capacity().floor(),
                properties.capacity().perInstanceCap());
    }

    @Bean
    DemandCollector demandCollector(AllocationRedisPort port) {
        return DemandCollector.of(port::activeCoupons, port::queueSizes, port::stocks);
    }

    /**
     * 평활화 상태를 <b>이월받아</b> 시작한다.
     *
     * <p>리더가 바뀔 때마다 0 에서 다시 시작하면 그 순간 표시 대기 시간이 튄다.
     * 하필 회복 직후가 진동하기 가장 쉬운 구간이라, 안 이어받으면 가장 나쁠 때
     * 흔들린다. 싣기만 하고 안 읽으면 이월이 반쪽이다.
     */
    @Bean
    AllocationRound allocationRound(DemandCollector collector, AllocationRedisPort port,
            GatewayRegistry registry, CapacityCollector capacity, Leadership leadership) {
        SnapshotCodec codec = SnapshotCodec.create();
        return AllocationRound.of(leadership::isLeader, collector::collect, capacity::lastKnown,
                registry::count, port::apply, port::publish, Instant::now,
                () -> port.load().map(hash ->
                        CreditSmoother.restore(SMOOTHING_ALPHA, codec.smoothing(hash))),
                codec);
    }

    /**
     * 배분 루프는 <b>전용 스레드</b>에서 돈다.
     *
     * <p>공용 스케줄러로 가면 요청 처리 뒤에 줄을 서서, 트래픽이 몰릴 때 정확히
     * 그만큼 틱이 밀린다.
     */
    @Bean(destroyMethod = "")
    Scheduler allocationScheduler() {
        return Schedulers.newSingle("allocation", true);
    }

    @Bean
    LeadershipLoop leadershipLoop(ControlPlaneProperties properties, Leadership leadership,
            Scheduler allocationScheduler) {
        return LeadershipLoop.of(properties.leader().renewDelay(), leadership::renew,
                allocationScheduler);
    }

    @Bean
    ControlPlaneLifecycle controlPlaneLifecycle(LeadershipLoop leadershipLoop,
            AllocationScheduler allocationLoop, Leadership leadership,
            ControlPlaneProperties properties, Scheduler allocationScheduler) {
        return ControlPlaneLifecycle.of(leadershipLoop, allocationLoop, leadership::release,
                properties.leader().attempt(), allocationScheduler);
    }

    @Bean
    AllocationScheduler allocationLoop(ControlPlaneProperties properties, Leadership leadership,
            AllocationRound round, Scheduler allocationScheduler) {
        return AllocationScheduler.of(properties.scheduler().tick(),
                properties.scheduler().firstTickDelay(), leadership::isLeader, round::run,
                nanos -> { }, allocationScheduler);
    }
}

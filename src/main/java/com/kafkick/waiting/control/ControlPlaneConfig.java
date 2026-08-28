package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.adapter.redis.LeaderRedisPort;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.Optional;
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

    /**
     * 큐 앞에서 볼 인원.
     *
     * <p><b>전수 검사는 틱마다 불가능하다.</b> 이번 틱에 배수할 인원 근처만
     * 정확하면 크레딧 낭비는 막힌다. 뒤에 남는 유령은 D-Q1 이 다룬다.
     */
    private static final int SWEEP_SCAN_LIMIT = 1_000;

    /** 이탈 기록 보관. 재방문자를 알아볼 수 있는 구간이다 (5.4절). */
    private static final long GRACE_SEC = 300;

    /** 한 판이 지우는 상한. 스크립트가 `unpack` 한계로 더 좁힌다. */
    private static final int SWEEP_BUDGET = 1_000;

    @Bean
    Leadership leadership(LeaderRedisPort port, ControlPlaneProperties properties) {
        String ownerId = Leadership.newOwnerId();
        return Leadership.of(ownerId, properties.leader().lease(), properties.leader().attempt(),
                () -> port.acquire(ownerId), () -> port.release(ownerId));
    }

    @Bean
    CapacityCollector capacityCollector(ControlPlaneProperties properties) {
        return CapacityCollector.of(properties.capacity().rampUp(),
                properties.capacity().freshness(), properties.capacity().floor(),
                properties.capacity().perInstanceCap());
    }

    @Bean
    DemandCollector demandCollector(AllocationRedisPort port) {
        return DemandCollector.of(port::activeCouponsTimed, port::queueSizes, port::stocks,
                port::queueModes);
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
            GatewayRegistry registry, CapacityCollector capacity, Leadership leadership,
            TunablesRefresh tunables, QueueSweeper sweeper) {
        SnapshotCodec codec = SnapshotCodec.create();
        return AllocationRound.of(leadership::isLeader, collector::collect, capacity::lastKnown,
                registry::count, port::apply, port::publish, Instant::now,
                () -> port.load().map(hash ->
                        CreditSmoother.restore(SMOOTHING_ALPHA, codec.smoothing(hash))),
                codec, capacity::lastFloor, tunables::current, sweeper);
    }

    /**
     * 이탈자 청소 (7.4).
     *
     * <p><b>멈추는 판단을 생성자가 필수로 받는다.</b> 계획이 산문으로 적어 둔
     * "매진·장애 중에는 멈춘다" 를 기계로 만드는 자리다.
     */
    @Bean
    QueueSweeper queueSweeper(AllocationRedisPort port, ControlPlaneProperties properties) {
        return QueueSweeper.of(SweepGate.create(),
                ids -> port.sweep(ids, Instant.now().getEpochSecond(),
                        SWEEP_SCAN_LIMIT, GRACE_SEC, SWEEP_BUDGET));
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

    /**
     * 배분 틱. <b>재료를 먼저 읽고 배분한다</b> — 안 읽으면 수집기가 첫 하한을
     * 영영 답으로 내고, 그 하한에서는 한산 통과 상한이 0 이라 대기열이 통째로
     * 켜진다. 읽기가 실패하면 수집을 건너뛴다 (아래 주석).
     */
    /**
     * 재료 읽기. <b>배분 예산의 1/4 만 쓴다</b> — 한 예산을 나눠 쓰면 읽기가 느릴 때
     * 판이 통째로 안 끝나고, 임계가 안 올라가 큐가 자라 다음 판이 더 무거워진다.
     */
    @Bean
    CapacityRefresh capacityRefresh(AllocationRedisPort port, CapacityCollector capacity,
            GatewayRegistry registry, ControlPlaneProperties properties,
            Scheduler allocationScheduler, MeterRegistry meters) {
        return CapacityRefresh.of(port::capacitySample, capacity, registry::count,
                properties.scheduler().tick().dividedBy(4), allocationScheduler, meters);
    }

    /**
     * 불변식의 선행 지표.
     *
     * <p>초과 발급 자체는 발급 계층만 안다. 게이트웨이는 스스로 계산한 값으로
     * 대신 본다 (6.9.1).
     */
    @Bean
    InvariantMetrics invariantMetrics(AllocationRound round, AllocationRedisPort port,
            MeterRegistry meters) {
        return InvariantMetrics.bind(round, port.clockSkew(), meters);
    }

    /**
     * 운영 값 읽기. <b>배분 판 밖이다</b> — 판 안에서 읽으면 발행이 그 왕복에
     * 매달려, 레디스가 조금 느려지는 것만으로 스냅샷이 아예 안 나간다.
     */
    @Bean
    TunablesRefresh tunablesRefresh(AllocationRedisPort port, SnapshotHolder holder,
            ControlPlaneProperties properties, Scheduler allocationScheduler,
            MeterRegistry meters) {
        // **승계 첫 판이 위험하다.** 새 리더의 캐시는 비어 있는데 그 상태로
        // 발행하면 앞 리더가 싣던 값이 지워진다 — 재료에 있던 것을 이어 싣는다.
        TunablesRefresh refresh = TunablesRefresh.of(port::readTunables,
                () -> Optional.ofNullable(holder.current().meta().tunables()),
                properties.scheduler().tick().dividedBy(4), allocationScheduler);
        // **게이지는 마지막 값을 계속 낸다.** 못 읽고 있다는 사실은 이 값으로만
        // 드러나고, 없으면 "5분째 못 받음" 을 걸 곳이 없다.
        Gauge.builder("waiting.tunable.stale.seconds", refresh,
                        TunablesRefresh::staleSeconds)
                .description("운영 값을 마지막으로 읽은 지 몇 초. 리더만 오른다")
                .strongReference(true)
                .register(meters);
        return refresh;
    }

    /** 배분 틱. <b>재료를 먼저 읽고 배분한다</b> — 안 읽으면 크레딧이 첫 하한에 머문다. */
    @Bean
    AllocationScheduler allocationLoop(ControlPlaneProperties properties, Leadership leadership,
            AllocationRound round, CapacityRefresh capacity, CapacityCollector collector,
            TunablesRefresh tunables, Scheduler allocationScheduler) {
        return AllocationScheduler.of(properties.scheduler().tick(),
                properties.scheduler().firstTickDelay(),
                // **승계는 유예를 처음부터 준다.** 비리더 구간에 얼어 있던 실패
                // 횟수를 이어 쓰면 재승계 첫 판이 곧바로 크레딧을 깎는다.
                LeadershipEdge.of(leadership::isLeader,
                        () -> {
                            collector.leadershipAcquired();
                            capacity.leadershipChanged();
                        },
                        capacity::leadershipChanged),
                // **운영 값을 먼저 읽고 배분한다.** 순서가 뒤면 방금 바꾼 값이
                // 한 틱 늦게 나가고, 장애 중의 한 틱은 길다.
                () -> capacity.refresh().then(tunables.refresh()).then(round.run()),
                nanos -> { }, allocationScheduler);
    }
}

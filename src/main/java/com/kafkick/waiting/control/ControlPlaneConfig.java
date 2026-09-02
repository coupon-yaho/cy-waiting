package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.adapter.redis.LeaderRedisPort;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import io.micrometer.core.instrument.Gauge;
import com.kafkick.waiting.domain.queue.GraceRetention;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import reactor.core.publisher.Mono;
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

    /** 이탈 기록 보관. <b>등록도 같은 값을 읽는다</b> — 갈리면 판정이 갈린다. */
    private static final long GRACE_SEC = GraceRetention.SECONDS;

    /** 한 회차가 지우는 상한. 스크립트가 `unpack` 한계로 더 좁힌다. */
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
            TunablesRefresh tunables, ControlPlaneProperties properties,
            SoldOutCleanup cleanup, QueueSweeper sweeper, SnapshotHolder holder) {
        SnapshotCodec codec = SnapshotCodec.create();
        return AllocationRound.of(leadership::isLeader, collector::collect,
                capacity::lastKnown,
                registry::count, port::apply, port::publish, Instant::now,
                () -> port.load().map(hash ->
                        CreditSmoother.restore(CreditSmoother.DEFAULT_ALPHA, codec.smoothing(hash))),
                codec, capacity::lastFloor, tunables::current,
                // **유예를 값으로 정한다** (7.3.2). 스냅샷 낡음 한계보다 충분히
                // 커야 마지막 폴링이 줄을 안 잃는다.
                cleanup,
                // **이제 실제로 지운다** (5.3.1). 선결 조건 셋이 다 닫혔다 —
                // 재고 미상과 0 을 가르고(CY-702), 지우기 직전에 재고를 다시
                // 보고(CY-765), 옛 리더의 명령을 울타리가 거른다(CY-766).
                //
                // **회차 번호를 그때그때 읽는다.** 붙잡아 두면 강등된 뒤에도 옛
                // 번호로 나가고, 그건 울타리를 우회하는 일이다. 리더가 아니면
                // 0 이 나가고 스크립트가 전부 거절한다 — 안전한 방향이다.
                ids -> port.dropSoldOutQueues(ids, leadership.fence()),
                // 세기 시작한 줄에 표만 세운다. 지웠을 때만 세우면 한 번도 안
                // 지운 줄에 표가 없어, 얼었다 깨어난 옛 리더를 못 막는다.
                ids -> port.claimSoldOutQueues(ids, leadership.fence()),
                sweeper,
                // 이 노드도 게이트웨이다. 자기가 든 재료의 나이가 노드들의
                // 폴링 상태에 가장 가까운 신호다.
                holder::isDataStale,
                // **클러스터가 본 것으로 조인다** (CY-791). 리더의 로컬 서킷을
                // 쓰면 리더만 멀쩡할 때 나머지가 다 열려 있어도 평소 속도로
                // 돌고, 그 몫은 이미 넘어진 뒷단으로 간다. 하트비트가 노드마다
                // 실어 온 것을 등록부가 다수결로 접어 둔다.
                //
                // **회차마다 한 번 읽는다.** 한 회차에서 두 번 읽으면 그 사이에
                // 상태가 뒤집혀 같은 회차가 자기모순인 값 둘로 판단한다.
                registry::circuit);
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
     * 회차가 통째로 안 끝나고, 임계가 안 올라가 큐가 자라 다음 회차가 더 무거워진다.
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
        return InvariantMetrics.bind(round, port.clockSkew(), meters, port::markersDropped);
    }

    /**
     * 운영 값 읽기. <b>배분 회차 밖이다</b> — 회차 안에서 읽으면 발행이 그 왕복에
     * 매달려, 레디스가 조금 느려지는 것만으로 스냅샷이 아예 안 나간다.
     */
    @Bean
    TunablesRefresh tunablesRefresh(AllocationRedisPort port, SnapshotHolder holder,
            ControlPlaneProperties properties, Scheduler allocationScheduler,
            MeterRegistry meters) {
        // **승계 첫 회차가 위험하다.** 새 리더의 캐시는 비어 있는데 그 상태로
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

    /** 배분 라운드와 리더십 경계가 같은 것을 봐야 한다 — 따로 만들면 승계에서 셈이 안 버려진다. */
    @Bean
    SoldOutCleanup soldOutCleanup(ControlPlaneProperties properties, MeterRegistry meters) {
        return SoldOutCleanup.of(properties.scheduler().soldOutGraceTicks(), meters);
    }

    /** 멈추는 판단을 생성자가 필수로 받는다 — 빠뜨리면 컴파일이 안 된다 (7.4). */
    @Bean
    QueueSweeper queueSweeper(AllocationRedisPort port, ControlPlaneProperties properties,
            MeterRegistry meters) {
        return QueueSweeper.of(SweepGate.of(properties.scheduler().tick(), PollIntervalPolicy.aliveTtl()),
                (ids, scanLimit, removeFront) -> port.sweep(ids, Instant.now().getEpochSecond(),
                        scanLimit, GRACE_SEC, SWEEP_BUDGET, removeFront),
                meters);
    }

    /** 배분 틱. <b>재료를 먼저 읽고 배분한다</b> — 안 읽으면 크레딧이 첫 하한에 머문다. */
    /**
     * 리더가 된 순간에 처음부터 줘야 하는 것들.
     *
     * <p><b>람다로 묻어 두지 않는다.</b> 여기 한 줄을 빠뜨리면 그 셈만 얼어
     * 있던 값을 이어 쓰는데, 그건 전 시험이 초록인 채로 일어난다.
     */
    static Runnable onLeadershipGained(CapacityCollector collector, CapacityRefresh capacity,
            SoldOutCleanup cleanup, QueueSweeper sweeper, AllocationRound round) {
        return () -> {
            collector.leadershipAcquired();
            capacity.leadershipChanged();
            // **평활화 이월도 여기서 버린다** (F9 · CY-859). 회차 안에서 버리려
            // 하면 그 회차는 리더일 때만 도므로 비리더 구간을 한 번도 못 본다 —
            // 되찾은 노드가 남이 움직인 값을 못 보고 옛 값을 이어 쓴다.
            round.leadershipAcquired();
            // **매진 유예를 처음부터 준다.** 얼어 있던 셈을 이어 쓰면 유예가
            // 설정값이 아니라 "내가 리더였던 틱 수" 가 되고, 그 둘은 장애
            // 중에 갈린다.
            cleanup.leadershipAcquired();
            // **이탈자 청소의 재개 유예도 같다** (CY-822). 그 표시는 리더
            // 메모리라 승계에서 사라지고, 새 리더는 신호가 얼마나 오래 멎어
            // 있었는지 모른다. 모른다는 것이 걷을 이유가 되면 안 된다 —
            // 걷힌 사람은 새 score 로 다시 서므로 순번이 뒤로 간다.
            sweeper.leadershipAcquired();
        };
    }

    @Bean
    AllocationScheduler allocationLoop(ControlPlaneProperties properties, Leadership leadership,
            AllocationRound round, CapacityRefresh capacity, CapacityCollector collector,
            TunablesRefresh tunables, Scheduler allocationScheduler, SoldOutCleanup cleanup,
            QueueSweeper sweeper) {
        return AllocationScheduler.of(properties.scheduler().tick(),
                properties.scheduler().firstTickDelay(),
                // **승계는 유예를 처음부터 준다.** 비리더 구간에 얼어 있던 실패
                // 횟수를 이어 쓰면 재승계 첫 회차가 곧바로 크레딧을 깎는다.
                LeadershipEdge.of(leadership::isLeader,
                        onLeadershipGained(collector, capacity, cleanup, sweeper, round),
                        capacity::leadershipChanged),
                // **운영 값을 먼저 읽고 배분한다.** 순서가 뒤면 방금 바꾼 값이
                // 한 틱 늦게 나가고, 장애 중의 한 틱은 길다.
                () -> capacity.refresh().then(tunables.refresh()).then(round.run()),
                nanos -> { }, allocationScheduler);
    }
}

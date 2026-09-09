package com.kafkick.waiting.control;

import com.kafkick.waiting.adapter.redis.AllocationRedisPort;
import com.kafkick.waiting.domain.routing.AllowedDestinations;
import com.kafkick.waiting.routing.RoutingProperties;
import java.time.Duration;
import com.kafkick.waiting.adapter.redis.LeaderRedisPort;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import com.kafkick.waiting.domain.queue.GraceRetention;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import reactor.core.publisher.Mono;
import java.time.Instant;
import java.util.Optional;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * 제어 평면 배선. 조각이 다 있어도 <b>안 엮이면 아무것도 안 돈다</b> — 각자 초록인데
 * 사이가 비어 있으면 배분이 영영 안 돌고, 그 상태로도 프로세스는 멀쩡히 뜬다. 토글로
 * 끌 수 있다: 배분을 도는 무리와 요청만 받는 무리를 나눈다.
 */
@Configuration
@ConditionalOnProperty(name = "waiting.scheduler.enabled", havingValue = "true",
        matchIfMissing = true)
public class ControlPlaneConfig {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneConfig.class);

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

    /**
     * <b>라우팅이 켜졌을 때만 목적지를 제한한다.</b> 노브 자체가 켠 배포에만 뜨므로
     * 없으면 라우팅이 꺼진 것이다. 목록이 비면 못 켜게 막는 것은 그 노브가 든다.
     */
    @Bean
    CapacityCollector capacityCollector(ControlPlaneProperties properties,
            ObjectProvider<RoutingProperties> routing, MeterRegistry meters) {
        Duration rampUp = properties.capacity().rampUp();
        Duration freshness = properties.capacity().freshness();
        long floor = properties.capacity().floor();
        long cap = properties.capacity().perInstanceCap();
        RoutingProperties on = routing.getIfAvailable();
        AllowedDestinations allowed = on == null || !on.enabled()
                ? AllowedDestinations.unrestricted()
                : AllowedDestinations.of(on.allowedDestinations(), on.allowedPorts());
        CapacityCollector collector =
                CapacityCollector.of(rampUp, freshness, floor, cap, allowed);
        Gauge.builder("waiting.routing.destination.denied", collector,
                        CapacityCollector::deniedDestinations)
                .description("직전 회차에 허용 목적지 밖이라 라우팅에서 뺀 인스턴스 수. 누적이 아니다")
                .strongReference(true)
                .register(meters);
        return collector;
    }

    @Bean
    DemandCollector demandCollector(AllocationRedisPort port) {
        return DemandCollector.of(port::activeCouponsTimed, port::queueSizes, port::stocks,
                port::queueModes);
    }

    /**
     * 평활화 상태를 <b>이월받아</b> 시작한다. 0 에서 다시 시작하면 표시 대기 시간이
     * 튀는데, 하필 회복 직후가 진동하기 가장 쉬운 구간이다. 싣기만 하고 안 읽으면
     * 이월이 반쪽이다.
     */
    @Bean
    AllocationRound allocationRound(DemandCollector collector, AllocationRedisPort port,
            GatewayRegistry registry, CapacityCollector capacity, Leadership leadership,
            TunablesRefresh tunables, ControlPlaneProperties properties,
            SoldOutCleanup cleanup, QueueSweeper sweeper, SnapshotHolder holder) {
        SnapshotCodec codec = SnapshotCodec.create();
        AllocationRound round = AllocationRound.of(leadership::isLeader, collector::collect,
                capacity::lastKnown,
                registry::count,
                // **임기를 회차마다 다시 읽는다.** 붙잡아 두면 강등된 뒤에도 옛
                // 번호로 나가고, 그것이 울타리가 막으려던 바로 그 경우다.
                grant -> port.apply(grant, leadership.fence()),
                hash -> port.publish(hash, leadership.fence()), Instant::now,
                () -> port.load().map(hash ->
                        CreditSmoother.restore(CreditSmoother.DEFAULT_ALPHA, codec.smoothing(hash))),
                codec, capacity::lastFloor, tunables::current,
                // **유예를 값으로 정한다.** 스냅샷 낡음 한계보다 충분히
                // 커야 마지막 폴링이 줄을 안 잃는다.
                cleanup,
                // **이제 실제로 지운다.** 선결 조건 셋이 닫혔다 — 재고 미상과 0 을 가르고,
                // 지우기 직전 재고를 다시 보고, 울타리가 옛 리더를 거른다. 회차 번호를
                // 붙잡으면 그 울타리를 우회한다.
                ids -> port.dropSoldOutQueues(ids, leadership.fence()),
                // 세기 시작한 줄에 표만 세운다. 지웠을 때만 세우면 한 번도 안
                // 지운 줄에 표가 없어, 얼었다 깨어난 옛 리더를 못 막는다.
                ids -> port.claimSoldOutQueues(ids, leadership.fence()),
                sweeper,
                // 이 노드도 게이트웨이다. 자기가 든 재료의 나이가 노드들의
                // 폴링 상태에 가장 가까운 신호다.
                holder::isDataStale,
                // **클러스터가 본 것으로 조인다.** 리더의 로컬 서킷만 보면 리더만
                // 멀쩡할 때 그 몫이 이미 넘어진 뒷단으로 간다. 회차마다 한 번 읽는다 —
                // 두 번 읽으면 그 사이 뒤집혀 같은 회차가 자기모순인 값 둘로 판단한다.
                registry::circuit,
                // **라우팅 목록도 같이 싣는다.** 보고는 리더만 읽으므로, 요청 경로에서
                // 레디스를 치지 않으려면 이 길밖에 없다. 합산에 든 값 그대로라 갓 뜬
                // 인스턴스의 램프가 깎은 몫이 여기에도 실린다.
                capacity::routable);
        return round;
    }

    /**
     * 배분 루프는 <b>전용 스레드</b>에서 돈다. 공용 스케줄러로 가면 요청 처리 뒤에
     * 줄을 서서, 트래픽이 몰릴 때 정확히 그만큼 틱이 밀린다.
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
     * 불변식의 선행 지표. 초과 발급 자체는 발급 계층만 알므로, 게이트웨이는 스스로
     * 계산한 값으로 대신 본다.
     */
    @Bean
    InvariantMetrics invariantMetrics(AllocationRound round, AllocationRedisPort port,
            MeterRegistry meters, GatewayRegistry registry) {
        // **도착 합은 배분을 안 거친다.** 상한으로 쓰면 관측이 제 출력에 오염돼
        // 진동하므로 뺐다. 남은 쓰임이 지표뿐이라 여기로 바로 온다.
        // **울타리 거절도 같이 낸다.** 막힌 동안 전 노드가 얼어붙은 재료를 읽는데,
        // 진입 로그는 구간의 첫 건뿐이라 그것만으로는 길이를 못 센다.
        FunctionCounter.builder("waiting.snapshot.publish.fenced", port,
                        AllocationRedisPort::publishFenced)
                .description("울타리가 막은 발행 회차 수. 0 이 아니면 이 노드의 재료가 안 나갔다")
                .register(meters);
        // **되돌릴 수 없는 쓰기가 막힌 수다.** 그 창 동안 죽은 줄이 폴링 예산을
        // 먹는데, 안 내면 막혔다는 사실이 어디에도 안 남는다 (CY-894).
        FunctionCounter.builder("waiting.queue.drop.fenced", port,
                        AllocationRedisPort::dropFenced)
                .description("옛 임기라 막힌 매진 큐 삭제 수")
                .register(meters);
        FunctionCounter.builder("waiting.allocation.apply.fenced", port,
                        AllocationRedisPort::applyFenced)
                .description("울타리가 막은 입장 적용 건수. 쿠폰마다 오르므로 회차 수가 아니다")
                .register(meters);
        return InvariantMetrics.bind(round, port.clockSkew(), meters, port::markersDropped,
                registry::passRate);
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

    /** 멈추는 판단을 생성자가 필수로 받는다 — 빠뜨리면 컴파일이 안 된다. */
    @Bean
    QueueSweeper queueSweeper(AllocationRedisPort port, ControlPlaneProperties properties,
            MeterRegistry meters) {
        return QueueSweeper.of(SweepGate.of(properties.scheduler().tick(), PollIntervalPolicy.aliveTtl()),
                (ids, scanLimit, removeFront) -> port.sweep(ids, Instant.now().getEpochSecond(),
                        scanLimit, GRACE_SEC, SWEEP_BUDGET, removeFront),
                meters);
    }

    /**
     * 리더가 된 순간에 처음부터 줘야 하는 것들. <b>람다로 묻어 두지 않는다</b> — 한 줄을
     * 빠뜨리면 그 셈만 얼어 있던 값을 이어 쓰고, 그건 전 시험이 초록인 채로 일어난다.
     */
    Runnable onLeadershipGained(CapacityCollector collector, CapacityRefresh capacity,
            SoldOutCleanup cleanup, QueueSweeper sweeper, AllocationRound round,
            SnapshotHolder holder, GatewayRegistry registry, Runnable sealFences) {
        return () -> {
            // **문을 먼저 잠근다.** 적용만으로는 그 쿠폰에 크레딧이 갈 때까지 표에
            // 옛 임기가 남고, 그 창에 유령이 먼저 도착하면 자기 번호와 같아서
            // 통과한다 — 같은 초에 두 리더의 몫이 다 나가면 초과 발급이다.
            sealFences.run();
            collector.leadershipAcquired();
            capacity.leadershipChanged();
            // **평활화 이월도 여기서 버린다.** 회차 안은 리더일 때만 돌아 비리더 구간을
            // 한 번도 못 본다. 램프 출발점은 발행된 몫이되 낡으면 한산 통과가 살아 있는
            // 최소 몫 — 0 에서 오르면 한산한 쿠폰이 줄을 서고, 낡은 큰 값은 브레이크를 푼다.
            SnapshotHolder.View seen = holder.view();
            round.leadershipAcquired(startingCredit(seen, holder, registry));
            // **매진 유예를 처음부터 준다.** 얼어 있던 셈을 이어 쓰면 유예가
            // 설정값이 아니라 "내가 리더였던 틱 수" 가 되고, 그 둘은 장애
            // 중에 갈린다.
            cleanup.leadershipAcquired();
            // **이탈자 청소의 재개 유예도 같다.** 표시가 리더 메모리라 승계에서
            // 사라지지만, 모른다는 것이 걷을 이유가 되면 안 된다 — 걷힌 사람은 새 score 로
            // 다시 서므로 순번이 뒤로 간다.
            sweeper.leadershipAcquired();
        };
    }

    /**
     * 승계 직후 활성 쿠폰의 문을 잠근다. <b>못 잠가도 회차는 돈다</b> — 안 잠긴
     * 쿠폰은 적용이 그 자리에서 다시 막으므로, 여기서 막으면 회복만 늦어진다.
     *
     * <p><b>매진 큐 삭제의 문도 같이 잠근다</b> (CY-894). 그쪽 표는 후보가 될 때
     * 서므로 승계와 첫 틱 사이가 비고, 그 창의 쓰기는 되돌릴 수 없다. 한 스크립트로
     * 둘을 잠근다 — 표마다 왕복하면 배분이 안 도는 시간이 곱해진다.
     */
    Runnable sealFences(AllocationRedisPort port, Leadership leadership, SealGate gate) {
        return () -> {
            long generation = gate.sealing();
            long fence = leadership.fence();
            // **권위 있는 자리에서 읽는다.** 마지막 발행의 쿠폰을 쓰면 발행이 밀렸거나
            // 갱신이 실패한 구간에 새로 활성이 된 쿠폰이 빠지고, 그 쿠폰이 정확히
            // 유령의 지연된 몫을 받는 자리다.
            Mono<Long> coupons = port.activeCoupons()
                    .flatMap(active -> port.sealFences(active, fence)
                            .doOnNext(locked -> {
                                if (locked < active.size()) {
                                    log.warn("울타리를 다 못 잠갔다 — {}/{} 개, 임기 {}. "
                                            + "못 잠근 쿠폰은 적용과 삭제가 그 자리에서 "
                                            + "다시 막는다", locked, active.size(), fence);
                                }
                            }))
                    // **못 잠가도 회차는 연다.** 여기서 멈추면 아무도 배분을 안 돌아
                    // 줄이 통째로 멎는다 — 못 잠근 쿠폰은 적용이 다시 막는다.
                    .doOnError(e -> log.warn("울타리를 못 잠갔다 — 임기 {}", fence, e))
                    .onErrorReturn(0L);
            // **발행의 문도 같이 잠근다** (CY-911). 그 표는 첫 발행에야 서므로 승계와
            // 첫 틱 사이가 비고, 그 창의 발행에 정리와 청소가 매달려 같이 나간다.
            Mono<Long> snapshot = port.sealSnapshotFence(fence)
                    // **못 잠근 것도 남긴다.** 리더가 됐다고 믿는 노드가 여기서
                    // 실패하는 것이 리더 둘을 가장 싸게 잡는 신호인데, 값으로만
                    // 두면 아무 데도 안 남는다.
                    .doOnNext(locked -> {
                        if (locked == 0) {
                            log.warn("발행 울타리를 못 잠갔다 — 임기 {}. 리더가 아니거나 더 "
                                    + "앞선 임기가 서 있다는 뜻이고, 둘 다 이 노드의 발행이 "
                                    + "계속 막힌다는 뜻이다", fence);
                        }
                    })
                    .doOnError(e -> log.warn("발행 울타리를 못 잠갔다 — 임기 {}. 첫 발행이 "
                            + "설 때까지 유령의 재료가 나갈 수 있다", fence, e))
                    .onErrorReturn(0L);
            // **발행의 문은 게이트가 안 기다린다.** 이 노드의 첫 발행이 어차피 같은
            // 번호를 심으므로 배분을 세울 이유가 없고, 스냅샷 슬롯이 죽어 있으면
            // 명령 시한만큼 새 리더의 첫 틱이 통째로 사라진다.
            snapshot.subscribe();
            coupons
                    // **이 잠금의 세대로 연다.** 승계가 잦으면 첫 잠금의
                    // 완료가 둘째 잠금이 도는 중에 문을 열어 버린다.
                    .doFinally(signal -> gate.sealed(generation))
                    .subscribe();
        };
    }

    /**
     * 승계 노드가 램프를 세울 출발점. 낡은 큰 값은 브레이크를 풀고, 안 주면 브레이크가
     * 없다. <b>한산 통과 하한은 여기서 안 지킨다</b> — 되올리는 것은
     * {@code ReleaseRamp.next} 다.
     *
     * @return 기동 직후면 음수(램프 없음), 그 밖에는 발행 몫이나 한산 통과 최소 몫 이하
     */
    long startingCredit(SnapshotHolder.View seen, SnapshotHolder holder,
            GatewayRegistry registry) {
        long floor = CapacityCollector.idleMinimum(registry.count());
        if (!seen.snapshot().isPublished()) {
            // **못 읽은 것과 아직 안 읽은 것은 다르다.** 샤드가 끊긴 채 몇 시간
            // 떠 있던 노드도 여기 오는데, 램프를 안 걸면 첫 틱이 목표를 통째로
            // 발행한다. 기동 직후만 면제한다.
            return seen.isBeforeFirstTick() ? -1 : floor;
        }
        long published = seen.snapshot().meta().globalCredit();
        return holder.isDataStale(seen) ? Math.min(published, floor) : published;
    }

    /**
     * 배분 틱. <b>재료를 먼저 읽고 배분한다</b> — 안 읽으면 수집기가 첫 하한을 영영 답으로
     * 내고, 그 하한에서는 한산 통과 상한이 0 이라 대기열이 통째로 켜진다. 읽기가 실패하면
     * 수집을 건너뛴다.
     */
    @Bean
    AllocationScheduler allocationLoop(ControlPlaneProperties properties, Leadership leadership,
            AllocationRound round, CapacityRefresh capacity, CapacityCollector collector,
            TunablesRefresh tunables, Scheduler allocationScheduler, SoldOutCleanup cleanup,
            QueueSweeper sweeper, SnapshotHolder holder, GatewayRegistry registry,
            AllocationRedisPort port) {
        SealGate gate = SealGate.of(leadership::isLeader);
        return AllocationScheduler.of(properties.scheduler().tick(),
                properties.scheduler().firstTickDelay(),
                // **승계는 유예를 처음부터 준다.** 비리더 구간에 얼어 있던 실패
                // 횟수를 이어 쓰면 재승계 첫 회차가 곧바로 크레딧을 깎는다.
                // **문을 잠글 때까지 리더로 안 친다.** 잠금이 끝나기 전에 회차가
                // 돌면, 새 리더가 안 만지는 쿠폰에 유령의 지연된 몫이 그대로 들어간다.
                LeadershipEdge.of(gate,
                        onLeadershipGained(collector, capacity, cleanup, sweeper, round, holder,
                                registry, sealFences(port, leadership, gate)),
                        capacity::leadershipChanged),
                // **운영 값을 먼저 읽고 배분한다.** 순서가 뒤면 방금 바꾼 값이
                // 한 틱 늦게 나가고, 장애 중의 한 틱은 길다.
                () -> capacity.refresh().then(tunables.refresh()).then(round.run()),
                nanos -> { }, allocationScheduler);
    }
}

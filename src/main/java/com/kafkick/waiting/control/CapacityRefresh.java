package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * 배분 재료를 읽어 수집기에 넣는다.
 *
 * <p><b>자기 예산을 갖는다.</b> 배분과 한 예산을 나눠 쓰면 읽기가 느려질 때 판이
 * 통째로 안 끝나고, 그러면 임계가 안 올라가 큐가 자라 다음 판이 더 무거워진다.
 */
public final class CapacityRefresh {

    private static final Logger log = LoggerFactory.getLogger(CapacityRefresh.class);

    private final Supplier<Mono<CapacitySample>> sample;
    private final CapacityCollector collector;
    private final IntSupplier nodes;
    private final Duration budget;
    /** 타임아웃 타이머와 수집이 함께 도는 곳. 배분과 같은 스레드다. */
    private final Scheduler worker;
    private final FailureWindow failures = FailureWindow.create();
    private final FailureWindow decaying = FailureWindow.create();
    private final FailureWindow pinned = FailureWindow.create();
    private final Counter readFailed;
    private final AtomicLong credit = new AtomicLong();
    private final AtomicInteger observed = new AtomicInteger();

    private CapacityRefresh(Supplier<Mono<CapacitySample>> sample,
            CapacityCollector collector, IntSupplier nodes,
            Duration budget, Scheduler worker, MeterRegistry meters) {
        if (budget == null || budget.isZero() || budget.isNegative()) {
            throw new IllegalArgumentException("budget 은 양수여야 한다: %s".formatted(budget));
        }
        this.sample = Objects.requireNonNull(sample, "sample 은 필수다");
        this.collector = Objects.requireNonNull(collector, "collector 는 필수다");
        this.nodes = Objects.requireNonNull(nodes, "nodes 는 필수다");
        this.budget = budget;
        this.worker = Objects.requireNonNull(worker, "worker 는 필수다");
        Objects.requireNonNull(meters, "meters 는 필수다");
        // **게이지로 둔다.** 판정의 분자와 분모라 지금 값이 궁금하지 누적이 아니다.
        meters.gauge("waiting.capacity.credit", credit, AtomicLong::get);
        meters.gauge("waiting.capacity.nodes", observed, AtomicInteger::get);
        // 못 읽은 판은 누적이 맞다 — 구간의 길이를 이 값으로 잰다.
        this.readFailed = meters.counter("waiting.capacity.read.failed");
    }

    public static CapacityRefresh of(Supplier<Mono<CapacitySample>> sample,
            CapacityCollector collector, IntSupplier nodes,
            Duration budget, Scheduler worker, MeterRegistry meters) {
        return new CapacityRefresh(sample, collector, nodes, budget, worker, meters);
    }

    /**
     * 한 판. <b>실패해도 완료로 끝난다</b> — 재료를 못 읽은 것이 배분을 막을 이유는
     * 아니다. 직전 값으로 돈다.
     */
    public Mono<Void> refresh() {
        // **읽기와 시각을 한 예산 안에서 같이 받는다.** 시각을 따로 받으면 그
        // 왕복이 예산 밖이라, 느릴 때 관측과 기준 시각이 서로 다른 순간의 것이 된다.
        // **보고와 기준 시각을 한 왕복으로 받는다.** 따로 내면 같은 순간이
        // 아니고, 클러스터에서는 아예 다른 노드의 벽시계가 된다.
        return Mono.defer(sample)
                // **타이머도 배분 스케줄러다.** 기본 스케줄러를 쓰면 제어 평면의
                // 시간과 분리되고, 시험이 가상 시간으로 재지 못한다.
                .timeout(budget, worker)
                // **수집을 레디스 이벤트 루프에서 돌리지 않는다.** 램프 기록은
                // 동기화 없는 맵이고, 재연결로 루프가 갈리면 두 스레드가 만진다.
                .publishOn(worker)
                .doOnNext(read -> collected(read.reports(), read.now()))
                .doOnError(this::failed)
                .onErrorResume(e -> Mono.empty())
                .then();
    }

    private void collected(List<CapacityReport> read, long now) {
        int seen = nodes.getAsInt();
        long value = collector.collect(read, now, seen);
        credit.set(value);
        observed.set(seen);
        // **하한에 박힌 것은 모드 전환이다.** 진입도 해제도 안 남기면, 크레딧이
        // 하한에 고정돼 한산 통과가 사실상 막힌 것을 사람이 게이지를 보고
        // 있어야만 안다 (LG-2).
        if (collector.lastFloor() > 0) {
            if (pinned.entered()) {
                log.warn("신선한 가용량 보고가 없다 — 크레딧을 하한 {}로 묶는다", value);
            }
        } else {
            pinned.exited().ifPresent(recovered ->
                    log.info("가용량 보고가 다시 온다 — {}초 만에, 그동안 {}판 하한이었다",
                            recovered.elapsedSeconds(), recovered.swallowed()));
        }
        failures.exited().ifPresent(recovered ->
                log.info("가용량을 다시 읽는다 — {}초 만에, 그동안 {}판 걸렀다",
                        recovered.elapsedSeconds(), recovered.swallowed()));
        decaying.exited().ifPresent(recovered ->
                log.info("크레딧을 다시 관측으로 낸다 — {}초 만에, 그동안 {}판 깎았다. 지금 {}",
                        recovered.elapsedSeconds(), recovered.swallowed(), value));
    }

    /**
     * 리더십이 갈렸다. <b>열린 창을 전부 닫는다.</b>
     *
     * <p>안 닫으면 다음 리더의 첫 실패가 진입으로 안 잡혀 로그가 빠지고, 그 뒤
     * 복귀 로그의 지속 시간에 비리더 구간이 섞인다 (LG-2).
     */
    public void leadershipChanged() {
        failures.exited().ifPresent(recovered ->
                log.info("리더십이 갈렸다 — 읽기 실패 창을 닫는다. 그동안 {}판 걸렀다",
                        recovered.swallowed()));
        decaying.exited().ifPresent(recovered ->
                log.info("리더십이 갈렸다 — 감쇠 창을 닫는다. 그동안 {}판 깎았다",
                        recovered.swallowed()));
    }

    /**
     * <b>0건과 못 읽은 것은 다르다.</b> 빈 목록을 넘기면 하한으로 떨어져 전면
     * 억제가 되는데, 그건 관측이 아니라 왕복 실패다.
     */
    private void failed(Throwable e) {
        long before = collector.lastKnown();
        collector.observationFailed(nodes.getAsInt());
        long after = collector.lastKnown();
        // **게이지가 배분값을 따라가야 한다.** 성공 판에서만 갱신하면 감쇠가 도는
        // 동안 지표는 장애 직전 값에 얼어 있고, 배분은 그와 다른 값으로 돈다 —
        // 회복 판정이 "아무 일도 없었다" 로 자동 통과한다 (RC6).
        credit.set(after);
        readFailed.increment();
        if (failures.entered()) {
            log.warn("가용량을 못 읽는다 — 직전 값으로 배분한다: {}", e.toString());
        }
        // 감쇠는 읽기 실패와 다른 모드다. 진입 조건도 해제 조건도 명확한데
        // 안 남기면 크레딧이 어디까지 깎였는지를 사후에 못 밝힌다 (LG-2).
        if (after < before && decaying.entered()) {
            log.warn("가용량을 오래 못 읽는다 — 크레딧을 깎기 시작한다: {} → {}", before, after);
        }
    }

}

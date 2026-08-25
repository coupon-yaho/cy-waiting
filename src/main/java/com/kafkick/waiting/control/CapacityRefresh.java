package com.kafkick.waiting.control;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
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

    private final Supplier<Mono<List<CapacityReport>>> reports;
    private final CapacityCollector collector;
    private final Supplier<Instant> clock;
    private final Duration budget;
    /** 타임아웃 타이머와 수집이 함께 도는 곳. 배분과 같은 스레드다. */
    private final Scheduler worker;
    private final FailureWindow failures = FailureWindow.create();

    private CapacityRefresh(Supplier<Mono<List<CapacityReport>>> reports,
            CapacityCollector collector, Supplier<Instant> clock, Duration budget,
            Scheduler worker) {
        if (budget == null || budget.isZero() || budget.isNegative()) {
            throw new IllegalArgumentException("budget 은 양수여야 한다: %s".formatted(budget));
        }
        this.reports = Objects.requireNonNull(reports, "reports 는 필수다");
        this.collector = Objects.requireNonNull(collector, "collector 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.budget = budget;
        this.worker = Objects.requireNonNull(worker, "worker 는 필수다");
    }

    public static CapacityRefresh of(Supplier<Mono<List<CapacityReport>>> reports,
            CapacityCollector collector, Supplier<Instant> clock, Duration budget,
            Scheduler worker) {
        return new CapacityRefresh(reports, collector, clock, budget, worker);
    }

    /**
     * 한 판. <b>실패해도 완료로 끝난다</b> — 재료를 못 읽은 것이 배분을 막을 이유는
     * 아니다. 직전 값으로 돈다.
     */
    public Mono<Void> refresh() {
        return Mono.defer(reports)
                // **타이머도 배분 스케줄러다.** 기본 스케줄러를 쓰면 제어 평면의
                // 시간과 분리되고, 시험이 가상 시간으로 재지 못한다.
                .timeout(budget, worker)
                // **수집을 레디스 이벤트 루프에서 돌리지 않는다.** 램프 기록은
                // 동기화 없는 맵이고, 재연결로 루프가 갈리면 두 스레드가 만진다.
                .publishOn(worker)
                .doOnNext(this::collected)
                .doOnError(this::failed)
                .onErrorResume(e -> Mono.empty())
                .then();
    }

    private void collected(List<CapacityReport> read) {
        collector.collect(read, clock.get().getEpochSecond());
        failures.exited().ifPresent(recovered ->
                log.info("가용량을 다시 읽는다 — {}초 만에, 그동안 {}판 걸렀다",
                        recovered.elapsedSeconds(), recovered.swallowed()));
    }

    /**
     * <b>0건과 못 읽은 것은 다르다.</b> 빈 목록을 넘기면 하한으로 떨어져 전면
     * 억제가 되는데, 그건 관측이 아니라 왕복 실패다.
     */
    private void failed(Throwable e) {
        collector.observationFailed();
        if (failures.entered()) {
            log.warn("가용량을 못 읽는다 — 직전 값으로 배분한다: {}", e.toString());
        }
    }
}

package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/**
 * 판정 재료 갱신 루프를 켜고 끈다.
 *
 * <p><b>멈추기 전에 드레이닝을 알린다.</b> 순서가 반대면 부하 분산기가 아직
 * 보내는 동안 재료가 늙기 시작하고, 살아 있음 판정이 그걸 정지로 세어 진행 중인
 * 요청을 든 파드를 죽인다.
 */
public final class SnapshotRefreshLifecycle implements SmartLifecycle {

    /**
     * 웹 서버가 빠지기 <b>전에</b> 드레이닝을 알려야 한다.
     *
     * <p>컨테이너는 단계가 큰 것부터 멈춘다. 가장 크게 두면 종료 신호를 가장
     * 먼저 받아, 부하 분산기가 뺄 시간을 번다.
     */
    private static final int PHASE = Integer.MAX_VALUE;

    private final SnapshotRefresher refresher;
    private final ShutdownState shutdown;
    private final Duration interval;

    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Scheduler scheduler;
    private volatile Disposable subscription;

    private SnapshotRefreshLifecycle(SnapshotRefresher refresher, ShutdownState shutdown,
            Duration interval) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval 은 양수여야 한다: %s".formatted(interval));
        }
        this.refresher = Objects.requireNonNull(refresher, "refresher 는 필수다");
        this.shutdown = Objects.requireNonNull(shutdown, "shutdown 은 필수다");
        this.interval = interval;
    }

    public static SnapshotRefreshLifecycle of(SnapshotRefresher refresher, ShutdownState shutdown,
            Duration interval) {
        return new SnapshotRefreshLifecycle(refresher, shutdown, interval);
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        scheduler = SnapshotRefresher.dedicatedScheduler();
        subscription = refresher.loop(interval, scheduler).subscribe();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        // 먼저 알린다. 뒤에 알리면 그 사이 재료가 늙어 정지로 보인다.
        shutdown.draining();
        Disposable current = subscription;
        if (current != null) {
            current.dispose();
        }
        Scheduler mine = scheduler;
        if (mine != null) {
            mine.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}

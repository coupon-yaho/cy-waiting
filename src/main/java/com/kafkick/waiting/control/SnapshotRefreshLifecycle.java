package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextClosedEvent;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/**
 * 판정 재료 갱신 루프를 켜고 끈다.
 *
 * <p><b>루프가 멎기 전에 드레이닝을 알린다.</b> 순서가 반대면 부하 분산기가 아직
 * 보내는 동안 재료가 늙기 시작하고, 살아 있음 판정이 그걸 정지로 세어 진행 중인
 * 요청을 든 파드를 죽인다.
 */
public final class SnapshotRefreshLifecycle
        implements SmartLifecycle, ApplicationListener<ContextClosedEvent> {

    private static final Logger log = LoggerFactory.getLogger(SnapshotRefreshLifecycle.class);

    /**
     * 갱신 루프가 웹 서버보다 <b>먼저</b> 멎어야 한다. 뒤면 이미 안 받는 상태에서
     * 레디스를 계속 두드린다.
     *
     * <p>컨테이너는 단계가 큰 것부터 멈추므로 가장 크게 둔다. 드레이닝 자체는
     * 닫힘 사건이 알리고, 그건 어떤 단계보다도 먼저 온다.
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
        // **종료는 되돌리지 않는다.** 다시 켜면 루프는 도는데 받는 판정은 영구히
        // 거절한다 — 뜨긴 뜨는데 트래픽을 영영 못 받는 파드가 된다.
        if (shutdown.isDraining()) {
            throw new IllegalStateException("종료한 제어 평면은 다시 시작하지 않는다");
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        scheduler = SnapshotRefresher.dedicatedScheduler();
        // 오류를 소비하지 않으면 루프가 조용히 죽고 왜 죽었는지도 안 남는다.
        subscription = refresher.loop(interval, scheduler)
                .subscribe(ignored -> { },
                        e -> log.error("갱신 루프가 끊겼다 — 재기동으로 복구된다", e));
    }

    /**
     * <b>여기서만 드레이닝을 알린다.</b> 정지는 컨테이너가 잠깐 멈출 때도 불려서,
     * 거기서 알리면 다시 켤 수 없는 상태가 된다. 닫힘 사건은 생명주기 정지보다
     * 먼저 오므로 순서도 그대로다.
     */
    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        shutdown.draining();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
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

package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/**
 * <b>루프는 웹 서버보다 오래 산다.</b> 드레이닝이 끝날 때까지 재료가 신선해야
 * 진행 중인 요청이 낡은 판정을 안 받는다. 드레이닝을 알리는 것은 이 클래스가
 * 받는 {@link ContextClosedEvent} 이고, 그 사건은 어떤 정지보다 먼저 온다.
 */
@Order(Ordered.HIGHEST_PRECEDENCE)
public final class SnapshotRefreshLifecycle
        implements SmartLifecycle, ApplicationContextAware,
                ApplicationListener<ContextClosedEvent> {

    private static final Logger log = LoggerFactory.getLogger(SnapshotRefreshLifecycle.class);

    /**
     * 컨테이너는 단계가 큰 것부터 멈춘다. <b>웹 서버보다 작아야 나중에 멎는다</b> —
     * 드레이닝 동안에도 재료가 신선해야 진행 중인 요청이 낡은 판정을 안 받는다.
     */
    private static final int PHASE = WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE - 1;

    private final SnapshotRefresher refresher;
    private final ShutdownState shutdown;
    private final Duration interval;

    /**
     * 판을 도는 스케줄러를 만든다.
     *
     * <p><b>안에서 만들면 시험이 가상 시계를 못 넣는다</b> — 관용치로 흔들림을
     * 덮은 시험이 남는다 (TS-4).
     */
    private final Supplier<Scheduler> schedulers;

    private final AtomicBoolean running = new AtomicBoolean();
    private volatile ApplicationContext owner;
    private volatile Scheduler scheduler;
    private volatile Disposable subscription;

    /** 드레이닝이 시작된 단조 시각. 0 이면 정상 정지다. */
    private volatile long drainingAt;

    private SnapshotRefreshLifecycle(SnapshotRefresher refresher, ShutdownState shutdown,
            Duration interval, Supplier<Scheduler> schedulers) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval 은 양수여야 한다: %s".formatted(interval));
        }
        this.refresher = Objects.requireNonNull(refresher, "refresher 는 필수다");
        this.shutdown = Objects.requireNonNull(shutdown, "shutdown 은 필수다");
        this.interval = interval;
        this.schedulers = Objects.requireNonNull(schedulers, "schedulers 는 필수다");
    }

    public static SnapshotRefreshLifecycle of(SnapshotRefresher refresher, ShutdownState shutdown,
            Duration interval) {
        return of(refresher, shutdown, interval, SnapshotRefresher::dedicatedScheduler);
    }

    /** 스케줄러를 밖에서 준다. 시험이 가상 시계를 넣는 자리다. */
    public static SnapshotRefreshLifecycle of(SnapshotRefresher refresher, ShutdownState shutdown,
            Duration interval, Supplier<Scheduler> schedulers) {
        return new SnapshotRefreshLifecycle(refresher, shutdown, interval, schedulers);
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
        try {
            scheduler = Objects.requireNonNull(schedulers.get(), "스케줄러가 null 이다");
            // 오류를 소비하지 않으면 루프가 조용히 죽고 왜 죽었는지도 안 남는다.
            subscription = refresher.loop(interval, scheduler)
                    .subscribe(ignored -> { },
                            e -> log.error("갱신 루프가 끊겼다 — 재기동으로 복구된다", e));
        } catch (RuntimeException e) {
            // **깃발을 되돌린다.** 안 되돌리면 다음 start() 가 즉시 돌아가고,
            // 루프는 안 도는데 도는 것처럼 보이는 상태로 굳는다.
            running.set(false);
            disposeScheduler();
            throw e;
        }
    }

    /**
     * <b>여기서만 알린다.</b> 정지는 잠깐 멈출 때도 불려서, 거기서 알리면 다시
     * 켤 수 없는 상태가 된다. 맨 앞에 서는 것은 앞선 리스너가 터지면 컨테이너가
     * 경고만 찍고 넘어가서, 알림이 빠지면 살아 있음 판정이 파드를 죽이기 때문이다.
     */
    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        // 하위 컨텍스트의 닫힘도 위로 전해진다. 관리 포트를 따로 열면 하위가
        // 실제로 생기므로, 그게 닫혔다고 서비스가 종료하는 것은 아니다.
        if (event.getApplicationContext() != owner) {
            return;
        }
        // **여기서 시각을 잡는다.** 정지 로그가 드레이닝을 얼마나 버텼는지 말하려면
        // 시작점이 필요한데, 그 시작점을 아는 것은 이 사건뿐이다.
        drainingAt = System.nanoTime();
        shutdown.draining();
    }

    private void disposeScheduler() {
        Scheduler mine = scheduler;
        if (mine != null) {
            mine.dispose();
            scheduler = null;
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext context) {
        this.owner = context;
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
        disposeScheduler();
        // **드레이닝 시작과 최대 30초 벌어진다.** 진입·해제 쌍이 제 역할을 하려면
        // 그 사이를 버텼는지가 여기 남아야 한다 — 없으면 창이 다시 열려도 모른다.
        long since = drainingAt;
        if (since == 0) {
            log.info("갱신 루프 정지");
        } else {
            log.info("갱신 루프 정지 — 드레이닝 시작 뒤 {}초",
                    (System.nanoTime() - since) / 1_000_000_000L);
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

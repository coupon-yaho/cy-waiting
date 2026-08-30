package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * 제어 평면을 켜고 끈다.
 *
 * <p><b>순서가 전부다.</b> 연장을 먼저 멈추지 않으면 방금 비운 락을 죽는 노드가
 * 다시 잡고, 커넥션이 닫힌 뒤에 놓으려 하면 매번 실패한다.
 */
// **멈추는 것과 끝나는 것을 가른다.** 컨테이너는 종료가 아닐 때도 stop() 을
// 부른다. 그 자리에서 락을 놓으면 리더십이 종단 상태가 되어, 다시 시작해도
// 영영 리더가 못 된다.
public final class ControlPlaneLifecycle implements SmartLifecycle, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneLifecycle.class);

    /**
     * 컨테이너는 <b>단계가 큰 것부터</b> 멈춘다.
     *
     * <p>웹 서버가 빠진 뒤, 커넥션 팩토리(0)가 닫히기 전 — 그 사이가 락을 놓을
     * 수 있는 유일한 창이다.
     */
    private static final int PHASE = 1024;

    private final LeadershipLoop renewLoop;
    private final AllocationScheduler allocationLoop;
    private final Supplier<Mono<Void>> release;
    private final Duration releaseTimeout;
    private final Scheduler timer;

    private final AtomicBoolean running = new AtomicBoolean();

    /** 놓는 것은 한 번뿐이다. 두 번 놓으면 그 사이에 잡은 남의 락을 지운다. */
    private final AtomicBoolean released = new AtomicBoolean();

    private ControlPlaneLifecycle(LeadershipLoop renewLoop, AllocationScheduler allocationLoop,
            Supplier<Mono<Void>> release, Duration releaseTimeout, Scheduler timer) {
        if (releaseTimeout == null || releaseTimeout.isZero() || releaseTimeout.isNegative()) {
            throw new IllegalArgumentException(
                    "releaseTimeout 은 양수여야 한다: %s".formatted(releaseTimeout));
        }
        this.renewLoop = Objects.requireNonNull(renewLoop, "renewLoop 은 필수다");
        this.allocationLoop = Objects.requireNonNull(allocationLoop, "allocationLoop 은 필수다");
        this.release = Objects.requireNonNull(release, "release 는 필수다");
        this.releaseTimeout = releaseTimeout;
        this.timer = Objects.requireNonNull(timer, "timer 는 필수다");
    }

    public static ControlPlaneLifecycle of(LeadershipLoop renewLoop,
            AllocationScheduler allocationLoop, Supplier<Mono<Void>> release,
            Duration releaseTimeout, Scheduler timer) {
        return new ControlPlaneLifecycle(renewLoop, allocationLoop, release, releaseTimeout, timer);
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        renewLoop.start();
        allocationLoop.start();
        log.info("제어 평면 시작");
    }

    /**
     * <b>루프만 멈춘다.</b> 락은 안 놓는다 — 이 호출이 종료라는 보장이 없다.
     *
     * <p>컨테이너는 캐시된 컨텍스트를 전환하거나 잠시 세울 때도 여기를 부른다.
     * 그때 놓으면 리더십이 종단 상태가 되고, 다시 시작해도 리더가 못 된다.
     * 놓는 자리는 {@link #destroy()} 다.
     */
    @Override
    public void stop(Runnable callback) {
        if (!running.compareAndSet(true, false)) {
            callback.run();
            return;
        }
        // **연장을 먼저 멈춘다.** 안 그러면 방금 비운 락을 죽는 노드가 다시 잡는다.
        renewLoop.stop();
        allocationLoop.stop(() -> { });
        log.info("제어 평면 정지");
        callback.run();
    }

    /**
     * <b>여기가 정말 끝나는 자리다.</b> 컨테이너가 빈을 버릴 때 락을 놓는다.
     *
     * <p>해제가 종료를 붙들면 안 된다. 못 놓아도 리스 만료로 풀리지만, 붙들면
     * 오케스트레이터가 강제로 끊고 그때는 진행 중인 요청까지 함께 끊긴다.
     * 그래서 상한을 걸고, 실패해도 넘어간다.
     */
    @Override
    public void destroy() {
        if (!released.compareAndSet(false, true)) {
            return;
        }
        stop();
        Mono.defer(release)
                .timeout(releaseTimeout, timer)
                .doOnError(e -> log.warn("리더 해제 실패 — 리스 만료로 풀린다", e))
                .onErrorResume(e -> Mono.empty())
                .doFinally(signal -> log.info("제어 평면 해제"))
                .subscribe();
    }

    @Override
    public void stop() {
        stop(() -> { });
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

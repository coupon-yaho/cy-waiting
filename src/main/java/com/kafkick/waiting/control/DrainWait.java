package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 종료 신호를 받은 뒤 부하 분산기가 우리를 뺄 때까지 기다립니다.
 *
 * <p>readiness 를 내려도 앞단이 그것을 알아채기 전까지는 계속 보냅니다. 기다리지
 * 않고 곧바로 드레인하면 그 사이 도착한 요청이 커넥션째 끊기고, 롤링 배포마다
 * 사용자가 오류를 봅니다.
 */
public final class DrainWait {

    private static final Logger log = LoggerFactory.getLogger(DrainWait.class);

    private final ShutdownState shutdown;
    private final Duration wait;
    private final LongConsumer sleeper;

    /** 두 번 불려도 한 번만 기다립니다. 곱해지면 배포가 그만큼 느려집니다. */
    private final AtomicBoolean waited = new AtomicBoolean();

    private DrainWait(ShutdownState shutdown, Duration wait, LongConsumer sleeper) {
        this.shutdown = Objects.requireNonNull(shutdown, "shutdown 은 필수다");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper 는 필수다");
        Objects.requireNonNull(wait, "wait 는 필수다");
        // **0 이면 이 장치가 없는 것과 같습니다.** 값으로 끄면 기다림이 사라졌다는
        // 사실이 설정 어디에도 안 드러납니다.
        if (wait.isNegative() || wait.isZero()) {
            throw new IllegalArgumentException("LB 제외 대기는 양수여야 한다: " + wait);
        }
        this.wait = wait;
    }

    /** 실제로 잠듭니다. 운영 배선이 쓰는 길입니다. */
    public static DrainWait of(ShutdownState shutdown, Duration wait) {
        return new DrainWait(shutdown, wait, DrainWait::sleep);
    }

    /** 잠드는 방식을 받습니다. 실제로 자면 이 시험만 장비 속도에 걸립니다 (TS-4). */
    public static DrainWait of(ShutdownState shutdown, Duration wait, LongConsumer sleeper) {
        return new DrainWait(shutdown, wait, sleeper);
    }

    /**
     * readiness 를 내리고, 부하 분산기가 뺄 시간을 준 뒤 드레인을 실행합니다.
     *
     * <p><b>순서가 뒤집히면 안 됩니다.</b> 기다린 뒤에 내리면 그 대기 시간 동안
     * 앞단은 우리가 멀쩡하다고 보고 계속 보냅니다.
     *
     * <p>드레인이 실패해도 삼킵니다. 여기서 막히면 노드가 안 죽고 배포가 멈춥니다.
     */
    public void beforeDrain(Runnable drain) {
        Objects.requireNonNull(drain, "drain 은 필수다");
        shutdown.draining();
        if (waited.compareAndSet(false, true)) {
            log.info("종료 시작 — readiness 를 내리고 {}초 동안 부하 분산기를 기다린다",
                    wait.toSeconds());
            try {
                sleeper.accept(wait.toMillis());
            } catch (RuntimeException e) {
                // 끊겼다고 안 죽으면 안 됩니다. 그 사실만 남기고 계속합니다.
                log.warn("부하 분산기 대기가 끊겼다 — 그대로 드레인한다: {}", e.toString());
            }
        }
        try {
            drain.run();
        } catch (RuntimeException e) {
            log.warn("드레인 준비가 실패했다 — 종료는 계속한다: {}", e.toString());
        }
    }

    // RULE-EXCEPTION(JS-13): 기본 잠듦 구현이라 인스턴스 상태가 없다. 인스턴스
    // 메서드로 두면 생성자에서 자기 자신을 참조해야 한다.
    private static void sleep(long millis) {
        try {
            // RULE-EXCEPTION(RX-1): 종료 경로다. 여기서 막는 것이 목적이고, 상한은
            // 생성자가 양수로 강제하며 spring.lifecycle.timeout 이 다시 덮는다.
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("대기가 끊겼다", e);
        }
    }
}

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
 * <p>readiness 를 내려도 앞단이 알아채기 전까지는 계속 보냅니다. 곧바로 드레인하면
 * 그 사이 도착한 요청이 커넥션째 끊겨, 롤링 배포마다 사용자가 오류를 봅니다.
 */
public final class DrainWait {

    private static final Logger log = LoggerFactory.getLogger(DrainWait.class);

    /**
     * 대기 상한. {@code spring.lifecycle.timeout-per-shutdown-phase} 와 같은 값이다.
     *
     * <p>이 대기는 컨테이너의 단계별 상한 <b>밖</b>이라 프레임워크가 못 끊는다.
     * 여기서 안 막으면 {@code 6s} 를 {@code 6m} 로 적은 오타 하나가 노드를 기동
     * 상태로 붙들고, 오케스트레이터가 진행 중인 요청째 강제 종료한다.
     */
    private static final Duration MAX_WAIT = Duration.ofSeconds(30);

    private final ShutdownState shutdown;
    private final Duration wait;
    private final LongConsumer sleeper;

    /** 두 번 불려도 한 번만 기다립니다. 곱해지면 배포가 그만큼 느려집니다. */
    private final AtomicBoolean waited = new AtomicBoolean();

    private DrainWait(ShutdownState shutdown, Duration wait, LongConsumer sleeper) {
        this.shutdown = Objects.requireNonNull(shutdown, "shutdown 은 필수다");
        // 안 주면 실제로 잠듭니다. 생성자에서 정하므로 필드는 늘 채워집니다.
        this.sleeper = sleeper == null ? this::sleep : sleeper;
        Objects.requireNonNull(wait, "wait 는 필수다");
        // **0 이면 이 장치가 없는 것과 같습니다.** 값으로 끄면 기다림이 사라졌다는
        // 사실이 설정 어디에도 안 드러납니다.
        if (wait.isNegative() || wait.isZero()) {
            throw new IllegalArgumentException("LB 제외 대기는 양수여야 한다: " + wait);
        }
        // **상한을 먼저 봅니다.** `toMillis()` 는 넘치면 `ArithmeticException` 을
        // 던지므로, 뒤에 두면 아주 큰 값이 검증을 통째로 우회합니다.
        if (wait.compareTo(MAX_WAIT) > 0) {
            throw new IllegalArgumentException(
                    "LB 제외 대기는 " + MAX_WAIT + " 이하여야 한다: " + wait);
        }
        // **밀리초 미만은 0 으로 잘립니다.** `PT0.0005S` 는 양수 검사를 통과하지만
        // 실제로는 안 기다리고, 기다림이 사라진 사실이 어디에도 안 드러납니다.
        if (wait.toMillis() < 1) {
            throw new IllegalArgumentException("LB 제외 대기는 1ms 이상이어야 한다: " + wait);
        }
        this.wait = wait;
    }

    /** 실제로 잠듭니다. 운영 배선이 쓰는 길입니다. */
    public static DrainWait of(ShutdownState shutdown, Duration wait) {
        return new DrainWait(shutdown, wait, null);
    }

    /** 잠드는 방식을 받습니다. 실제로 자면 이 시험만 장비 속도에 걸립니다 (TS-4). */
    public static DrainWait of(ShutdownState shutdown, Duration wait, LongConsumer sleeper) {
        return new DrainWait(shutdown, wait, sleeper);
    }

    /**
     * readiness 를 내리고, 부하 분산기가 뺄 시간을 준 뒤 돌아옵니다.
     *
     * <p><b>순서가 뒤집히면 안 됩니다.</b> 기다린 뒤에 내리면 그 대기 시간 동안
     * 앞단은 우리가 멀쩡하다고 보고 계속 보냅니다.
     *
     * <p>드레인 자체는 이 뒤에 컨테이너가 합니다 — 부르는 쪽이 돌아가야 시작됩니다.
     */
    public void beforeDrain() {
        shutdown.draining();
        if (!waited.compareAndSet(false, true)) {
            return;
        }
        log.info("종료 시작 — readiness 를 내리고 {}초 동안 부하 분산기를 기다린다",
                wait.toSeconds());
        long startedAt = System.nanoTime();
        try {
            sleeper.accept(wait.toMillis());
        } catch (RuntimeException e) {
            // 끊겼다고 안 죽으면 안 됩니다. 그 사실만 남기고 계속합니다.
            log.warn("부하 분산기 대기가 끊겼다 — 덜 기다린 채로 드레인한다", e);
        }
        // 진입만 남기면 얼마나 버텼는지를 사후에 못 답한다 (LG-2).
        log.info("부하 분산기 대기 끝 — {}ms 기다렸다. 드레인을 시작한다",
                (System.nanoTime() - startedAt) / 1_000_000L);
    }

    private void sleep(long millis) {
        try {
            // RULE-EXCEPTION(RX-1): 종료 경로다. 여기서 막는 것이 목적이고, 상한은
            // 생성자가 MAX_WAIT 이하의 양수로 강제한다.
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // **끊긴 표시를 다시 세우지 않는다.** 이 스레드는 곧바로 컨테이너의
            // 단계별 정지로 들어가 `latch.await` 로 드레인을 기다리는데, 표시가
            // 서 있으면 그 기다림이 즉시 깨진다 — 지켜 주려던 진행 중인 요청이
            // 바로 그때 끊긴다. 끊겼다는 사실은 부르는 쪽이 로그로 남긴다.
            throw new IllegalStateException("대기가 끊겼다", e);
        }
    }
}

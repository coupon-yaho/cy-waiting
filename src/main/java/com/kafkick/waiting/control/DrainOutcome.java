package com.kafkick.waiting.control;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 드레인이 <b>상한 안에 끝났는지</b>를 남깁니다 (6.4.2).
 *
 * <p>안 남기면 오케스트레이터가 진행 중인 요청째 죽인 것과 곱게 빠진 것이 로그에서
 * 같습니다. 롤링 배포마다 사용자가 오류를 보는데 그 사실이 어디에도 안 드러납니다.
 */
public final class DrainOutcome {

    private static final Logger log = LoggerFactory.getLogger(DrainOutcome.class);

    /** 얼마나 자주 볼 것인가. 촘촘하면 다 빠진 뒤 붙들고 있는 시간이 짧아집니다. */
    private static final long TICK_MILLIS = 50;

    private final IntSupplier inFlight;
    private final Duration limit;
    private final LongConsumer sleeper;
    private final Consumer<String> recorder;

    /** 기다림이 끊겼는가. 끊겼으면 상한까지 안 기다린 것이라 그 사실을 같이 남긴다. */
    private volatile boolean interrupted;

    private DrainOutcome(IntSupplier inFlight, Duration limit, LongConsumer sleeper,
            Consumer<String> recorder) {
        this.inFlight = Objects.requireNonNull(inFlight, "inFlight 는 필수다");
        Objects.requireNonNull(limit, "limit 는 필수다");
        // **0 이면 지켜보는 것이 아닙니다.** 값으로 끄면 그 사실이 설정 어디에도
        // 안 드러나고, 로그가 조용한 것이 곧 정상으로 읽힙니다.
        if (limit.isNegative() || limit.isZero()) {
            throw new IllegalArgumentException("드레인 상한은 양수여야 한다: " + limit);
        }
        this.limit = limit;
        this.sleeper = sleeper == null ? this::sleep : sleeper;
        this.recorder = recorder == null ? log::warn : recorder;
    }

    public static DrainOutcome of(IntSupplier inFlight, Duration limit) {
        return new DrainOutcome(inFlight, limit, null, null);
    }

    /** 잠드는 방식과 남기는 곳을 받습니다. 실제로 자면 시험이 장비 속도에 걸립니다 (TS-4). */
    public static DrainOutcome of(IntSupplier inFlight, Duration limit, LongConsumer sleeper,
            Consumer<String> recorder) {
        return new DrainOutcome(inFlight, limit, sleeper, recorder);
    }

    /**
     * 다 빠질 때까지, 또는 상한까지 기다립니다.
     *
     * @return 상한 안에 다 빠졌으면 참
     */
    public boolean await() {
        long deadline = System.nanoTime() + limit.toNanos();
        int left = inFlight.getAsInt();
        while (left > 0 && !interrupted && System.nanoTime() < deadline) {
            sleeper.accept(TICK_MILLIS);
            left = inFlight.getAsInt();
        }
        if (left > 0) {
            // **건수를 같이 남깁니다.** 그 숫자가 곧 강제 종료로 끊길 요청 수이고,
            // 없으면 롤링 배포의 오류가 이것 때문인지 못 가립니다.
            recorder.accept((interrupted
                    ? "드레인 대기가 끊겼다 — %d건이 남았다. ".formatted(left)
                    : "드레인 상한 초과 — %d초를 기다렸는데 %d건이 남았다. "
                            .formatted(limit.toSeconds(), left))
                    + "오케스트레이터가 이 요청들을 끊는다");
            return false;
        }
        recorder.accept("드레인 끝 — 진행 중인 요청이 다 빠졌다");
        return true;
    }

    /**
     * 한 회차만 잔다.
     *
     * <p><b>끊긴 표시를 다시 세우지 않고 던지지도 않는다.</b> 이 스레드는 곧바로
     * 컨테이너의 단계별 정지로 들어가 드레인을 기다리는데, 표시가 서 있으면 그
     * 기다림이 즉시 깨진다 — 지켜 주려던 진행 중인 요청이 바로 그때 끊긴다.
     * 던지면 종료 리스너가 통째로 중단돼 같은 일이 난다.
     */
    private void sleep(long millis) {
        try {
            // RULE-EXCEPTION(RX-1): 종료 경로다. 여기서 막는 것이 목적이고,
            // 상한은 생성자가 양수로 강제한다.
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // 덜 기다린 채로 돌아간다. 남은 건수는 부르는 쪽이 그대로 센다.
            interrupted = true;
        }
    }
}

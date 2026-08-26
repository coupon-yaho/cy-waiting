package com.kafkick.waiting.adapter.redis;

import com.kafkick.waiting.control.FailureWindow;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 레디스 서버 시각을 <b>뒤로 안 가게</b> 받는다 (A-9).
 *
 * <p>뒤로 간 값을 기준으로 삼으면 뒷단 보고가 전부 미래가 되어 한꺼번에 낡음이
 * 되고, 시계가 따라잡을 때까지 크레딧이 하한에 박힌다.
 */
public final class ServerClock {

    private static final Logger log = LoggerFactory.getLogger(ServerClock.class);

    /** 2026-01-01. 이보다 이르면 시계가 안 선 값이다. */
    private static final long EARLIEST_SANE = 1_767_225_600L;

    /**
     * 2100-01-01. 이보다 늦으면 초가 아니라 밀리초다.
     *
     * <p><b>위쪽이 더 위험하다.</b> 밀리초가 바닥값에 한 번 들어가면 그 뒤 정상
     * 시각이 전부 그 값으로 보정돼 크레딧이 영영 하한이다.
     */
    private static final long LATEST_SANE = 4_102_444_800L;

    private final AtomicLong floor = new AtomicLong();
    private final ClockSkewTracker skew = ClockSkewTracker.create();
    private final FailureWindow wentBack = FailureWindow.create();

    private ServerClock() {
    }

    public static ServerClock create() {
        return new ServerClock();
    }

    /**
     * 관측한 서버 시각을 바닥값에 걸러 돌려준다.
     *
     * @throws IllegalStateException 초로 볼 수 없는 값일 때
     */
    public long observe(long seconds) {
        if (seconds < EARLIEST_SANE || seconds > LATEST_SANE) {
            throw new IllegalStateException("서버 시각이 초로 안 보인다: " + seconds);
        }
        long floored = floor.accumulateAndGet(seconds, Math::max);
        boolean applied = floored > seconds;
        skew.record(applied, (floored - seconds) * 1_000_000L);
        if (applied) {
            if (wentBack.entered()) {
                log.warn("레디스 시각이 뒤로 갔다 — 바닥값으로 돈다: {}초 역행", floored - seconds);
            }
        } else {
            wentBack.exited().ifPresent(recovered ->
                    log.info("레디스 시각이 다시 앞선다 — {}초 만에, 그동안 {}번 보정했다",
                            recovered.elapsedSeconds(), recovered.swallowed()));
        }
        return floored;
    }

    /** 조용히 보정하면 <b>왜 그랬는지를 영영 못 밝힌다.</b> 횟수와 폭을 남긴다. */
    public ClockSkewTracker skew() {
        return skew;
    }
}

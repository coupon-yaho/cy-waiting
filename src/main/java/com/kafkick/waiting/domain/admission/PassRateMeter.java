package com.kafkick.waiting.domain.admission;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * 이 노드가 뒷단으로 넘긴 초당 요청 수. 회복 봉우리를 정상과 견주려면 그 수를
 * 알아야 하는데 노드는 제 것만 안다 (RC4). <b>지금은 지표로만 낸다</b> — 이 값으로
 * 자르면 관측이 제 출력에 오염돼 진동한다 (AIJ-0250).
 */
public final class PassRateMeter {

    /** 기본 창(ms). 배분 틱보다 넉넉해 한 틱의 흔들림에 안 눕는다. */
    public static final long DEFAULT_WINDOW_MS = 5_000;

    /** 창의 상한. 넘기면 {@code windowMs * 2} 가 넘쳐 음수가 되고 값이 영영 0 이다. */
    private static final long MAX_WINDOW_MS = 3_600_000;

    /** 창을 안 연 상태. 첫 통과가 여기서 시작을 찍는다. */
    private static final long IDLE = Long.MIN_VALUE;

    private final long windowMs;

    /** 초당으로 나눌 때의 최소 구간. 갓 연 창의 몇 건이 부풀지 않게 한다. */
    private final long minSpanMs;

    /**
     * 창 하나. <b>수를 {@link LongAdder} 로 든다</b> — 요청마다 CAS 를 돌면 노드
     * 하나의 캐시 라인에 피크 부하가 통째로 몰린다 (R4).
     *
     * @param previous 직전 창이 낸 값. 한 창도 안 채웠으면 음수다
     */
    private record Window(long startedAt, LongAdder count, long previous) {
    }

    private final AtomicReference<Window> window =
            new AtomicReference<>(new Window(IDLE, new LongAdder(), -1));

    private PassRateMeter(long windowMs) {
        if (windowMs <= 0 || windowMs > MAX_WINDOW_MS) {
            throw new IllegalArgumentException(
                    "windowMs 는 1.." + MAX_WINDOW_MS + " 여야 한다: " + windowMs);
        }
        this.windowMs = windowMs;
        this.minSpanMs = Math.max(1, windowMs / 5);
    }

    public static PassRateMeter of(long windowMs) {
        return new PassRateMeter(windowMs);
    }

    /**
     * 한 건이 뒷단으로 갔다. <b>접을 때만 CAS 를 돈다</b> — 창 경계에 겹친 몇 건은
     * 어느 창에도 안 들어갈 수 있지만, 초당 수에는 그 오차가 안 보인다.
     */
    public void passed(long nowMs) {
        Window w = window.get();
        if (rolls(w, nowMs)) {
            w = window.updateAndGet(now -> advance(now, nowMs));
        }
        w.count().increment();
    }

    /** 이 도장이 창을 접는가. 대부분의 회차가 여기서 끝나 CAS 를 안 돈다. */
    private boolean rolls(Window w, long nowMs) {
        if (w.startedAt() == IDLE) {
            return true;
        }
        long elapsed = nowMs - w.startedAt();
        return elapsed > windowMs || elapsed < -windowMs;
    }

    /**
     * 창이 다 찼으면 접고 새로 연다. <b>읽는 쪽이 아니라 여기서 접는다</b> —
     * 읽기가 상태를 바꾸면 묻는 주기에 따라 값이 달라진다.
     */
    private Window advance(Window w, long nowMs) {
        if (!rolls(w, nowMs)) {
            return w;
        }
        if (w.startedAt() == IDLE) {
            return new Window(nowMs, new LongAdder(), w.previous());
        }
        long elapsed = nowMs - w.startedAt();
        // **유휴를 건너뛴 창은 직전 값이 아니다.** 창이 두 배를 넘도록 열려
        // 있었다는 것은 그사이 한 건도 안 왔다는 뜻이다.
        long carried = elapsed > windowMs * 2 ? -1 : perWindow(w.count().sum());
        return new Window(nowMs, new LongAdder(), carried);
    }

    /**
     * 창 안의 초당 수. <b>읽어도 상태를 안 바꾼다</b> — 묻는 주기에 따라 값이
     * 달라지면 상한이 그만큼 흔들린다.
     */
    public long perSecond(long nowMs) {
        Window w = window.get();
        if (w.startedAt() == IDLE) {
            return Math.max(0, w.previous());
        }
        long elapsed = nowMs - w.startedAt();
        // **한 창이 통째로 비면 0 이다.** 창은 열린 뒤 창 길이만큼만 통과를
        // 받으므로, 그 두 배가 지났다는 것은 최소 한 창을 쉬었다는 뜻이다.
        if (elapsed >= windowMs * 2) {
            return 0;
        }
        if (w.previous() >= 0) {
            return w.previous();
        }
        // **첫 창은 실제 구간으로 나누되 바닥을 둔다.** 창 길이로 나누면 회복
        // 첫 초의 봉우리가 5분의 1로 눌리고, 실제 구간만 쓰면 5ms 에 지나간 세
        // 건이 600건/s 가 된다.
        return perSpan(w.count().sum(), Math.max(elapsed, minSpanMs));
    }

    /** 창 하나의 초당 수. 접힌 창은 창 길이만큼 열려 있었다. */
    private long perWindow(long count) {
        return perSpan(count, windowMs);
    }

    /**
     * <b>0 으로 반올림하지 않는다</b> — 창이 길면 한두 건이 "부하 없음" 이 된다.
     * 창은 통과가 여므로 수가 0 일 수 없고, 유휴는 읽는 쪽이 먼저 0 으로 끊는다.
     */
    private long perSpan(long count, long spanMs) {
        return Math.max(1, Math.round(count * 1000.0 / spanMs));
    }
}

package com.kafkick.waiting.domain.admission;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 이 노드가 뒷단으로 보낸 초당 요청 수.
 *
 * <p>회복 봉우리를 정상과 견주려면 그 수를 알아야 하는데, 노드는 제 것만 안다
 * (RC4). <b>지금은 지표로만 낸다</b> — 이 값으로 자르면 관측이 제 출력에 오염돼
 * 진동한다 (AIJ-0250).
 */
public final class PassRateMeter {

    /** 기본 창(ms). 배분 틱보다 넉넉해 한 틱의 흔들림에 안 눕는다. */
    public static final long DEFAULT_WINDOW_MS = 5_000;

    /** 창의 상한. 넘기면 {@code windowMs * 2} 가 넘쳐 음수가 되고 값이 영영 0 이다. */
    private static final long MAX_WINDOW_MS = 3_600_000;

    /** 창을 안 연 상태. 첫 통과가 여기서 시작을 찍는다. */
    private static final long IDLE = Long.MIN_VALUE;

    private final long windowMs;

    /**
     * 창 하나. <b>셋을 한 덩어리로 바꾼다</b> — 나눠 두면 읽는 쪽이 새 시작과
     * 0 으로 리셋된 수를 짝지어 부하 중에 0 을 낸다.
     *
     * @param previous 직전 창이 낸 값. 한 창도 안 채웠으면 음수다
     */
    private record Window(long startedAt, long count, long previous) {
    }

    private final AtomicReference<Window> window =
            new AtomicReference<>(new Window(IDLE, 0, -1));

    private PassRateMeter(long windowMs) {
        if (windowMs <= 0 || windowMs > MAX_WINDOW_MS) {
            throw new IllegalArgumentException(
                    "windowMs 는 1.." + MAX_WINDOW_MS + " 여야 한다: " + windowMs);
        }
        this.windowMs = windowMs;
    }

    public static PassRateMeter of(long windowMs) {
        return new PassRateMeter(windowMs);
    }

    /** 한 건이 뒷단으로 갔다. */
    public void passed(long nowMs) {
        window.updateAndGet(w -> {
            Window open = advance(w, nowMs);
            return new Window(open.startedAt(), open.count() + 1, open.previous());
        });
    }

    /**
     * 창이 다 찼으면 접고 새로 연다. <b>읽는 쪽이 아니라 여기서 접는다</b> —
     * 읽기가 상태를 바꾸면 묻는 주기에 따라 값이 달라진다.
     */
    private Window advance(Window w, long nowMs) {
        if (w.startedAt() == IDLE) {
            return new Window(nowMs, 0, w.previous());
        }
        long elapsed = nowMs - w.startedAt();
        if (elapsed <= windowMs) {
            // **작은 역행은 시계가 아니라 도장 순서다.** 스레드마다 제 시계를
            // 읽으므로 1ms 뒤진 도장이 흔하다. 그것으로 창을 버리면 한 건이
            // 초당 수를 0 으로 떨어뜨리고 창 하나를 다 채워야 돌아온다.
            return elapsed >= -windowMs ? w : new Window(nowMs, 0, -1);
        }
        // **유휴를 건너뛴 창은 직전 값이 아니다.** 창이 두 배를 넘도록 열려
        // 있었다는 것은 그사이 한 건도 안 왔다는 뜻이다 — 그 값을 물려주면
        // 세일이 다시 열릴 때까지 지난 봉우리를 보고한다.
        return new Window(nowMs, 0, elapsed <= windowMs * 2 ? perWindow(w.count()) : -1);
    }

    /**
     * 창 안의 초당 수. <b>읽어도 상태를 안 바꾼다</b> — 묻는 주기에 따라 값이
     * 달라지면 상한이 그만큼 흔들린다.
     */
    public long perSecond(long nowMs) {
        Window w = window.get();
        long previous = Math.max(0, w.previous());
        if (w.startedAt() == IDLE) {
            return previous;
        }
        long elapsed = nowMs - w.startedAt();
        // **한 창이 통째로 비면 0 이다.** 창은 열린 뒤 창 길이만큼만 통과를
        // 받으므로, 그 두 배가 지났다는 것은 최소 한 창을 쉬었다는 뜻이다.
        if (elapsed >= windowMs * 2) {
            return 0;
        }
        // **분모는 언제나 창 길이다.** 실제 구간으로 나누면 접을 때와 읽을 때가
        // 갈리고, 갓 연 창의 몇 건이 초당으로 부풀어 그 값이 상한이 된다.
        return w.previous() < 0 ? perWindow(w.count()) : previous;
    }

    /**
     * 창 하나의 초당 수. <b>0 으로 반올림하지 않는다</b> — 창이 길면 한두 건이
     * 0 이 되어 "부하 없음" 으로 읽힌다. 창은 통과가 열므로 수가 0 일 수 없다.
     */
    private long perWindow(long count) {
        return Math.max(1, Math.round(count * 1000.0 / windowMs));
    }
}

package com.kafkick.waiting.domain.queue;

import java.time.Duration;
import java.util.function.DoubleSupplier;

/**
 * 오류 경로의 재시도 안내. 장애 중 503 을 받은 대기자는 <b>전원이 같은 초에
 * 오류를 받는다</b> — 같은 값을 주면 전원이 같은 초에 돌아오고, 그 파도가 회복을
 * 2차 장애로 만든다.
 */
public final class ErrorBackoff {

    /**
     * 첫 실패의 간격(초). <b>바닥이 없을 때만 보이는 값이다.</b> 조회 경로는 폴링
     * 예산의 바닥(30초)을 함께 넘겨, 다섯째 계단(2·2^4 = 32)부터 바닥을 넘어선다.
     *
     * @see #retryAfterSec(int, long, DoubleSupplier)
     */
    public static final long BASE_SEC = 2;

    /**
     * 백오프 상한(초). <b>없으면 장애가 끝난 뒤의 장애가 된다.</b> 안내가 무한히
     * 멀어지면 뒷단이 회복해도 한참 아무도 안 돌아온다. 폴링 상한과 같은 자리다.
     */
    public static final long MAX_SEC = 60;

    /**
     * 흔들림 폭. <b>정상 경로보다 넓다.</b> 정상 경로는 사람마다 폴링 시점이 이미
     * 흩어져 있지만, 오류 시점은 전원이 같아 같은 폭으로는 안 흩어진다.
     */
    public static final double JITTER_RATIO = 0.5;

    /**
     * 연속 실패를 몇 번까지 배로 늘릴지. 상한이 있으니 그 위는 계산할 필요가 없다.
     * <b>지수가 넘치는 것을 막는 자리이기도 하다</b> — 실패 수는 밖에서 온다.
     */
    private static final int MAX_DOUBLINGS = 16;

    /**
     * 흔들 자리로 남겨 두는 초. <b>두 자리에서 다른 일을 한다</b> — 바닥이 상한에
     * 붙는 것을 막고(그러면 흔들 자리가 없다), 좁은 비율에서 폭이 반올림에 먹히는
     * 것을 막는다. 근거와 도달 조건은 AIJ-0271 에 있다.
     */
    private static final long MIN_SPREAD_SEC = 1;

    private final long baseSec;
    private final long maxSec;
    private final double jitterRatio;

    private ErrorBackoff(long baseSec, long maxSec, double jitterRatio) {
        this.baseSec = baseSec;
        this.maxSec = maxSec;
        this.jitterRatio = jitterRatio;
    }

    /**
     * 백오프를 푸는 데 필요한 무실패 시간. 한 계단 폭과 같이 둔다 — 한 계단 오르는
     * 만큼은 조용해야 푼다는 뜻이고, 그래야 부분 장애에서 오르내림이 대칭이 된다.
     */
    public static Duration quiet() {
        return step();
    }

    /** 한 계단의 폭. 실패가 이 시간만큼 이어질 때마다 한 계단 멀어진다. */
    public static Duration step() {
        return Duration.ofSeconds(BASE_SEC);
    }

    public static ErrorBackoff defaults() {
        return of(BASE_SEC, MAX_SEC, JITTER_RATIO);
    }

    /**
     * @param baseSec     첫 실패의 간격. 1 이상이어야 한다
     * @param maxSec      상한. 기본 간격 이상이어야 한다
     * @param jitterRatio 흔들림 폭. 0 이상 1 이하다 — 1 을 넘으면 음수가 나온다
     */
    public static ErrorBackoff of(long baseSec, long maxSec, double jitterRatio) {
        if (baseSec < 1) {
            throw new IllegalArgumentException("기본 간격은 1 이상이어야 한다: %d".formatted(baseSec));
        }
        if (maxSec < baseSec) {
            throw new IllegalArgumentException(
                    "상한은 기본 간격 이상이어야 한다: %d < %d".formatted(maxSec, baseSec));
        }
        if (!Double.isFinite(jitterRatio) || jitterRatio < 0 || jitterRatio > 1) {
            throw new IllegalArgumentException(
                    "흔들림 폭은 0 이상 1 이하여야 한다: %s".formatted(jitterRatio));
        }
        return new ErrorBackoff(baseSec, maxSec, jitterRatio);
    }

    /**
     * 다시 와도 되는 때.
     *
     * @param consecutiveFailures 이 노드가 연이어 실패한 횟수. 1 미만은 첫 실패로 본다
     * @param random              [0, 1) 난수
     */
    public long retryAfterSec(int consecutiveFailures, DoubleSupplier random) {
        return retryAfterSec(consecutiveFailures, 0, random);
    }

    /**
     * 바닥을 함께 받는다.
     *
     * @param floorSec 이보다 빨리 안 부른다. 폴링 예산이 정한 값이다 — 무시하면
     *                 예산이 빠듯한 장애 구간에 거절받은 사람만 예산 밖으로 온다.
     *                 <b>상한에 닿으면 흔들 자리만큼 물러선다</b> (AIJ-0271)
     */
    public long retryAfterSec(int consecutiveFailures, long floorSec, DoubleSupplier random) {
        int streak = Math.min(Math.max(consecutiveFailures, 1), MAX_DOUBLINGS);
        // **천장은 상한이 아니라 상한/(1+폭) 이다.** 상한으로 자른 뒤 흔들면 위로
        // 흩어진 값이 상한 한 점에 모여 흔들림이 0 이 된다 — 장애가 길어진 구간에서
        // 이 백오프가 막으려던 파도가 그대로 다시 생긴다.
        long ceiling = Math.round(maxSec / (1 + jitterRatio));
        // **시프트로 안 키운다.** 기본 간격이 크면 열여섯 번 미만에도 넘쳐
        // 음수가 되고, 그러면 상한을 씌우기 전에 값이 이미 뒤집힌다. 천장에
        // 닿으면 멈추는 곱셈이 넘칠 수 없다.
        long grown = baseSec;
        for (int i = 1; i < streak && grown < ceiling; i++) {
            grown = grown > ceiling / 2 ? ceiling : grown * 2;
        }
        // **바닥은 천장보다 세다.** 다만 상한에 붙이면 흔들 자리가 없다. 비율이
        // 0 이면 안 흔드므로 물러설 이유도 없다. 근거는 AIJ-0271.
        long room = jitterRatio == 0 ? maxSec
                : Math.max(baseSec, maxSec - MIN_SPREAD_SEC);
        long base = Math.clamp(Math.max(grown, floorSec), 1, room);
        // **남은 여유만큼 편다.** 위 끝을 자르면 잘린 만큼이 상한 한 점에 쌓인다.
        // 좁은 비율에서는 반올림이 폭을 통째로 먹으므로 초 단위 바닥을 같이 둔다.
        long top = jitterRatio == 0 ? base
                : Math.min(Math.max(Math.round(base * (1 + jitterRatio)),
                        base + MIN_SPREAD_SEC), maxSec);
        // **위로만 흔든다.** 아래로 흔들면 바닥보다 빨리 부르게 되어 장애
        // 구간에만 폴링 예산이 안 걸린다.
        double jittered = base + (top - base) * random.getAsDouble();
        // 0 은 안 준다. 즉시 재시도는 흩어짐이 없어 파도를 그대로 되돌린다.
        return Math.clamp(Math.round(jittered), 1, maxSec);
    }
}

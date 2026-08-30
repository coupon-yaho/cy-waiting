package com.kafkick.waiting.domain.queue;

import java.util.function.DoubleSupplier;

/**
 * 오류 경로의 재시도 안내 (F7).
 *
 * <p>장애 중 503 을 받은 대기자는 <b>전원이 같은 초에 오류를 받는다.</b> 같은
 * 값을 주면 전원이 같은 초에 돌아오고, 그 파도가 회복을 2차 장애로 만든다.
 */
public final class ErrorBackoff {

    /**
     * 첫 실패의 간격(초).
     *
     * <p>정상 경로의 가장 짧은 밴드보다 넓게 잡는다. 오류를 받은 사람은 아직
     * 자기 자리를 모르므로, 짧게 부르면 알 것도 없이 다시 온다.
     */
    public static final long BASE_SEC = 2;

    /**
     * 백오프 상한(초).
     *
     * <p><b>없으면 장애가 끝난 뒤의 장애가 된다.</b> 안내가 무한히 멀어지면
     * 뒷단이 회복해도 한참 아무도 안 돌아온다. 폴링 상한과 같은 자리에 둔다.
     */
    public static final long MAX_SEC = 60;

    /**
     * 흔들림 폭. <b>정상 경로보다 넓다.</b>
     *
     * <p>정상 경로는 사람마다 폴링 시점이 이미 흩어져 있어서 좁은 폭으로도
     * 흩어진다. 오류 시점은 전원이 같으므로 같은 폭으로는 안 흩어진다.
     */
    public static final double JITTER_RATIO = 0.5;

    /**
     * 연속 실패를 몇 번까지 배로 늘릴지.
     *
     * <p>상한이 있으니 그 위는 계산할 필요가 없다. <b>지수가 넘치는 것을 막는
     * 자리이기도 하다</b> — 연속 실패 수는 밖에서 오므로 크게 들어올 수 있다.
     */
    private static final int MAX_DOUBLINGS = 16;

    private final long baseSec;
    private final long maxSec;
    private final double jitterRatio;

    private ErrorBackoff(long baseSec, long maxSec, double jitterRatio) {
        this.baseSec = baseSec;
        this.maxSec = maxSec;
        this.jitterRatio = jitterRatio;
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
        int streak = Math.min(Math.max(consecutiveFailures, 1), MAX_DOUBLINGS);
        // **상한을 먼저 씌운다.** 곱한 뒤에 흔들면 상한을 넘는 값이 나가고,
        // 그러면 회복한 뒤에도 그만큼 아무도 안 돌아온다.
        long base = Math.min(baseSec << (streak - 1), maxSec);
        double jittered = base * (1 + jitterRatio * (2 * random.getAsDouble() - 1));
        // 0 은 안 준다. 즉시 재시도는 흩어짐이 없어 파도를 그대로 되돌린다.
        return Math.clamp(Math.round(jittered), 1, maxSec);
    }
}

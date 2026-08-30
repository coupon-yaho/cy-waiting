package com.kafkick.waiting.domain.queue;

import java.time.Duration;
import java.util.function.DoubleSupplier;

/**
 * 오류 경로의 재시도 안내 (F7).
 *
 * <p>장애 중 503 을 받은 대기자는 <b>전원이 같은 초에 오류를 받는다.</b>
 */
// 같은 값을 주면 전원이 같은 초에 돌아오고, 그 파도가 회복을 2차 장애로 만든다.
public final class ErrorBackoff {

    /**
     * 첫 실패의 간격(초). <b>바닥이 없을 때만 보이는 값이다.</b>
     *
     * @see #retryAfterSec(int, long, DoubleSupplier)
     */
    // 조회 경로는 폴링 예산이 정한 바닥(ETA 미상 밴드 = 30초)을 함께 넘기므로,
    // 실제로는 다섯째 계단(2·2^4 = 32)부터 이 값이 바닥을 넘어선다.
    // 그 앞의 네 계단은 바닥에 가려 안 보인다 — 바닥이 이미 그만큼 멀기
    // 때문이고, 거기서 더 당길 이유가 없다.
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

    /**
     * 백오프를 푸는 데 필요한 무실패 시간.
     *
     * <p>한 계단 폭과 같이 둔다. 한 계단 오르는 데 걸리는 만큼은 조용해야
     * 푼다는 뜻이고, 그래야 부분 장애에서 오르내림이 대칭이 된다.
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
     * @param floorSec 이보다 빨리 부르지 않는다. 폴링 예산이 정한 값이다 —
     *                 장애 구간이 곧 예산이 빠듯한 구간이라, 무시하면 하필
     *                 그때 거절받은 사람만 예산 밖으로 돌아온다
     */
    public long retryAfterSec(int consecutiveFailures, long floorSec, DoubleSupplier random) {
        int streak = Math.min(Math.max(consecutiveFailures, 1), MAX_DOUBLINGS);
        // **천장은 상한이 아니라 상한/(1+폭) 이다.** 상한으로 자른 뒤 흔들면
        // 위로 흩어진 값이 전부 상한 한 점에 모여 그 구간의 흔들림이 0 이 된다.
        // 하필 그 구간이 장애가 길어진 때라, F7 이 막으려던 파도가 거기서 그대로
        // 다시 생긴다. 정상 경로가 이미 같은 방식으로 푼 문제다.
        long ceiling = Math.round(maxSec / (1 + jitterRatio));
        // **시프트로 안 키운다.** 기본 간격이 크면 열여섯 번 미만에도 넘쳐
        // 음수가 되고, 그러면 상한을 씌우기 전에 값이 이미 뒤집힌다. 천장에
        // 닿으면 멈추는 곱셈이 넘칠 수 없다.
        long grown = baseSec;
        for (int i = 1; i < streak && grown < ceiling; i++) {
            grown = grown > ceiling / 2 ? ceiling : grown * 2;
        }
        // **바닥은 천장보다 세다.** 바닥은 폴링 예산이 정한 최소 간격이라, 그보다
        // 빨리 부르면 장애 구간에만 예산이 안 걸린다. 상한까지만 따른다.
        long base = Math.clamp(Math.max(grown, floorSec), 1, maxSec);
        // **남은 여유만큼 편다.** 위 끝을 상한으로 자르면 잘린 만큼이 상한 한
        // 점에 쌓인다. 자르는 대신 폭을 줄이면 값이 [base, 위끝] 에 고르게 남고,
        // 바닥이 상한 가까이 와도 몰리지 않는다.
        long top = Math.min(Math.round(base * (1 + jitterRatio)), maxSec);
        // **위로만 흔든다.** 아래로 흔들면 바닥보다 빨리 부르게 되어 장애
        // 구간에만 폴링 예산이 안 걸린다.
        double jittered = base + (top - base) * random.getAsDouble();
        // 0 은 안 준다. 즉시 재시도는 흩어짐이 없어 파도를 그대로 되돌린다.
        return Math.clamp(Math.round(jittered), 1, maxSec);
    }
}

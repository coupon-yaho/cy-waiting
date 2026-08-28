package com.kafkick.waiting.domain.queue;

import java.time.Duration;

import java.util.function.DoubleSupplier;

/**
 * 폴링 간격을 서버가 정한다 (D-2).
 *
 * <p>부하를 정하는 것은 대기 인원이 아니라 <b>큐의 시간 깊이</b>다. 개인은
 * 자기가 얼마나 기다릴지 모르므로 클라이언트에 맡길 수 없다.
 */
public class PollIntervalPolicy {

    /** ETA 밴드 경계(초). 위 경계는 다음 밴드에 속한다. */
    private static final double[] BAND_EDGES = {5, 30, 120};

    /** 밴드별 기본 간격(초). 가장 먼 밴드가 30 이라 예산 식이 {@code waiting/30} 이다. */
    private static final long[] BAND_INTERVALS = {1, 3, 10, 30};

    private static final long MIN_INTERVAL_SEC = 1;
    private static final long MAX_INTERVAL_SEC = 60;

    /**
     * 클라이언트에게 줄 수 있는 가장 긴 간격.
     *
     * <p>매진 큐 정리가 이 값을 넘겨 기다려야 한다 — 마지막 폴링이 언제
     * 오는지를 정하는 것이 이 값이다.
     */
    public static Duration maxInterval() {
        return Duration.ofSeconds(MAX_INTERVAL_SEC);
    }

    /** 백그라운드 탭이 분당 1회로 스로틀돼도 살아 있다고 봐야 한다. */
    private static final long MIN_ALIVE_TTL_SEC = 30;

    private static final long ALIVE_TTL_FACTOR = 3;

    private final double jitterRatio;

    private PollIntervalPolicy(double jitterRatio) {
        this.jitterRatio = jitterRatio;
    }

    /** {@code jitterRatio} 는 기본 간격에 곱해지는 흔들림 폭이다. */
    public static PollIntervalPolicy of(double jitterRatio) {
        if (!Double.isFinite(jitterRatio) || jitterRatio < 0) {
            throw new IllegalArgumentException(
                    "jitterRatio 는 0 이상 유한값이어야 한다: %s".formatted(jitterRatio));
        }
        return new PollIntervalPolicy(jitterRatio);
    }

    public long intervalSec(double etaSec, DoubleSupplier random) {
        return intervalSec(etaSec, random, 1.0);
    }

    /**
     * 이 사람의 폴링 간격.
     *
     * <p>{@code random} 은 {@code [0,1]} 을 낸다. <b>주입받는다</b> — 도메인이
     * 난수원을 직접 부르면 실패를 재현할 수 없다 (DS-1).
     *
     * @param pollScale 전역 예산이 모자랄 때 모두의 간격을 함께 늘리는 배수
     */
    public long intervalSec(double etaSec, DoubleSupplier random, double pollScale) {
        long base = bandInterval(etaSec);
        double scaled = base * Math.max(1.0, pollScale);
        // [-jitter, +jitter] 로 흔들어 같은 밴드가 동시에 두드리지 않게 한다
        double jittered = scaled * (1 + jitterRatio * (2 * random.getAsDouble() - 1));
        return Math.clamp(Math.round(jittered), MIN_INTERVAL_SEC, MAX_INTERVAL_SEC);
    }

    /**
     * 이 간격으로 폴링하는 사람의 생존 TTL.
     *
     * <p>간격만 보고 잡으면 백그라운드 탭이 스로틀된 사람이 이탈자로 지워진다.
     */
    public long aliveTtlSec(long intervalSec) {
        return Math.max(MIN_ALIVE_TTL_SEC, intervalSec * ALIVE_TTL_FACTOR);
    }

    /** ETA 를 모르면 가장 먼 밴드다 — 모를수록 자주 묻게 하면 안 된다. */
    private long bandInterval(double etaSec) {
        // 모름과 말이 안 되는 값을 같이 본다. 음수는 첫 밴드에 걸려 1초가 되는데,
        // 그 값이 나오는 조건이 하필 배수가 멈춘 순간이다.
        if (!(etaSec >= 0)) {
            return BAND_INTERVALS[BAND_INTERVALS.length - 1];
        }
        for (int i = 0; i < BAND_EDGES.length; i++) {
            if (etaSec < BAND_EDGES[i]) {
                return BAND_INTERVALS[i];
            }
        }
        return BAND_INTERVALS[BAND_INTERVALS.length - 1];
    }
}

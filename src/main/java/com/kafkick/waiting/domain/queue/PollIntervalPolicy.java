package com.kafkick.waiting.domain.queue;

import java.time.Duration;

import java.util.function.DoubleSupplier;

/**
 * 폴링 간격을 서버가 정한다. 부하를 정하는 것은 대기 인원이 아니라 <b>큐의
 * 시간 깊이</b>이고, 개인은 자기가 얼마나 기다릴지 몰라 클라이언트에 맡길 수 없다.
 */
public class PollIntervalPolicy {

    /** ETA 밴드 경계(초). 위 경계는 다음 밴드에 속한다. */
    private static final double[] BAND_EDGES = {5, 30, 120};

    /** 밴드별 기본 간격(초). 가장 먼 밴드가 30 이라 예산 식이 {@code waiting/30} 이다. */
    private static final long[] BAND_INTERVALS = {1, 3, 10, 30};

    /**
     * 배수를 안 거는 갈래가 넘기는 값. {@code 1.0} 을 그대로 쓰면 배수를 깜빡한
     * 것과 구분이 안 된다. 안 거는 <b>이유</b>는 갈래마다 다르니 그 자리에 적는다.
     */
    public static final double NO_SCALE = 1.0;

    /**
     * 정상 경로의 흔들림 폭. <b>배선값을 여기 둔다</b> — 오류 경로가 이보다 넓어야
     * 한다는 관계를 시험이 재는데, 배선에만 있으면 리터럴 둘의 비교가 된다.
     */
    public static final double NORMAL_JITTER_RATIO = 0.2;

    private static final long MIN_INTERVAL_SEC = 1;
    private static final long MAX_INTERVAL_SEC = 60;

    /**
     * 서버가 시킨 간격을 지키는 사람이 <b>몇 번까지 놓쳐도 되는가</b>. 모바일
     * 브라우저가 탭을 뒤로 보내면 타이머가 뭉텅이로 밀린다. 두 번은 흔하고 세
     * 번은 드물다 — 그 위는 사람이 떠난 것으로 본다.
     */
    private static final long MISSED_POLLS = 3;

    /**
     * 마지막 폴링의 왕복과 타이머 드리프트를 덮는 여유(초). 없으면 약속한 횟수가
     * 실제로는 하나 적다 ({@link #aliveTtl()}). <b>아직 가정이다</b> — 240초짜리
     * 창의 4%이고, 과부하 구간의 왕복 p99 가 2.5초를 넘으면 4회분을 못 덮는다.
     */
    private static final long TTL_MARGIN_SEC = 10;

    /**
     * 생존 신호 수명. <b>상수로 두면 안 된다</b> — 간격이 60초까지 늘어나면 성실히
     * 줄 선 사람의 신호가 먼저 만료돼 이탈자로 걷히고, 재입장은 새 순번이라 순번
     * 역행이다. k 번 놓친 사람은 (k+1)·간격에 오므로 하나 더 곱하고 여유를 더한다.
     */
    public static Duration aliveTtl() {
        return maxInterval().multipliedBy(MISSED_POLLS + 1)
                .plusSeconds(TTL_MARGIN_SEC);
    }

    /** 매진 큐 정리가 이 값을 넘겨 기다려야 한다 — 마지막 폴링을 이것이 정한다. */
    public static Duration maxInterval() {
        return Duration.ofSeconds(MAX_INTERVAL_SEC);
    }

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

    /**
     * 이 사람의 폴링 간격. {@code random} 은 {@code [0,1]} 을 낸다. <b>주입받는다</b>
     * — 도메인이 난수원을 직접 부르면 실패를 재현할 수 없다.
     *
     * @param pollScale 전역 예산이 모자랄 때 모두의 간격을 함께 늘리는 배수
     */
    public long intervalSec(double etaSec, DoubleSupplier random, double pollScale) {
        long base = bandInterval(etaSec);
        // **자르고 나서 흔든다.** 흔든 뒤에 자르면 상한 위 값이 상한 하나로 모여
        // 배수가 걸린 밴드에서 지터가 0 이 되고, 그 밴드 전원이 같은 초에 돌아온다.
        // 천장을 흔들림의 위쪽 끝에 걸어, 평균은 상한이 아니라 상한/(1+지터) 다.
        //
        // 클라이언트가 받는 값은 그래도 60 을 안 넘으므로, 생존 신호 수명을
        // 이 상한에서 끌어내는 것은 그대로다.
        double ceiling = MAX_INTERVAL_SEC / (1 + jitterRatio);
        // **하한을 여기서도 건다.** SnapshotMeta 가 이미 정규화했지만 이 인자는
        // 그냥 double 이라, 1 미만이 들어오면 한산할 때 오히려 부하를 만든다.
        // 사본이 아니라 공개 API 의 방어이고, 양쪽 다 자기 시험이 있다.
        double scaled = Math.min(base * Math.max(1.0, pollScale), ceiling);
        // [-jitter, +jitter] 로 흔들어 같은 밴드가 동시에 두드리지 않게 한다
        double jittered = scaled * (1 + jitterRatio * (2 * random.getAsDouble() - 1));
        return Math.clamp(Math.round(jittered), MIN_INTERVAL_SEC, MAX_INTERVAL_SEC);
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

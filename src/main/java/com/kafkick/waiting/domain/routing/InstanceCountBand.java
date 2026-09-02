package com.kafkick.waiting.domain.routing;

/**
 * 인스턴스 수가 가정(A7) 안인가.
 *
 * <p><b>자동으로 전략을 안 바꾼다</b> (R-9). 인스턴스 수로 전환하면 임계 근처에서
 * 진동하고 — 롤링 배포가 정확히 그 구간을 지난다 — 어느 구간이 무슨 모드였는지가
 * 대시보드에 안 남아 장애 분석이 막힌다. 알리고 판단은 사람이 한다.
 */
public enum InstanceCountBand {

    /** 3~5 대. 이 규모에서는 가중 라운드로빈이 더 정확하고 단순하다. */
    BELOW,

    /** 10~20 대. 가정 그대로다. */
    EXPECTED,

    /** 50 대 이상. 전수 조회 비용을 다시 봐야 한다. */
    ABOVE;

    /** A7 이 정한 하한. 이보다 적으면 무작위 둘을 뽑는 이득이 없다. */
    private static final int LOW = 10;

    /** A7 이 정한 상한. 이보다 많으면 후보를 다 훑는 비용이 다시 문제가 된다. */
    private static final int HIGH = 20;

    /** <b>0 도 범위 밖이다.</b> 보낼 곳이 없다는 것 자체가 알려야 할 상태다. */
    public static InstanceCountBand of(int instances) {
        if (instances < LOW) {
            return BELOW;
        }
        return instances > HIGH ? ABOVE : EXPECTED;
    }

    /** 가정 안인가. 밖이면 사람이 전략을 다시 판단해야 한다. */
    public boolean withinAssumption() {
        return this == EXPECTED;
    }

    /** 사람이 읽을 사유. 로그에 그대로 실린다. */
    public String describe(int instances) {
        return switch (this) {
            case BELOW -> "인스턴스가 %d 대다 — 가정(A7)의 10 대보다 적다. 이 규모에서는 "
                    .formatted(instances)
                    + "가중 라운드로빈이 더 정확하다 (waiting.routing.strategy=round-robin)";
            case ABOVE -> "인스턴스가 %d 대다 — 가정(A7)의 20 대보다 많다. 후보를 다 훑는 "
                    .formatted(instances) + "비용을 다시 본다";
            case EXPECTED -> "인스턴스가 %d 대다 — 가정(A7) 안이다".formatted(instances);
        };
    }
}

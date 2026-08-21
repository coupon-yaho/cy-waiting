package com.kafkick.waiting.control;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 배분의 분모가 되는 <b>게이트웨이 수</b>를 들고 있다.
 *
 * <p>각 노드는 {@code credit / N} 을 자기 몫으로 쓴다. 미달은 지연이지만 초과는
 * 초과 발급으로 번진다 — 그래서 증감의 무게가 다르다.
 *
 * <p><b>신선도는 여기서 안 잰다.</b> 하트비트 스크립트가 레디스 서버 시계 하나로
 * 판정한다. 여기서 또 재면 임계도 시계도 둘이 된다.
 */
public final class GatewayRegistry {

    /**
     * 감소를 확정하기까지 필요한 연속 관측 수.
     *
     * <p><b>개별 항목의 신선도만으로는 F5 가 안 지켜진다.</b> 레디스가 임계보다
     * 오래 끊겼다가 돌아오면 모든 항목이 낡은 상태라, 노드들이 하나씩 붙는 동안
     * 분모가 1·2·3… 으로 올라온다. 그 틱의 총 통과는 크레딧의 세 배 가까이 된다.
     * 그래서 <b>집계값 자체에도</b> 비대칭을 건다.
     */
    private final int rampDownTicks;

    private final AtomicInteger denominator;
    private final AtomicInteger smallerStreak = new AtomicInteger();

    private GatewayRegistry(int rampDownTicks, int initial) {
        this.rampDownTicks = rampDownTicks;
        this.denominator = new AtomicInteger(initial);
    }

    /**
     * @param rampDownTicks 감소를 확정하기까지의 연속 관측 수. 1 이면 즉시
     * @param initial       첫 관측 전에 쓸 분모. <b>예상 레플리카 수를 준다</b> —
     *                      1 로 두면 뜨는 노드가 전역 크레딧 전부를 자기 몫으로
     *                      쓴다. 틀리더라도 큰 쪽으로 틀리는 값을 고른다
     */
    public static GatewayRegistry of(int rampDownTicks, int initial) {
        if (rampDownTicks < 1) {
            throw new IllegalArgumentException(
                    "rampDownTicks 는 1 이상이어야 한다: %d".formatted(rampDownTicks));
        }
        if (initial < 1) {
            throw new IllegalArgumentException(
                    "initial 은 1 이상이어야 한다 — 0 으로 나눌 수 없다: %d".formatted(initial));
        }
        return new GatewayRegistry(rampDownTicks, initial);
    }

    /**
     * 하트비트 스크립트가 센 값을 받는다.
     *
     * <p><b>증가는 즉시, 감소는 연속 관측 뒤.</b> 늘었는데 늦게 반영하면 기존
     * 노드가 작은 분모로 나눠 총합이 크레딧을 넘는다. 줄었는데 늦게 반영하면
     * 총합이 미달할 뿐이다.
     *
     * @param observed 스크립트가 센 살아 있는 노드 수. 1 미만은 무시한다 —
     *                 스크립트는 자기 자신을 먼저 쓰므로 정상이면 최소 1 이다
     */
    public void observed(int observed) {
        if (observed < 1) {
            return;
        }
        int current = denominator.get();
        if (observed >= current) {
            denominator.set(observed);
            smallerStreak.set(0);
            return;
        }
        if (smallerStreak.incrementAndGet() >= rampDownTicks) {
            denominator.set(observed);
            smallerStreak.set(0);
        }
    }

    /**
     * 이번 관측이 실패했다. <b>직전 값을 지킨다.</b>
     *
     * <p>레디스가 안 되면 모든 노드가 같이 실패하므로, 여기서 분모를 낮추면
     * 전 노드가 동시에 몫을 키운다. 그건 회복이 아니라 증폭이다. 연속 감소
     * 카운터도 되돌린다 — 실패는 "더 작게 관측했다" 가 아니다.
     */
    public void observationFailed() {
        smallerStreak.set(0);
    }

    /** 지금 쓰는 분모. <b>1 아래로 내려가지 않는다</b> — 나누는 쪽이 있다. */
    public int count() {
        return denominator.get();
    }
}

package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.admission.CircuitState;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
     * 분모와 연속 감소 횟수. <b>한 덩어리로 바꾼다.</b>
     *
     * <p>둘을 따로 두면 읽고·비교하고·쓰는 사이에 다른 관측이 끼어, 큰 값이 작은
     * 값에 덮이거나 연속 카운트가 한 틱에 임계에 닿는다. 분모가 낮아지는 방향은
     * 초과 발급이다.
     */
    private record Denominator(int value, int smallerStreak) {
    }

    private final int rampDownTicks;
    private final AtomicReference<Denominator> current;

    /**
     * 클러스터가 본 서킷과 <b>푼 방향으로 이어진 관측 수</b> (CY-791).
     *
     * <p>한 덩어리로 바꾼다. 분모와 같은 이유다 — 따로 두면 읽고·비교하고·쓰는
     * 사이에 다른 관측이 끼어 조인 값이 푼 값에 덮인다.
     */
    private record Vote(CircuitState state, int easingStreak) {
    }

    /**
     * 리더 한 대의 로컬 관측을 쓰면 두 방향으로 틀린다 — 리더만 정상이면
     * 나머지가 다 열려 있어도 배분이 평소 속도로 돈다.
     */
    private final AtomicReference<Vote> clusterCircuit =
            new AtomicReference<>(new Vote(CircuitState.CLOSED, 0));

    /** 하트비트가 연속으로 못 돈 횟수. 표가 낡았는지를 이걸로 안다. */
    private final AtomicInteger circuitMisses = new AtomicInteger();

    private GatewayRegistry(int rampDownTicks, int initial) {
        this.rampDownTicks = rampDownTicks;
        this.current = new AtomicReference<>(new Denominator(initial, 0));
    }

    /**
     * @param rampDownTicks 감소를 확정하기까지의 연속 관측 수. 1 이면 즉시
     * @param initial       첫 관측 전에 쓸 분모. <b>예상 레플리카 수를 준다</b>
     */
    public static GatewayRegistry of(int rampDownTicks, int initial) {
        if (rampDownTicks < 1) {
            throw new IllegalArgumentException(
                    "rampDownTicks 는 1 이상이어야 한다: %d".formatted(rampDownTicks));
        }
        // 1 로 두면 뜨는 노드가 전역 크레딧 전부를 자기 몫으로 쓴다. 0 은 나눌 수 없다.
        if (initial < 1) {
            throw new IllegalArgumentException(
                    "initial 은 1 이상이어야 한다: %d".formatted(initial));
        }
        return new GatewayRegistry(rampDownTicks, initial);
    }

    /**
     * 하트비트가 센 것을 그대로 받는다. 셋을 나눠 받으면 다른 판의 것이 섞인다.
     *
     * <p><b>조이는 방향은 즉시, 푸는 방향은 연속 관측 뒤.</b> 분모와 같은
     * 비대칭이다. 한 틱 만에 풀면 표 하나가 늦거나 시체가 만료되는 것만으로
     * 전면 개방이 일어나고, 과반 경계에서는 매 틱 뒤집혀 배분이 0 과 평시를
     * 오간다 — 0 인 틱마다 서킷의 시험 호출이 끊겨 회복 시도가 늘어난다.
     */
    public void circuitObserved(int alive, int open, int halfOpen) {
        circuitMisses.set(0);
        apply(ClusterCircuit.of(alive, open, halfOpen));
    }

    /**
     * 하트비트가 못 돌았다. <b>연속으로 놓치면 로컬 관측으로 돌아간다</b>.
     *
     * <p>안 그러면 마지막 판정에 얼어붙는다. 조인 채로 얼면 레디스가 회복될
     * 때까지 배분이 0 이고, 푼 채로 얼면 뒷단이 무너져도 안 조인다. 어느 쪽이든
     * 뒷단 게이팅이 레디스 가용성에 묶이는데, 원래 이 판단은 메모리 안에 있었다.
     *
     * @param local 이 노드가 지금 보고 있는 서킷
     */
    public void circuitMissed(CircuitState local) {
        if (circuitMisses.incrementAndGet() >= rampDownTicks) {
            apply(local);
        }
    }

    private void apply(CircuitState seen) {
        clusterCircuit.updateAndGet(now -> {
            if (ClusterCircuit.severity(seen) >= ClusterCircuit.severity(now.state())) {
                return new Vote(seen, 0);
            }
            int streak = now.easingStreak() + 1;
            return streak >= rampDownTicks ? new Vote(seen, 0) : new Vote(now.state(), streak);
        });
    }

    /** 배분이 읽는 값. 관측이 오기 전에는 닫힌 것으로 본다. */
    public CircuitState circuit() {
        return clusterCircuit.get().state();
    }

    /**
     * 하트비트 스크립트가 센 값을 받는다. <b>증가는 즉시, 감소는 연속 관측 뒤.</b>
     *
     * @param observed 스크립트가 센 살아 있는 노드 수. 1 미만은 무시한다 —
     *                 스크립트는 자기 자신을 먼저 쓰므로 정상이면 최소 1 이다
     */
    public void observed(int observed) {
        if (observed < 1) {
            return;
        }
        // 늘었는데 늦게 반영하면 기존 노드가 작은 분모로 나눠 총합이 크레딧을
        // 넘는다. 줄었는데 늦게 반영하면 총합이 미달할 뿐이다.
        current.updateAndGet(now -> {
            if (observed >= now.value()) {
                return new Denominator(observed, 0);
            }
            int streak = now.smallerStreak() + 1;
            return streak >= rampDownTicks
                    ? new Denominator(observed, 0)
                    : new Denominator(now.value(), streak);
        });
    }

    /**
     * 이번 관측이 실패했다. <b>직전 값을 지킨다.</b>
     *
     * <p>레디스가 안 되면 모든 노드가 같이 실패한다. 여기서 분모를 낮추면 전
     * 노드가 동시에 몫을 키운다 — 회복이 아니라 증폭이다.
     */
    public void observationFailed() {
        // 실패는 "더 작게 관측했다" 가 아니다. 섞어 세면 실패만으로 내려간다.
        current.updateAndGet(now -> new Denominator(now.value(), 0));
    }

    /** 지금 쓰는 분모. <b>1 아래로 내려가지 않는다</b> — 나누는 쪽이 있다. */
    public int count() {
        return current.get().value();
    }
}

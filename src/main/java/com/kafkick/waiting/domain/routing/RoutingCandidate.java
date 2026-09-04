package com.kafkick.waiting.domain.routing;

import java.time.Duration;
import java.util.Objects;

/**
 * 라우팅 후보 하나. <b>견주는 것은 절대량이 아니라 여유 대비 부하다</b> (R-4).
 *
 * <p>in-flight 만 균등화하면 여유 200 인 대와 40 인 대에 같은 양이 간다 —
 * 뒤엣것이 먼저 무너진다.
 *
 * @param instanceId 인스턴스 식별자
 * @param credits    이 인스턴스가 받을 수 있는 양. 0 이면 후보가 아니다
 * @param inFlight   지금 이 게이트웨이가 이 인스턴스에 물려 둔 요청 수
 * @param seed       기동 직후 램프 구간에만 실리는 보고값. 램프가 끝나면 0 이다.
 *                   배제의 램프는 이 자리를 안 쓰고 여유 쪽을 줄인다
 */
public record RoutingCandidate(String instanceId, long credits, int inFlight, double seed) {

    public RoutingCandidate {
        Objects.requireNonNull(instanceId, "instanceId");
        if (credits < 0) {
            throw new IllegalArgumentException("credits 는 0 이상이어야 한다: " + credits);
        }
        if (inFlight < 0) {
            throw new IllegalArgumentException("inFlight 는 0 이상이어야 한다: " + inFlight);
        }
        if (!(seed >= 0) || !Double.isFinite(seed)) {
            throw new IllegalArgumentException("seed 는 0 이상 유한값이어야 한다: " + seed);
        }
    }

    /** 램프가 끝난 평상시. 초기값을 안 쓴다 — 낡은 보고로 라우팅하면 되레 해롭다. */
    public static RoutingCandidate of(String instanceId, long credits, int inFlight) {
        return new RoutingCandidate(instanceId, credits, inFlight, 0);
    }

    public static RoutingCandidate of(String instanceId, long credits, int inFlight, double seed) {
        return new RoutingCandidate(instanceId, credits, inFlight, seed);
    }

    /**
     * 기동 직후 램프 구간에만 실리는 초기값.
     *
     * <p><b>막 뜬 게이트웨이는 전 인스턴스가 0 으로 보인다</b> — 열화된 대를
     * 못 가려 정상 비율만큼 보낸다 (G9.12). 보고는 낡았지만 0 보다는 낫다.
     */
    // 로컬 관측이 쌓일수록 무게를 선형으로 뺀다. 램프가 끝나면 식이 원래대로
    // 돌아가고, 그때부터는 진단용이라는 원칙이 그대로다.
    public static double seed(double reportedInFlight, Duration elapsed, Duration ramp) {
        // **밀리초로 재므로 1ms 미만은 램프가 없는 것과 같다.** 안 가르면 분모가
        // 0 이 되어 NaN 이 나오고, 그 값이 후보 생성에서 터져 라우팅이 통째로 멎는다.
        //
        // 음수도 이 한 줄이 덮는다 — 따로 검사하면 밟을 수 없는 갈래가 된다.
        if (ramp.toMillis() <= 0 || !(reportedInFlight > 0)) {
            return 0;
        }
        double remaining = 1 - (double) elapsed.toMillis() / ramp.toMillis();
        return remaining <= 0 ? 0 : reportedInFlight * remaining;
    }

    /** 보낼 수 있는 대인가. <b>여유 0 은 후보가 아니다</b> — 0 으로 나누지 않는다. */
    public boolean eligible() {
        return credits > 0;
    }

    /** 여유 대비 얼마나 찼는가. 이 값이 작은 쪽으로 보낸다. */
    public double loadFactor() {
        if (!eligible()) {
            throw new IllegalStateException("여유가 0 인 후보의 부하율을 물었다: " + instanceId);
        }
        return (inFlight + seed) / credits;
    }
}

package com.kafkick.waiting.domain.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 여유 비율대로 <b>결정적으로</b> 돈다 (R-9).
 *
 * <p>여유대로 세어 놓고 몰아 주면 한 바퀴의 합은 맞아도 그 구간에 그 대가
 * 무너진다. 매번 여유만큼 더하고 가장 앞선 대를 골라 총합만큼 빼는 방식이라
 * 비율이 정확하면서 고루 펴진다.
 */
public final class WeightedRoundRobin implements InstanceChooser {

    private WeightedRoundRobin() {
    }

    /** 새 순환을 연다. 누적을 들고 있으므로 인스턴스마다 하나여야 한다. */
    public static WeightedRoundRobin create() {
        return new WeightedRoundRobin();
    }

    /** 인스턴스별 누적. 사라진 대는 지운다 — 식별자가 재기동마다 새로 온다 (R-3). */
    private final Map<String, Long> 누적 = new HashMap<>();

    @Override
    public synchronized Optional<RoutingCandidate> choose(List<RoutingCandidate> candidates) {
        List<RoutingCandidate> 살아있는 = new ArrayList<>();
        long 총합 = 0;
        Set<String> 지금_있는 = new HashSet<>();
        for (RoutingCandidate c : candidates) {
            지금_있는.add(c.instanceId());
            if (c.eligible()) {
                살아있는.add(c);
                총합 += c.credits();
            }
        }
        누적.keySet().retainAll(지금_있는);
        if (살아있는.isEmpty()) {
            return Optional.empty();
        }

        RoutingCandidate 고른 = null;
        long 가장_앞선 = Long.MIN_VALUE;
        for (RoutingCandidate c : 살아있는) {
            long 값 = 누적.merge(c.instanceId(), c.credits(), Long::sum);
            if (값 > 가장_앞선) {
                가장_앞선 = 값;
                고른 = c;
            }
        }
        누적.merge(고른.instanceId(), -총합, Long::sum);
        return Optional.of(고른);
    }

    /** 지금 누적을 들고 있는 인스턴스들. 사라진 대가 남아 있는지를 시험이 본다. */
    public synchronized Set<String> tracked() {
        return Set.copyOf(누적.keySet());
    }
}

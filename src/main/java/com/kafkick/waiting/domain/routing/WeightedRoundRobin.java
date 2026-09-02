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
    private final Map<String, Long> credit = new HashMap<>();

    @Override
    public synchronized Optional<RoutingCandidate> choose(List<RoutingCandidate> candidates) {
        List<RoutingCandidate> eligible = new ArrayList<>();
        long total = 0;
        Set<String> present = new HashSet<>();
        for (RoutingCandidate c : candidates) {
            present.add(c.instanceId());
            if (c.eligible()) {
                eligible.add(c);
                // **넘치면 배분이 뒤집힌다.** 부호가 바뀌면 가장 여유 있는 대가
                // 가장 안 뽑히는 대가 되고, 그 배포 내내 조용히 그렇게 돈다.
                total = Math.addExact(total, c.credits());
            }
        }
        credit.keySet().retainAll(present);
        if (eligible.isEmpty()) {
            return Optional.empty();
        }

        // **다 셈한 뒤에 쓴다.** 누적을 그때그때 갱신하면 산술이 중간에 터졌을 때
        // 앞엣것만 움직인 채로 아무것도 안 고르고 돌아간다 — 다음 호출이 그
        // 유령 누적으로 고른다. 여기서 터지면 상태는 부른 적 없는 것과 같다.
        //
        // **지금은 그 경로를 못 만든다.** 위 합산이 같은 값을 먼저 더하므로,
        // 누적이 넘칠 값이면 거기서 먼저 터진다. 다만 인스턴스가 드나들면
        // 누적의 합이 0 이 아니게 되어 조금씩 밀릴 수 있고, 그때는 여기가
        // 먼저 넘친다 — 그 경우를 시험으로 못 만들어서 방어만 둔다.
        Map<String, Long> next = new HashMap<>();
        RoutingCandidate chosen = null;
        long leading = Long.MIN_VALUE;
        for (RoutingCandidate c : eligible) {
            long value = Math.addExact(credit.getOrDefault(c.instanceId(), 0L), c.credits());
            next.put(c.instanceId(), value);
            if (value > leading) {
                leading = value;
                chosen = c;
            }
        }
        next.put(chosen.instanceId(), Math.subtractExact(leading, total));
        credit.putAll(next);
        return Optional.of(chosen);
    }

    /** 지금 누적을 들고 있는 인스턴스들. 사라진 대가 남아 있는지를 시험이 본다. */
    public synchronized Set<String> tracked() {
        return Set.copyOf(credit.keySet());
    }
}

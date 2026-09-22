package com.kafkick.waiting.control;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 클러스터가 뺀 뒷단. 노드마다 따로 센 배제를 과반으로 접는다. <b>분모는 표를 실은 수가 아니라
 * 산 수다</b> — 롤아웃 중 새 노드 둘이 과반이 되면 옛 노드가 아직 보내는 대의 예산이 지워진다.
 */
public final class ClusterEjection {

    private ClusterEjection() {
    }

    /**
     * @param alive 살아 있는 노드 수. 1 미만이면 관측이 망가진 것이라 아무것도 안 뺀다
     * @param votes 인스턴스마다 그것을 뺀 노드 수
     */
    public static Set<String> majority(int alive, Map<String, Integer> votes) {
        if (alive < 1) {
            return Set.of();
        }
        return votes.entrySet().stream()
                .filter(vote -> vote.getValue() * 2 > alive)
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }
}

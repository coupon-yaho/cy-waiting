package com.kafkick.waiting.domain.routing;

import java.util.List;
import java.util.Optional;

/**
 * 후보 중 하나를 고른다.
 *
 * <p><b>둘을 다 만든다</b> (R-9). 인스턴스가 3~5대로 줄면 가중 라운드로빈이 더
 * 정확하고 단순하다 — 어느 쪽이 나은지는 실측으로 정할 문제라, 코드에 하나만
 * 박아 두면 그 측정을 할 수가 없다.
 */
public interface InstanceChooser {

    /** @return 보낼 인스턴스. 보낼 곳이 없으면 비어 있다 */
    Optional<RoutingCandidate> choose(List<RoutingCandidate> candidates);
}

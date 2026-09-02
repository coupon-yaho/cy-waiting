package com.kafkick.waiting.domain.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntUnaryOperator;

/**
 * 무작위 둘 중 <b>여유 대비 덜 찬 쪽</b>으로 보낸다.
 *
 * <p>최소값을 그냥 고르면 게이트웨이 M 대가 같은 인스턴스로 동시에 몰린다.
 * 그놈이 순식간에 가장 바쁜 놈이 되고 다 같이 다음으로 옮겨 간다 — 진동한다.
 * 무작위 둘을 뽑는 것만으로 그 쏠림이 깨진다.
 */
public final class WeightedP2c implements InstanceChooser {

    private final IntUnaryOperator random;

    /** @param random 상한 미만의 자리를 하나 준다. {@code Random::nextInt} 를 넘긴다 */
    public WeightedP2c(IntUnaryOperator random) {
        this.random = Objects.requireNonNull(random, "random");
    }

    @Override
    public Optional<RoutingCandidate> choose(List<RoutingCandidate> candidates) {
        List<RoutingCandidate> 살아있는 = new ArrayList<>();
        for (RoutingCandidate c : candidates) {
            if (c.eligible()) {
                살아있는.add(c);
            }
        }
        if (살아있는.isEmpty()) {
            return Optional.empty();
        }
        if (살아있는.size() == 1) {
            return Optional.of(살아있는.get(0));
        }
        RoutingCandidate 첫째 = 살아있는.get(자리(살아있는.size()));
        RoutingCandidate 둘째 = 살아있는.get(자리(살아있는.size()));
        return Optional.of(첫째.loadFactor() <= 둘째.loadFactor() ? 첫째 : 둘째);
    }

    /** 범위 밖 값이 오면 터지는 대신 접는다 — 라우팅이 무작위 구현에 안 걸린다. */
    private int 자리(int 상한) {
        return Math.floorMod(random.applyAsInt(상한), 상한);
    }
}

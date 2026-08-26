package com.kafkick.waiting.control;

import java.util.List;
import java.util.Objects;

/**
 * 배분 대상과 <b>그것을 읽은 시각</b>.
 *
 * <p>리더 벽시계로 찍으면 같은 스냅샷이 노드마다 다르게 낡는다.
 *
 * @param now 읽은 순간의 <b>레디스</b> 시각(초)
 */
public record TimedCoupons(List<String> coupons, long now) {

    public TimedCoupons {
        Objects.requireNonNull(coupons, "coupons 는 필수다");
        coupons = List.copyOf(coupons);
    }
}

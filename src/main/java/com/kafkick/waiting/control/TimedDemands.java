package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.allocation.CouponDemand;
import java.util.List;
import java.util.Objects;

/**
 * 이번 틱의 수요와 <b>그것을 읽은 시각</b>.
 *
 * <p>재료를 읽은 순간이 곧 그 재료의 나이가 시작되는 지점이다.
 *
 * @param readAt 읽은 순간의 <b>레디스</b> 시각(초)
 */
public record TimedDemands(List<CouponDemand> demands, long readAt) {

    public TimedDemands {
        Objects.requireNonNull(demands, "demands 는 필수다");
        demands = List.copyOf(demands);
    }
}

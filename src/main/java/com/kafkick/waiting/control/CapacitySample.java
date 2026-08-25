package com.kafkick.waiting.control;

import java.util.List;
import java.util.Objects;

/**
 * 한 순간의 가용량 관측.
 *
 * <p><b>보고와 기준 시각을 갈라서 들지 않는다.</b> 갈리면 두 왕복 사이의 갱신
 * 때문에 나이가 음수로 나오고, 클러스터에서는 아예 다른 노드의 벽시계가 된다.
 *
 * @param reports 신선도를 아직 안 본 보고들
 * @param now     그 보고들을 읽은 노드의 시각(초)
 */
public record CapacitySample(List<CapacityReport> reports, long now) {

    public CapacitySample {
        Objects.requireNonNull(reports, "reports 는 필수다");
        reports = List.copyOf(reports);
    }
}

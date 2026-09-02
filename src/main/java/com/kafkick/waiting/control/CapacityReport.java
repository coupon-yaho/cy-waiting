package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.routing.InstanceAddress;
import java.util.Optional;

/**
 * 뒷단 인스턴스 하나가 보고한 여유.
 *
 * <p><b>밖에서 추측하지 않는다.</b> 자기 상태는 그쪽이 가장 잘 알고, 그래야
 * 게이트웨이가 인스턴스 목록을 알 필요도 없어진다.
 *
 * @param reportedAt 읽는 쪽의 {@code now} 와 <b>같은 시계</b>여야 한다
 * @param address    라우팅할 주소 (D-C1). <b>없을 수 있다</b> — 그러면 크레딧에는
 *                   들지만 라우팅 후보는 아니다
 */
public record CapacityReport(String instanceId, long credits, long reportedAt,
        InstanceAddress address) {

    /** 주소를 안 실은 보고. 크레딧 계산은 그대로 하고 라우팅에서만 빠진다. */
    // **주소가 없다고 버리지 않는다.** 버리면 그 인스턴스 몫만큼 전역 크레딧이
    // 조용히 줄어, 계약을 아직 안 따르는 판올림 구간에 전체가 조여진다.
    public CapacityReport(String instanceId, long credits, long reportedAt) {
        this(instanceId, credits, reportedAt, null);
    }

    /** 여기로 보낼 수 있는가. 모양을 못 지킨 주소는 여기서 이미 빠져 있다. */
    public Optional<InstanceAddress> routableAddress() {
        return Optional.ofNullable(address);
    }
}

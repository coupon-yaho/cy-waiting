package com.kafkick.waiting.domain.routing;

import java.util.Objects;

/**
 * 라우팅에 쓰는 인스턴스 하나. <b>스냅샷에 실려 전 노드에 간다.</b>
 *
 * <p>요청 경로는 레디스를 안 친다 (불변식 1). 보고는 리더만 읽고, 라우팅에
 * 필요한 것만 골라 판정 재료에 실어 보낸다.
 *
 * @param address 뒷단이 보고에 실은 주소. 모양을 못 지킨 값은 여기 못 온다
 * @param credits 이 인스턴스가 받을 수 있는 양
 */
public record InstanceRouting(String instanceId, InstanceAddress address, long credits) {

    /** 인코딩이 쓰는 구분자. 식별자에 섞이면 그 줄이 통째로 어긋난다. */
    private static final String SEPARATORS = ",|";

    public InstanceRouting {
        Objects.requireNonNull(instanceId, "instanceId");
        Objects.requireNonNull(address, "address");
        if (credits < 0) {
            throw new IllegalArgumentException("credits 는 0 이상이어야 한다: " + credits);
        }
    }

    /** 식별자가 구분자를 담지 않는가. <b>밖에서 오는 값이라 확인하고 싣는다.</b> */
    public boolean encodable() {
        if (instanceId.isBlank()) {
            return false;
        }
        for (int i = 0; i < SEPARATORS.length(); i++) {
            if (instanceId.indexOf(SEPARATORS.charAt(i)) >= 0) {
                return false;
            }
        }
        return true;
    }
}

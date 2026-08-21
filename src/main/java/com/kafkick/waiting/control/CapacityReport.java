package com.kafkick.waiting.control;

/**
 * 뒷단 인스턴스 하나가 보고한 여유.
 *
 * <p><b>밖에서 추측하지 않는다.</b> 자기 상태는 그쪽이 가장 잘 안다.
 *
 * @param instanceId 보고한 인스턴스
 * @param credits    초당 받을 수 있다고 스스로 판단한 양
 * @param reportedAt 보고 시각(초). 읽는 쪽의 {@code now} 와 <b>같은 시계</b>여야 한다
 */
public record CapacityReport(String instanceId, long credits, long reportedAt) {
}

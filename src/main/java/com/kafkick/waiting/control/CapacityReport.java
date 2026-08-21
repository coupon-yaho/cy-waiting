package com.kafkick.waiting.control;

/**
 * 뒷단 인스턴스 하나가 보고한 여유.
 *
 * <p><b>밖에서 추측하지 않는다.</b> 자기 상태는 그쪽이 가장 잘 알고, 그래야
 * 게이트웨이가 인스턴스 목록을 알 필요도 없어진다.
 *
 * @param reportedAt 읽는 쪽의 {@code now} 와 <b>같은 시계</b>여야 한다
 */
public record CapacityReport(String instanceId, long credits, long reportedAt) {
}

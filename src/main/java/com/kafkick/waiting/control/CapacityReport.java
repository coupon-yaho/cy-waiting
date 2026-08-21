package com.kafkick.waiting.control;

/**
 * 뒷단 인스턴스 하나가 보고한 여유.
 *
 * <p><b>밖에서 추측하지 않는다.</b> 자기 상태는 그쪽이 가장 잘 알고, 그래야
 * 게이트웨이가 인스턴스 목록을 알 필요도 없어진다.
 *
 * @param instanceId 보고한 인스턴스
 * @param credits    초당 받을 수 있다고 스스로 판단한 양
 * @param reportedAt 보고 시각(초). <b>레디스 서버 시각</b>이다
 */
public record CapacityReport(String instanceId, long credits, long reportedAt) {
}

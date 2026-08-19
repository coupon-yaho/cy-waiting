package com.kafkick.waiting.domain.admission;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;

/**
 * 판정에 필요한 재료 전부. 도메인은 이것 말고 아무것도 안 본다.
 *
 * <p>{@code dataStale} 과 {@code justEnqueued} 는 <b>주입받는 값</b>이다. 도메인은
 * 시계도 노드 로컬 상태도 참조하지 않는다 (DS-1).
 *
 * @param couponKey     예산을 나누는 단위
 * @param state         스냅샷에서 읽은 쿠폰 상태
 * @param meta          전역 값
 * @param dataStale     스케줄러가 멎어 판정 재료가 오래됐는가
 * @param validToken    차례가 와서 받은 토큰을 들고 왔는가
 * @param justEnqueued  이 노드가 이 쿠폰을 방금 큐로 보냈는가
 * @param epochSecond   리미터 윈도우를 가르는 시각
 * @param maxEtaSec     받아도 되는 최대 대기 시간
 */
public record AdmissionRequest(
        String couponKey,
        CouponState state,
        SnapshotMeta meta,
        boolean dataStale,
        boolean validToken,
        boolean justEnqueued,
        long epochSecond,
        long maxEtaSec) {

    public AdmissionRequest withDataStale(boolean value) {
        return new AdmissionRequest(
                couponKey, state, meta, value, validToken, justEnqueued, epochSecond, maxEtaSec);
    }

    public AdmissionRequest withValidToken(boolean value) {
        return new AdmissionRequest(
                couponKey, state, meta, dataStale, value, justEnqueued, epochSecond, maxEtaSec);
    }

    public AdmissionRequest withJustEnqueued(boolean value) {
        return new AdmissionRequest(
                couponKey, state, meta, dataStale, validToken, value, epochSecond, maxEtaSec);
    }
}

package com.kafkick.waiting.domain.allocation;

/**
 * 배분 결과.
 *
 * <p><b>인원이 아니라 초당 속도다.</b> 인원으로 주면 노드가 그것을 언제 쓸지
 * 각자 정하게 되고, 같은 초에 몰리면 뒷단이 받는 순간 부하가 배분과 무관해진다.
 */
public record Grant(String couponId, long credit) {

    public Grant {
        if (couponId == null || couponId.isBlank()) {
            throw new IllegalArgumentException("couponId 는 필수다");
        }
        if (credit < 0) {
            throw new IllegalArgumentException("credit 은 0 이상이어야 한다: " + credit);
        }
    }
}

package com.kafkick.waiting.domain.allocation;

/**
 * 배분 결과. 이 쿠폰이 이번 틱에 통과시켜도 되는 양이다.
 *
 * @param couponId 배분 단위
 * @param credit   초당 통과 허용량
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

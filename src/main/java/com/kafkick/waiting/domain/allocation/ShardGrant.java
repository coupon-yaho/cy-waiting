package com.kafkick.waiting.domain.allocation;

/**
 * 샤드 하나에 배정된 몫.
 *
 * <p>샤드가 하나뿐인 지금도 번호를 들고 다닌다. 나중에 붙이면 이 값을 실어
 * 나르는 경로를 전부 다시 고쳐야 한다 (DS-7).
 */
public record ShardGrant(String couponId, int shardIndex, long credit) {

    public ShardGrant {
        if (couponId == null || couponId.isBlank()) {
            throw new IllegalArgumentException("couponId 는 필수다");
        }
        if (shardIndex < 0) {
            throw new IllegalArgumentException("shardIndex 는 0 이상이어야 한다: " + shardIndex);
        }
        if (credit < 0) {
            throw new IllegalArgumentException("credit 은 0 이상이어야 한다: " + credit);
        }
    }
}

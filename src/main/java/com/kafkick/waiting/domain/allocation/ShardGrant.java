package com.kafkick.waiting.domain.allocation;

/**
 * 샤드 하나에 배정된 몫.
 *
 * @param couponId   배분 단위
 * @param shardIndex 큐 샤드 번호. 샤드가 하나면 항상 0
 * @param credit     그 샤드가 통과시켜도 되는 양
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

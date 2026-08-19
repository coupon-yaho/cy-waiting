package com.kafkick.waiting.domain.allocation;

import java.util.List;

/**
 * 샤드가 하나인 동안의 구현. 전량을 0번 샤드에 준다.
 *
 * <p>몫이 0 이어도 목록을 비우지 않는다 — 비우면 호출부가 "이 쿠폰은 없다" 와
 * 구분하지 못한다.
 */
public class SingleShardSplitter implements ShardSplitter {

    private static final int ONLY_SHARD = 0;

    @Override
    public List<ShardGrant> split(Grant grant) {
        return List.of(new ShardGrant(grant.couponId(), ONLY_SHARD, grant.credit()));
    }
}

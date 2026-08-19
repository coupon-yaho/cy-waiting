package com.kafkick.waiting.domain.allocation;

import java.util.List;

/**
 * 쿠폰 몫을 큐 샤드에 나눈다.
 *
 * <p>지금 구현은 하나뿐이지만 <b>두 번째 사례가 Phase 10 에 예정돼 있어</b>
 * 미리 가른다 (DS-7). 나중에 가르려면 배분 로직까지 함께 건드려야 한다.
 */
public interface ShardSplitter {

    /** 빈 목록을 돌려주지 않는다 — 호출부가 "이 쿠폰은 없다" 와 구분하지 못한다. */
    List<ShardGrant> split(Grant grant);

    default List<ShardGrant> splitAll(List<Grant> grants) {
        return grants.stream().flatMap(g -> split(g).stream()).toList();
    }
}

package com.kafkick.waiting.domain.allocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 큐 분할의 자리만 미리 잡아 둔다.
 *
 * <p>지금은 샤드가 하나뿐이라 나눌 것이 없다. 그래도 <b>배분과 분할을 지금
 * 갈라 두면</b> Phase 10 에서 비례 분할을 끼워 넣을 때 배분을 안 건드린다.
 */
class ShardSplitterTest {

    @Test
    @DisplayName("샤드가_하나면_전량을_그_샤드에_배정한다")
    void 샤드가_하나면_전량을_그_샤드에_배정한다() {
        ShardSplitter splitter = SingleShardSplitter.create();

        assertThat(splitter.split(new Grant("c1", 500)))
                .containsExactly(new ShardGrant("c1", 0, 500));
    }

    @Test
    @DisplayName("몫이_0이어도_샤드_하나를_돌려준다")
    void 몫이_0이어도_샤드_하나를_돌려준다() {
        // 빈 목록을 주면 호출부가 "이 쿠폰은 없다" 와 구분하지 못한다.
        assertThat(SingleShardSplitter.create().split(new Grant("c1", 0)))
                .containsExactly(new ShardGrant("c1", 0, 0));
    }

    @Test
    @DisplayName("여러_배분도_각각_한_샤드로_간다")
    void 여러_배분도_각각_한_샤드로_간다() {
        List<ShardGrant> split = SingleShardSplitter.create()
                .splitAll(List.of(new Grant("a", 10), new Grant("b", 20)));

        assertThat(split).containsExactly(
                new ShardGrant("a", 0, 10), new ShardGrant("b", 0, 20));
    }

    @Test
    @DisplayName("잘못된_샤드_배정은_만들_수_없다")
    void 잘못된_샤드_배정은_만들_수_없다() {
        assertThatThrownBy(() -> new ShardGrant(null, 0, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShardGrant("  ", 0, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShardGrant("c1", -1, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShardGrant("c1", 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("잘못된_배분_결과는_만들_수_없다")
    void 잘못된_배분_결과는_만들_수_없다() {
        assertThatThrownBy(() -> new Grant(null, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Grant("  ", 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Grant("c1", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

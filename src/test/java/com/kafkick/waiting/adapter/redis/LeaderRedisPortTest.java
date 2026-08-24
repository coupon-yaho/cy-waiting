package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 리스가 락의 실제 수명이다. 값이 성립하지 않으면 <b>기동을 막는다</b> — 0 이면
 * 잡자마자 풀리고 음수는 드라이버에 따라 영영 안 풀린다.
 */
class LeaderRedisPortTest {

    private void 만든다(Duration lease) {
        new LeaderRedisPort(null, lease);
    }

    @Test
    @DisplayName("리스가_양수가_아니면_안_만들어진다")
    void 리스가_양수가_아니면_안_만들어진다() {
        assertThatThrownBy(() -> 만든다(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> 만든다(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> 만든다(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("연결_없이는_안_만들어진다")
    void 연결_없이는_안_만들어진다() {
        // 리스만 보고 넘기면 널 연결을 쥔 포트가 생기고, 첫 호출에서야 터진다.
        assertThatThrownBy(() -> 만든다(Duration.ofSeconds(2)))
                .isInstanceOf(NullPointerException.class);
    }
}

package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 샤드 배정은 <b>반드시 sticky</b> 여야 한다 (E-7).
 *
 * <p>같은 사람이 틱마다 다른 큐에 서면 순위가 앞뒤로 튀고, 불변식 3(순번 역행 0)이
 * 깨진다. 그래서 JVM 해시에 기대지 않고 CRC16 을 직접 구현한다.
 */
class ShardHashTest {

    @Test
    @DisplayName("같은_memberId는_항상_같은_샤드로_간다")
    void 같은_memberId는_항상_같은_샤드로_간다() {
        int first = ShardHash.shardOf("member-42", 16);

        for (int i = 0; i < 1000; i++) {
            assertThat(ShardHash.shardOf("member-42", 16)).isEqualTo(first);
        }
    }

    @Test
    @DisplayName("JVM_해시에_의존하지_않는다")
    void JVM_해시에_의존하지_않는다() {
        // String.hashCode() 는 판이 바뀌면 값이 달라질 수 있다. 그 순간
        // 전원이 다른 샤드로 옮겨 가고 진행 중인 큐가 통째로 어긋난다.
        // CRC16-CCITT(XMODEM) 의 알려진 값으로 못 박는다.
        assertThat(ShardHash.crc16("")).isZero();
        assertThat(ShardHash.crc16("123456789")).isEqualTo(0x31C3);
        assertThat(ShardHash.crc16("A")).isEqualTo(0x58E5);
    }

    @Test
    @DisplayName("샤드_수가_1이면_항상_0이다")
    void 샤드_수가_1이면_항상_0이다() {
        assertThat(ShardHash.shardOf("무엇이든", 1)).isZero();
        assertThat(ShardHash.shardOf("다른값", 1)).isZero();
    }

    @Test
    @DisplayName("결과는_항상_범위_안이다")
    void 결과는_항상_범위_안이다() {
        // 범위를 벗어나면 아무도 안 보는 키가 생기고 그 큐는 영영 안 빠진다.
        for (int i = 0; i < 5000; i++) {
            assertThat(ShardHash.shardOf("m" + i, 16)).isBetween(0, 15);
        }
    }

    @Test
    @DisplayName("샤드_수가_1미만이면_거부한다")
    void 샤드_수가_1미만이면_거부한다() {
        assertThatThrownBy(() -> ShardHash.shardOf("m1", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("빈_식별자는_거부한다")
    void 빈_식별자는_거부한다() {
        assertThatThrownBy(() -> ShardHash.shardOf(null, 4))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShardHash.shardOf("", 4))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

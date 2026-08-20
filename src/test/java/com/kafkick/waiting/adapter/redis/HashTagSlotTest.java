package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 같은 샤드의 키는 같은 슬롯에 모여야 한다 (RD-2).
 *
 * <p>흩어지면 하나의 Lua 가 원자적으로 다룰 수 없어 클러스터에서 거부된다.
 * <b>클러스터를 띄우지 않고</b> 확인한다 — 빠른 되먹임이 목적이다.
 */
class HashTagSlotTest {

    /** 레디스 클러스터의 슬롯 수. */
    private static final int SLOTS = 16_384;

    private int slotOf(String key) {
        return ShardHash.crc16(RedisKeys.hashTagOf(key)) % SLOTS;
    }

    @Test
    @DisplayName("같은_샤드의_키들이_같은_슬롯에_들어간다")
    void 같은_샤드의_키들이_같은_슬롯에_들어간다() {
        List<String> keys = List.of(
                RedisKeys.queue("c1", 4, 3),
                RedisKeys.maxScore("c1", 4, 3),
                RedisKeys.admitted("c1", 4, 3),
                RedisKeys.grace("c1", 4, 3),
                RedisKeys.alive("c1", 4, 3));

        assertThat(keys).extracting(this::slotOf).containsOnly(slotOf(keys.get(0)));
    }

    @Test
    @DisplayName("샤드가_다르면_슬롯도_갈린다")
    void 샤드가_다르면_슬롯도_갈린다() {
        // 갈려야 부하가 퍼진다. 안 갈리면 샤딩이 이름뿐이다.
        int distinct = (int) IntStream.range(0, 16)
                .mapToObj(s -> RedisKeys.queue("c1", 16, s))
                .map(this::slotOf)
                .distinct()
                .count();

        assertThat(distinct).isGreaterThan(8);
    }

    @Test
    @DisplayName("샤드가_하나여도_쿠폰별로_슬롯이_갈린다")
    void 샤드가_하나여도_쿠폰별로_슬롯이_갈린다() {
        assertThat(slotOf(RedisKeys.queue("c1", 1, 0)))
                .isNotEqualTo(slotOf(RedisKeys.queue("c2", 1, 0)));
    }

    @Test
    @DisplayName("재고_키는_큐와_같은_슬롯이_아닐_수_있다")
    void 재고_키는_큐와_같은_슬롯이_아닐_수_있다() {
        // 발급 계층이 소유하고 샤드 무관이다. 샤딩하면 슬롯이 갈리므로
        // Lua 에서 만지지 않고 별도로 읽는다 — 그 사실을 여기에 못 박는다.
        assertThat(slotOf(RedisKeys.stock("c1")))
                .isNotEqualTo(slotOf(RedisKeys.queue("c1", 4, 3)));
    }
}

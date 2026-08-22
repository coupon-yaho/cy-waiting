package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import com.kafkick.waiting.domain.allocation.Grant;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 스케줄러가 레디스에 내는 명령.
 *
 * <p>수요 수집은 <b>Lua 가 아니다.</b> 쿠폰마다 슬롯이 갈려 클러스터에서 못 돈다.
 * 재고도 샤드 무관 키라 같은 스크립트에서 못 읽는다.
 */
@Tag("integration")
@SpringBootTest
class AllocationRedisPortTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final int SHARDS = 1;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private AllocationRedisPort port;

    @BeforeEach
    void 준비() {
        port = AllocationRedisPort.of(redis, SHARDS);
        redis.delete(RedisKeys.ACTIVE_COUPONS,
                RedisKeys.queue("c1", SHARDS, 0), RedisKeys.admitted("c1", SHARDS, 0),
                RedisKeys.queue("c2", SHARDS, 0), RedisKeys.admitted("c2", SHARDS, 0),
                RedisKeys.stock("c1"), RedisKeys.stock("c2")).block(WAIT);
    }

    private void 줄_세운다(String couponId, long... scores) {
        for (long score : scores) {
            redis.opsForZSet().add(RedisKeys.queue(couponId, SHARDS, 0), "m" + score, score)
                    .block(WAIT);
        }
    }

    @Test
    @DisplayName("배분_대상만_읽는다")
    void 배분_대상만_읽는다() {
        // 목록에 없는 쿠폰은 스케줄러가 보지 않는다. 안 그러면 끝난 쿠폰까지
        // 매 틱 왕복이 늘어 틱이 밀린다.
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, "c1").block(WAIT);
        줄_세운다("c1", 10, 20);
        줄_세운다("c2", 10);

        assertThat(port.activeCoupons().block(WAIT)).containsExactly("c1");
    }

    @Test
    @DisplayName("샤드를_합쳐_대기를_센다")
    void 샤드를_합쳐_대기를_센다() {
        줄_세운다("c1", 10, 20, 30);
        줄_세운다("c2", 10);

        // **위치가 아니라 쿠폰으로 짝짓는다.** 위치로 맞추면 응답이 한 칸만 밀려도
        // A 의 대기가 B 의 재고와 붙는데, 그 조합은 도메인이 안 막는다.
        assertThat(port.queueSizes(List.of("c1", "c2")).block(WAIT))
                .containsOnly(entry("c1", 3L), entry("c2", 1L));
    }

    @Test
    @DisplayName("재고를_따로_읽는다")
    void 재고를_따로_읽는다() {
        // 재고는 샤드 무관 키라 큐와 슬롯이 갈린다. 같은 스크립트에서 못 읽는다.
        redis.opsForValue().set(RedisKeys.stock("c1"), "70").block(WAIT);

        // 없는 재고는 아예 안 담는다. 부르는 쪽이 "모른다" 를 0 으로 접는다.
        assertThat(port.stocks(List.of("c1", "c2")).block(WAIT))
                .containsOnly(entry("c1", 70L));
    }

    @Test
    @DisplayName("적용하면_임계가_올라간다")
    void 적용하면_임계가_올라간다() {
        줄_세운다("c1", 10, 20, 30);

        Long 들인_인원 = port.apply(new Grant("c1", 2)).block(WAIT);

        assertThat(들인_인원).isEqualTo(2);
        assertThat(redis.opsForValue().get(RedisKeys.admitted("c1", SHARDS, 0)).block(WAIT))
                .isEqualTo("20");
    }

    @Test
    @DisplayName("발행한_것을_그대로_읽는다")
    void 발행한_것을_그대로_읽는다() {
        port.publish(Map.of("c1", "OFF:QUEUEING:1:10:5:1.0", "#credit", "7")).block(WAIT);

        assertThat(port.load().block(WAIT))
                .containsEntry("#credit", "7")
                .containsEntry("c1", "OFF:QUEUEING:1:10:5:1.0");
    }

    @Test
    @DisplayName("발행이_실패해도_옛_값이_남는다")
    void 발행이_실패해도_옛_값이_남는다() {
        // 지우고 쓰는 것을 나눠 치면 그 사이에 끊길 때 키가 없는 채로 남는다.
        // 그러면 전 노드가 판정 재료를 잃고 낡음으로 넘어가, 줄 없는 쿠폰이
        // 통째로 통과한다. 리더가 스스로 공유 상태를 부수는 셈이다.
        port.publish(Map.of("c1", "a", "#credit", "7")).block(WAIT);

        assertThatThrownBy(() -> port.publish(Map.of()).block(WAIT))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(port.load().block(WAIT)).containsEntry("c1", "a");
    }

    @Test
    @DisplayName("발행은_통째로_갈아_끼운다")
    void 발행은_통째로_갈아_끼운다() {
        // 남기면 끝난 쿠폰이 스냅샷에 영영 남아, 각 노드가 없는 쿠폰을 계속 판정한다.
        port.publish(Map.of("c1", "a", "c2", "b")).block(WAIT);

        port.publish(Map.of("c1", "c")).block(WAIT);

        assertThat(port.load().block(WAIT)).containsOnlyKeys("c1");
    }
}

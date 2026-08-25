package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.coupon.QueueMode;
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
                RedisKeys.stock("c1"), RedisKeys.stock("c2"),
                RedisKeys.COUPON_POLICY).block(WAIT);
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
    @DisplayName("키로_못_쓰는_대상은_그것만_뺀다")
    void 키로_못_쓰는_대상은_그것만_뺀다() {
        // 밖에서 쓰는 키다. 못 쓰는 멤버 하나가 판을 죽이면 멀쩡한 쿠폰 전부의
        // 배분이 멎고, 사람이 목록을 고치기 전에는 안 풀린다.
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, "c1", "#credit", "a:b", "{x}").block(WAIT);

        assertThat(port.activeCoupons().block(WAIT)).containsExactly("c1");
    }

    @Test
    @DisplayName("샤드가_여럿이면_합쳐_센다")
    void 샤드가_여럿이면_합쳐_센다() {
        // 합산은 명령을 내는 쪽이 한다. 샤드를 하나라도 빠뜨리면 그 줄만큼
        // 크레딧이 덜 나가고, 줄 선 사람이 그만큼 오래 기다린다.
        int 샤드_넷 = 4;
        AllocationRedisPort 넷 = AllocationRedisPort.of(redis, 샤드_넷);
        for (int shard = 0; shard < 샤드_넷; shard++) {
            redis.opsForZSet().add(RedisKeys.queue("c1", 샤드_넷, shard), "m" + shard, shard)
                    .block(WAIT);
        }
        try {
            assertThat(넷.queueSizes(List.of("c1")).block(WAIT)).containsOnly(entry("c1", 4L));
        } finally {
            for (int shard = 0; shard < 샤드_넷; shard++) {
                redis.delete(RedisKeys.queue("c1", 샤드_넷, shard)).block(WAIT);
            }
        }
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

    private void 정책을_건다(String couponId, String json) {
        redis.opsForHash().put(RedisKeys.COUPON_POLICY, couponId, json).block(WAIT);
    }

    private Map<String, QueueMode> 정책(String... couponIds) {
        return port.queueModes(List.of(couponIds)).block(WAIT);
    }

    @Test
    @DisplayName("정책을_안_건_쿠폰은_비어_온다")
    void 정책을_안_건_쿠폰은_비어_온다() {
        // 없는 것은 고장이 아니다. 부르는 쪽이 기본값으로 채운다.
        assertThat(정책("c1", "c2")).isEmpty();
    }

    @Test
    @DisplayName("건_정책을_읽어_온다")
    void 건_정책을_읽어_온다() {
        정책을_건다("c1", "{\"mode\":\"ALWAYS\"}");
        // **소문자도 받는다.** 운영자가 손으로 넣는 값이라 대소문자를 못 믿는다.
        정책을_건다("c2", "{\"mode\":\"off\"}");

        assertThat(정책("c1", "c2"))
                .containsEntry("c1", QueueMode.ALWAYS)
                .containsEntry("c2", QueueMode.OFF);
    }

    @Test
    @DisplayName("모르는_필드는_무시한다")
    void 모르는_필드는_무시한다() {
        // 앞뒤 호환. Phase 10 이 같은 키에 shards 를 얹는다 (E-12).
        정책을_건다("c1", "{\"mode\":\"OFF\",\"shards\":4}");

        assertThat(정책("c1")).containsEntry("c1", QueueMode.OFF);
    }

    @Test
    @DisplayName("못_읽는_정책은_그_쿠폰만_뺀다")
    void 못_읽는_정책은_그_쿠폰만_뺀다() {
        // **판을 죽이지 않는다.** 운영자의 오타 하나가 전 쿠폰의 배분을 멈추면
        // 안 된다. 넷 다 예외가 나는 모양이 달라 하나로 못 묶는다.
        정책을_건다("c1", "{{");
        정책을_건다("c2", "{\"mode\":\"NOPE\"}");
        정책을_건다("c3", "{\"queueMode\":\"ALWAYS\"}");
        정책을_건다("c4", "\"ALWAYS\"");
        정책을_건다("c5", "{\"mode\":\"ADAPTIVE\"}");

        assertThat(정책("c1", "c2", "c3", "c4", "c5"))
                .containsExactly(entry("c5", QueueMode.ADAPTIVE));
    }

    @Test
    @DisplayName("활성_목록_밖의_정책은_안_읽는다")
    void 활성_목록_밖의_정책은_안_읽는다() {
        // 정책 해시에는 청소가 없어 끝난 쿠폰이 쌓인다. 통째로 받으면 매 틱
        // 그 전부를 파싱하고 몇 개만 쓴다.
        정책을_건다("c1", "{\"mode\":\"OFF\"}");
        정책을_건다("끝난쿠폰", "{\"mode\":\"ALWAYS\"}");

        assertThat(정책("c1")).containsExactly(entry("c1", QueueMode.OFF));
    }

    @Test
    @DisplayName("빈_목록이면_묻지_않는다")
    void 빈_목록이면_묻지_않는다() {
        // 빈 인자로 명령을 보내면 레디스가 오류를 낸다. 그 오류가 판을 죽인다.
        assertThat(port.queueModes(List.of()).block(WAIT)).isEmpty();
    }
}

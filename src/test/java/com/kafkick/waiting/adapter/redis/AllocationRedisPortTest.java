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

    /**
     * <b>줄과 생존 신호만 지웁니다</b> (7.3.1).
     *
     * <p>나머지는 지우면 되돌릴 수 없는 손해가 납니다. 입장 임계는 단조여야
     * 하고(A-7), 입장 표시는 차례가 왔던 사람이 종료를 안 받게 막는 유일한
     * 장치이며, 활성 목록에서 빼면 <b>매진 종결이 통째로 꺼집니다.</b>
     */
    @Test
    @DisplayName("매진_큐는_줄과_생존_신호만_지운다")
    void 매진_큐는_줄과_생존_신호만_지운다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, "c1", "c2").block(WAIT);
        줄_세운다("c1", 1, 2);
        줄_세운다("c2", 3);
        redis.opsForZSet().add(RedisKeys.alive("c1", SHARDS, 0), "m1", 100).block(WAIT);
        redis.opsForHash().put(RedisKeys.grace("c1", SHARDS, 0), "m1", "a:5").block(WAIT);
        redis.opsForValue().set(RedisKeys.admitted("c1", SHARDS, 0), "7").block(WAIT);

        port.dropSoldOutQueues(List.of("c1")).block(WAIT);

        assertThat(redis.hasKey(RedisKeys.queue("c1", SHARDS, 0)).block(WAIT)).isFalse();
        assertThat(redis.hasKey(RedisKeys.alive("c1", SHARDS, 0)).block(WAIT)).isFalse();
        // **입장 임계는 안 지운다.** 지우면 임계가 뒤로 가고, 이미 입장한
        // 사람이 두 번째 토큰을 받을 수 있다 (A-7).
        assertThat(redis.opsForValue().get(RedisKeys.admitted("c1", SHARDS, 0)).block(WAIT))
                .as("입장 임계").isEqualTo("7");
        // **입장 표시도 안 지운다.** 차례가 왔던 사람이 종료를 안 받게 막는
        // 유일한 장치이고 보관이 5분이다.
        assertThat(redis.opsForHash().get(RedisKeys.grace("c1", SHARDS, 0), "m1").block(WAIT))
                .as("입장 표시").isEqualTo("a:5");
        // **활성 목록에 남긴다.** 빼면 그 쿠폰이 스냅샷에서 사라져 조회는
        // 레디스로 내려가고, 발급은 404 가 되며, 재료가 낡으면 미지 쿠폰
        // 경로가 fail-open 으로 뒷단에 흘린다 — 사다리 1번을 우회한다.
        assertThat(redis.opsForSet().members(RedisKeys.ACTIVE_COUPONS)
                .collectList().block(WAIT)).containsExactlyInAnyOrder("c1", "c2");
        // 지목 안 한 쿠폰은 그대로다. 한 쿠폰의 정리가 옆 줄을 지우면 안 된다.
        assertThat(redis.hasKey(RedisKeys.queue("c2", SHARDS, 0)).block(WAIT)).isTrue();
    }

    /**
     * <b>시계 역행 바닥값은 살아남습니다.</b>
     *
     * <p>지우면 승격된 복제본의 시계가 뒤처졌을 때 새 score 가 앞으로 가고,
     * 줄 선 사람이 통째로 추월당합니다 (A-9).
     */
    @Test
    @DisplayName("정리해도_시계_바닥값은_남는다")
    void 정리해도_시계_바닥값은_남는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, "c1").block(WAIT);
        줄_세운다("c1", 1);
        redis.opsForValue().set(RedisKeys.maxScore("c1", SHARDS, 0), "1700000000000000")
                .block(WAIT);

        port.dropSoldOutQueues(List.of("c1")).block(WAIT);

        assertThat(redis.opsForValue().get(RedisKeys.maxScore("c1", SHARDS, 0)).block(WAIT))
                .isEqualTo("1700000000000000");
    }

    /** 지울 것이 없으면 아무 명령도 안 냅니다. 빈 목록에 왕복을 쓰면 틱이 밀립니다. */
    @Test
    @DisplayName("지울_것이_없으면_왕복하지_않는다")
    void 지울_것이_없으면_왕복하지_않는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, "c1").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of()).block(WAIT)).isZero();

        assertThat(redis.opsForSet().members(RedisKeys.ACTIVE_COUPONS)
                .collectList().block(WAIT)).containsExactly("c1");
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

    /** 읽기를 실패시킨다. 형이 다른 키를 놓으면 HMGET 이 WRONGTYPE 을 낸다. */
    private void 정책_읽기를_깨뜨린다() {
        redis.delete(RedisKeys.COUPON_POLICY).block(WAIT);
        redis.opsForValue().set(RedisKeys.COUPON_POLICY, "해시가-아니다").block(WAIT);
    }

    /**
     * <b>정책은 부가 정보다.</b> 이것 하나 때문에 판이 죽으면 대기 수와 재고가
     * 멀쩡해도 스냅샷이 안 나간다. 빈 판으로 접으면 전원이 적응형이 되어
     * ALWAYS 가 조용히 풀리므로 직전 값을 다시 쓴다.
     */
    @Test
    @DisplayName("읽기가_실패하면_직전_값으로_돈다")
    void 읽기가_실패하면_직전_값으로_돈다() {
        정책을_건다("c1", "{\"mode\":\"ALWAYS\"}");
        assertThat(정책("c1")).containsEntry("c1", QueueMode.ALWAYS);

        정책_읽기를_깨뜨린다();

        assertThat(정책("c1")).containsEntry("c1", QueueMode.ALWAYS);
    }

    /**
     * <b>이번 판에 안 물어본 쿠폰의 정책이 사라지면 안 된다.</b> 판마다 기억을
     * 통째로 갈아치우면, 그 쿠폰이 돌아왔을 때 읽기가 실패하는 순간 ALWAYS 가
     * 조용히 적응형이 된다 — 운영자가 켠 대기열이 안 켜진다.
     */
    @Test
    @DisplayName("안_물어본_쿠폰의_정책도_기억한다")
    void 안_물어본_쿠폰의_정책도_기억한다() {
        정책을_건다("c1", "{\"mode\":\"ALWAYS\"}");
        정책을_건다("c2", "{\"mode\":\"OFF\"}");
        정책("c1", "c2");
        // c1 이 활성 목록에서 빠진 판이 한 번 지난다.
        정책("c2");

        정책_읽기를_깨뜨린다();

        assertThat(정책("c1", "c2"))
                .containsEntry("c1", QueueMode.ALWAYS)
                .containsEntry("c2", QueueMode.OFF);
    }

    /** 정책을 지우면 그 자리는 비어야 한다. 기억이 옛 값을 붙들면 못 끈다. */
    @Test
    @DisplayName("지운_정책은_기억에서도_빠진다")
    void 지운_정책은_기억에서도_빠진다() {
        정책을_건다("c1", "{\"mode\":\"ALWAYS\"}");
        정책("c1");
        redis.opsForHash().remove(RedisKeys.COUPON_POLICY, "c1").block(WAIT);
        정책("c1");

        정책_읽기를_깨뜨린다();

        assertThat(정책("c1")).isEmpty();
    }

    @Test
    @DisplayName("빈_목록이면_묻지_않는다")
    void 빈_목록이면_묻지_않는다() {
        // 빈 인자로 명령을 보내면 레디스가 오류를 낸다. 그 오류가 판을 죽인다.
        assertThat(port.queueModes(List.of()).block(WAIT)).isEmpty();
    }

}

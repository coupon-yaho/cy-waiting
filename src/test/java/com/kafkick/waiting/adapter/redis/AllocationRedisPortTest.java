package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import com.kafkick.waiting.domain.allocation.Grant;
import com.kafkick.waiting.domain.coupon.QueueMode;
import java.time.Duration;
import com.kafkick.waiting.control.SnapshotCodec;
import java.util.LinkedHashMap;
import com.kafkick.waiting.control.QueueSweeper;
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
                RedisKeys.COUPON_POLICY,
                RedisKeys.alive("c1", SHARDS, 0), RedisKeys.grace("c1", SHARDS, 0),
                RedisKeys.stock("c3"), RedisKeys.maxScore("c1", SHARDS, 0),
                RedisKeys.dropFence("c1", SHARDS, 0)).block(WAIT);
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
        // 삭제가 재고를 직접 본다 (CY-765). 안 심으면 못 읽은 것이라 안 지운다.
        redis.opsForValue().set(RedisKeys.stock("c1"), "0").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of("c1"), 1).block(WAIT))
                .as("지운 쿠폰을 돌려준다").containsExactly("c1");

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
        redis.opsForValue().set(RedisKeys.stock("c1"), "0").block(WAIT);

        port.dropSoldOutQueues(List.of("c1"), 1).block(WAIT);

        assertThat(redis.opsForValue().get(RedisKeys.maxScore("c1", SHARDS, 0)).block(WAIT))
                .isEqualTo("1700000000000000");
    }

    /** 지울 것이 없으면 아무 명령도 안 냅니다. 빈 목록에 왕복을 쓰면 틱이 밀립니다. */
    @Test
    @DisplayName("지울_것이_없으면_왕복하지_않는다")
    void 지울_것이_없으면_왕복하지_않는다() {
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, "c1").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of(), 1).block(WAIT)).isEmpty();

        assertThat(redis.opsForSet().members(RedisKeys.ACTIVE_COUPONS)
                .collectList().block(WAIT)).containsExactly("c1");
    }

    /**
     * <b>생존 신호가 없는 사람을 걷습니다</b> (7.4.4).
     *
     * <p>이탈자가 줄에 남으면 배분이 그 자리에 크레딧을 허공에 발행합니다.
     */
    @Test
    @DisplayName("생존_신호가_없으면_걷는다")
    void 생존_신호가_없으면_걷는다() {
        long 지금 = 1_700_000_000L;
        줄_세운다("c1", 1, 2, 3);
        redis.opsForZSet().add(RedisKeys.alive("c1", SHARDS, 0), "m1", 지금 + 60).block(WAIT);

        QueueSweeper.SweepResult 결과 =
                port.sweep(List.of("c1"), 지금, 100, 300, 100).block(WAIT);

        assertThat(결과.swept()).as("걷은 수").isEqualTo(2);
        assertThat(redis.opsForZSet().size(RedisKeys.queue("c1", SHARDS, 0)).block(WAIT))
                .as("남은 줄").isEqualTo(1);
        // **이탈 기록을 남깁니다.** 안 남기면 돌아온 사람을 알아볼 방법이 없고
        // 유예 재입장(7.5)이 설 자리가 없습니다.
        assertThat(redis.opsForHash().size(RedisKeys.grace("c1", SHARDS, 0)).block(WAIT))
                .as("이탈 기록").isEqualTo(2);
    }

    /**
     * <b>전부 만료된 신호는 "살아 있다" 가 아닙니다.</b>
     *
     * <p>만료된 항목은 정리가 걷기 전까지 물리적으로 남아 있습니다. 개수만
     * 보면 회복 첫 판이 "신호가 있다" 로 읽히고, 그 판에서 앞줄이 통째로 걷힙니다.
     */
    @Test
    @DisplayName("전부_만료된_신호로는_앞줄을_안_걷는다")
    void 전부_만료된_신호로는_앞줄을_안_걷는다() {
        long 지금 = 1_700_000_000L;
        줄_세운다("c1", 1, 2, 3);
        for (int i = 1; i <= 3; i++) {
            redis.opsForZSet().add(RedisKeys.alive("c1", SHARDS, 0), "m" + i, 지금 - 10)
                    .block(WAIT);
        }

        QueueSweeper.SweepResult 결과 =
                port.sweep(List.of("c1"), 지금, 100, 300, 100).block(WAIT);

        assertThat(결과.swept()).as("걷은 수").isZero();
        assertThat(redis.opsForZSet().size(RedisKeys.queue("c1", SHARDS, 0)).block(WAIT))
                .as("줄이 그대로").isEqualTo(3);
        // **정리까지 건너뛰지는 않습니다.** 앞줄 제거만 접고 만료 신호는 걷습니다.
        assertThat(결과.expiredSignals()).as("만료 신호는 걷는다").isEqualTo(3);
    }

    /**
     * <b>신호가 통째로 없으면 아무것도 안 걷습니다.</b>
     *
     * <p>줄에 사람이 있는데 생존 신호가 하나도 없다는 것은 "전원이 떠났다" 가
     * 아니라 <b>그 저장소를 잃었다</b> 는 뜻입니다 — 승격된 복제본, AOF 유실,
     * 매진 구간이 길어져 폴링이 멎은 뒤의 회복이 전부 이 모양입니다.
     */
    @Test
    @DisplayName("생존_신호가_통째로_없으면_안_걷는다")
    void 생존_신호가_통째로_없으면_안_걷는다() {
        long 지금 = 1_700_000_000L;
        줄_세운다("c1", 1, 2, 3);

        QueueSweeper.SweepResult 결과 =
                port.sweep(List.of("c1"), 지금, 100, 300, 100).block(WAIT);

        assertThat(결과.swept()).as("걷은 수").isZero();
        assertThat(redis.opsForZSet().size(RedisKeys.queue("c1", SHARDS, 0)).block(WAIT))
                .as("줄이 그대로").isEqualTo(3);
    }

    /**
     * <b>차례가 온 사람은 안 걷습니다.</b>
     *
     * <p>배분은 임계만 올리고 큐에서 빼지 않습니다 — 빼는 것은 그 사람이
     * 폴링할 때입니다. 그래서 앞줄에는 "입장 확정인데 아직 안 걷어간 사람" 이
     * 섞이고, 걷으면 그가 다음 폴링에서 종료를 받습니다 (불변식 4).
     */
    @Test
    @DisplayName("입장_임계_안의_사람은_안_걷는다")
    void 입장_임계_안의_사람은_안_걷는다() {
        long 지금 = 1_700_000_000L;
        줄_세운다("c1", 1, 5);
        // 한 명은 살아 있다고 말해 "신호가 통째로 없다" 가드를 지납니다.
        redis.opsForZSet().add(RedisKeys.alive("c1", SHARDS, 0), "m5", 지금 + 60).block(WAIT);
        redis.opsForValue().set(RedisKeys.admitted("c1", SHARDS, 0), "1").block(WAIT);

        QueueSweeper.SweepResult 결과 =
                port.sweep(List.of("c1"), 지금, 100, 300, 100).block(WAIT);

        assertThat(결과.swept()).as("걷은 수").isZero();
        assertThat(redis.opsForZSet().score(RedisKeys.queue("c1", SHARDS, 0), "m1").block(WAIT))
                .as("입장 확정자가 줄에 남는다").isEqualTo(1.0);
    }

    /**
     * <b>차례가 온 사람의 입장 표시는 안 덮습니다.</b>
     *
     * <p>덮으면 그 사람이 다음 폴링에서 종료를 받고, 다시 서면 그동안 온 사람
     * 뒤로 갑니다 — `queue_status` 가 그 표시 하나로 막고 있는 것이 그것입니다.
     */
    // **임계 아래여야 이 성질이 성립합니다.** 임계 위의 표시는 지난 판의 것이라
    // 낡음이 증명되고, 그때는 덮는 것이 맞는 답입니다 — 안 덮고 건너뛰면 임계가
    // 그를 지나가 창 밖이 되고 그 뒤로 영영 안 걷힙니다.
    @Test
    @DisplayName("차례가_온_사람의_입장_표시는_안_덮는다")
    void 차례가_온_사람의_입장_표시는_안_덮는다() {
        long 지금 = 1_700_000_000L;
        줄_세운다("c1", 1, 5);
        // m5 는 살아 있어 "신호 전무" 가드를 지나고, m1 은 만료됐다.
        redis.opsForZSet().add(RedisKeys.alive("c1", SHARDS, 0), "m5", 지금 + 60).block(WAIT);
        redis.opsForHash().put(RedisKeys.grace("c1", SHARDS, 0), "m1", "a:" + 지금).block(WAIT);
        // 임계를 m1 자리까지 올린다 — 차례가 온 사람이다.
        redis.opsForValue().set(RedisKeys.admitted("c1", SHARDS, 0), "1").block(WAIT);

        QueueSweeper.SweepResult 결과 =
                port.sweep(List.of("c1"), 지금, 100, 300, 100).block(WAIT);

        assertThat(결과.swept()).as("차례가 온 사람은 안 걷는다").isZero();
        assertThat(redis.opsForZSet().score(RedisKeys.queue("c1", SHARDS, 0), "m1").block(WAIT))
                .as("줄에 순번까지 그대로").isEqualTo(1.0);
        assertThat(redis.opsForHash().get(RedisKeys.grace("c1", SHARDS, 0), "m1").block(WAIT))
                .as("입장 표시가 살아남는다").isEqualTo("a:" + 지금);
    }

    /**
     * <b>임계 위의 입장 표시는 낡은 값이라 덮고 걷습니다.</b>
     *
     * <p>지금 차례가 온 사람은 임계 아래라 창에 없습니다. 그러니 창 안의 표시는
     * 지난 판의 것이고, 이탈 기록으로 덮는 것이 맞는 답입니다 — 그 사람이 다시
     * 오면 재방문자입니다.
     */
    // **표시는 이탈 기록으로 덮습니다.** 큐에서만 빼고 남기면 다음 폴링이
    // 입장이라고 답합니다 — 차례가 안 왔는데 입장이라 줄 전체를 추월합니다.
    @Test
    @DisplayName("임계_위의_입장_표시는_덮고_걷는다")
    void 임계_위의_입장_표시는_덮고_걷는다() {
        long 지금 = 1_700_000_000L;
        줄_세운다("c1", 1, 5);
        // m5 만 살아 있다. m1 은 만료됐고 입장 표시를 들고 있다.
        redis.opsForZSet().add(RedisKeys.alive("c1", SHARDS, 0), "m5", 지금 + 60).block(WAIT);
        redis.opsForHash().put(RedisKeys.grace("c1", SHARDS, 0), "m1", "a:" + 지금).block(WAIT);

        QueueSweeper.SweepResult 결과 =
                port.sweep(List.of("c1"), 지금, 100, 300, 100).block(WAIT);

        assertThat(결과.failed()).as("실패").isZero();
        assertThat(결과.swept()).as("걷은 수").isOne();
        assertThat(redis.opsForZSet().score(RedisKeys.queue("c1", SHARDS, 0), "m1").block(WAIT))
                .as("큐에서 빠진다").isNull();
        assertThat(redis.opsForHash().get(RedisKeys.grace("c1", SHARDS, 0), "m1").block(WAIT))
                .as("이탈 기록으로 덮인다").isEqualTo("d:" + 지금);
    }

    /**
     * <b>보관 기간 안의 입장 표시는 청소를 견딥니다.</b>
     *
     * <p>같은 해시에 writer 가 둘입니다. 종류를 안 가르면 청소가 입장 표시를
     * 낡은 것으로 보고 지우고, 입장한 사람이 다음 폴링에서 종료를 받습니다.
     */
    @Test
    @DisplayName("보관_기간_안의_입장_표시는_청소를_견딘다")
    void 보관_기간_안의_입장_표시는_청소를_견딘다() {
        long 지금 = 1_700_000_000L;
        줄_세운다("c1", 1);
        redis.opsForZSet().add(RedisKeys.alive("c1", SHARDS, 0), "m1", 지금 + 60).block(WAIT);
        redis.opsForHash().put(RedisKeys.grace("c1", SHARDS, 0), "m9", "a:" + 지금).block(WAIT);

        port.sweep(List.of("c1"), 지금 + 100, 100, 300, 100).block(WAIT);

        assertThat(redis.opsForHash().get(RedisKeys.grace("c1", SHARDS, 0), "m9").block(WAIT))
                .isEqualTo("a:" + 지금);
    }

    /** 쓸 것이 없으면 아무 명령도 안 냅니다. 틱마다 도는 자리입니다. */
    @Test
    @DisplayName("쓸_것이_없으면_왕복하지_않는다")
    void 쓸_것이_없으면_왕복하지_않는다() {
        assertThat(port.sweep(List.of(), 1_700_000_000L, 100, 300, 100).block(WAIT))
                .isEqualTo(QueueSweeper.SweepResult.NOTHING);
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

        // **못 읽으면 아예 안 담는다.** 부르는 쪽이 그 빈자리를 미상으로 싣는다
        // — 0 으로 접으면 재고 키를 잃은 쿠폰이 매진이 된다 (CY-702).
        //
        // 수가 아닌 값도 여기서 빠진다. 그건 대개 안 낫는 손상이라 그 쿠폰이
        // 영영 미상으로 남는다 — 종결도 정리도 청소 재개도 안 온다. 지금은
        // 그것을 감수한다. 잘못 읽은 수로 재고를 판단하는 쪽이 더 나쁘다.
        redis.opsForValue().set(RedisKeys.stock("c3"), "몇 개더라").block(WAIT);

        assertThat(port.stocks(List.of("c1", "c2", "c3")).block(WAIT))
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
        port.publish(Map.of("c1", "OFF:QUEUEING:1:10:5", "#credit", "7")).block(WAIT);

        assertThat(port.load().block(WAIT))
                .containsEntry("#credit", "7")
                .containsEntry("c1", "OFF:QUEUEING:1:10:5");
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

    /**
     * <b>상한을 넘으면 미상 표시부터 버리고 발행한다.</b>
     *
     * <p>표시는 쿠폰마다 필드를 하나 더 쓰므로 실을 수 있는 쿠폰이 절반이 된다.
     * 그런데 그 두 배가 되는 순간은 재고를 통째로 못 읽는 순간이라, 하필 그때
     * 발행이 죽는다 — 전 노드가 낡음으로 넘어가고 정리도 청소도 같이 멎는다.
     */
    // 표시를 잃으면 그 쿠폰이 거짓 매진으로 읽힌다. 나쁘지만 스냅샷이 아예
    // 안 나가는 것보다 낫다 — 옛 노드가 오늘 하는 것과 같은 자리다.
    @Test
    @DisplayName("상한을_넘으면_미상_표시부터_버린다")
    void 상한을_넘으면_미상_표시부터_버린다() {
        Map<String, String> 큰_판 = new LinkedHashMap<>();
        for (int i = 0; i < 1_600; i++) {
            큰_판.put("c" + i, "OFF:QUEUEING:1:0:5:1.0");
            큰_판.put(SnapshotCodec.STOCK_UNKNOWN_FIELD + "c" + i, "1");
        }

        port.publish(큰_판).block(WAIT);

        Map<String, String> 실린것 = port.load().block(WAIT);
        assertThat(실린것).as("쿠폰은 다 실린다").containsKey("c1599");
        assertThat(실린것).as("상한까지 채워 싣는다").hasSize(3_000);
        // **필요한 만큼만 버린다.** 하나를 넘었다고 전부 버리면 안 버려도 될
        // 쿠폰까지 거짓 매진이 되고, 그 하나하나가 줄을 잃는 경로를 탄다.
        assertThat(실린것.keySet().stream()
                .filter(f -> f.startsWith(SnapshotCodec.STOCK_UNKNOWN_FIELD)).count())
                .as("남길 수 있는 표시는 남긴다").isEqualTo(1_400);
        assertThat(port.markersDropped()).as("버린 사실이 지표로 남는다").isEqualTo(200);
    }

    /**
     * <b>줄이 빈 쿠폰의 표시부터 버린다.</b> 그 표시를 잃으면 신규 유입만
     * 거절되고 다음 판이 되돌린다. 줄이 선 쿠폰의 표시를 잃으면 그 줄이 통째로
     * 종결로 읽히고, 되돌릴 방법이 없다.
     */
    @Test
    @DisplayName("줄이_빈_쿠폰의_표시부터_버린다")
    void 줄이_빈_쿠폰의_표시부터_버린다() {
        Map<String, String> 큰_판 = new LinkedHashMap<>();
        for (int i = 0; i < 1_600; i++) {
            // 짝수만 줄이 서 있다. 버릴 것은 홀수 쪽에서 다 나와야 한다.
            큰_판.put("c" + i, "OFF:QUEUEING:1:0:" + (i % 2 == 0 ? 5 : 0) + ":1.0");
            큰_판.put(SnapshotCodec.STOCK_UNKNOWN_FIELD + "c" + i, "1");
        }

        port.publish(큰_판).block(WAIT);

        Map<String, String> 실린것 = port.load().block(WAIT);
        assertThat(실린것.keySet().stream()
                .filter(f -> f.startsWith(SnapshotCodec.STOCK_UNKNOWN_FIELD))
                .filter(f -> Integer.parseInt(f.substring(4)) % 2 == 0).count())
                .as("줄이 선 쿠폰의 표시는 하나도 안 버린다").isEqualTo(800);
    }

    /** 표시를 다 버려도 안 되면 그때는 실패다. 잘라 실으면 원자성이 깨진다. */
    @Test
    @DisplayName("표시를_버려도_상한을_넘으면_실패한다")
    void 표시를_버려도_상한을_넘으면_실패한다() {
        // **표시를 다 달아 둔다.** 표시가 없으면 버릴 것이 없어서 실패하는
        // 것이라, 이 시험이 말하는 "다 버려도 안 된다" 를 한 번도 안 밟는다.
        Map<String, String> 큰_판 = new LinkedHashMap<>();
        for (int i = 0; i < 3_100; i++) {
            큰_판.put("c" + i, "OFF:QUEUEING:1:0:5:1.0");
            큰_판.put(SnapshotCodec.STOCK_UNKNOWN_FIELD + "c" + i, "1");
        }

        assertThatThrownBy(() -> port.publish(큰_판).block(WAIT))
                .isInstanceOf(IllegalStateException.class);

        // **안 나간 판은 거짓 매진을 안 만든다.** 여기서 세면 지표가 "표시를
        // 버려 매진으로 읽힌 쿠폰" 이 아니라 "버리려고 시도한 횟수" 가 되고,
        // 같은 판이 매 틱 실패하는 구간에서 그 수가 끝없이 부푼다.
        assertThat(port.markersDropped()).as("안 나간 판은 안 센다").isZero();
    }

    /** 상한과 같으면 그대로 싣는다. 하나 넘어야 버리기 시작한다. */
    @Test
    @DisplayName("상한과_같으면_표시를_안_버린다")
    void 상한과_같으면_표시를_안_버린다() {
        Map<String, String> 판 = new LinkedHashMap<>();
        for (int i = 0; i < 1_500; i++) {
            판.put("c" + i, "OFF:QUEUEING:1:0:5:1.0");
            판.put(SnapshotCodec.STOCK_UNKNOWN_FIELD + "c" + i, "1");
        }

        port.publish(판).block(WAIT);

        assertThat(port.load().block(WAIT)).hasSize(3_000);
        assertThat(port.markersDropped()).isZero();
    }

    /**
     * <b>짝 없는 표시가 와도 안 터진다.</b> 발행은 아무 맵이나 받으므로 쿠폰
     * 값이 없는 표시가 들어올 수 있다. 그때는 줄이 선 것으로 보고 덜 버린다.
     */
    @Test
    @DisplayName("짝_없는_표시는_덜_버리는_쪽으로_친다")
    void 짝_없는_표시는_덜_버리는_쪽으로_친다() {
        Map<String, String> 판 = new LinkedHashMap<>();
        판.put(SnapshotCodec.STOCK_UNKNOWN_FIELD + "orphan", "1");
        판.put("broken", "OFF:QUEUEING");
        판.put(SnapshotCodec.STOCK_UNKNOWN_FIELD + "broken", "1");
        for (int i = 0; i < 1_500; i++) {
            판.put("c" + i, "OFF:QUEUEING:1:0:0:1.0");
            판.put(SnapshotCodec.STOCK_UNKNOWN_FIELD + "c" + i, "1");
        }

        port.publish(판).block(WAIT);

        Map<String, String> 실린것 = port.load().block(WAIT);
        assertThat(실린것).as("줄이 빈 쪽부터 버려 상한을 맞춘다").hasSize(3_000);
        assertThat(실린것).as("짝 없는 표시는 남긴다")
                .containsKey(SnapshotCodec.STOCK_UNKNOWN_FIELD + "orphan")
                .containsKey(SnapshotCodec.STOCK_UNKNOWN_FIELD + "broken");
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
        redis.delete(RedisKeys.COUPON_POLICY,
                RedisKeys.alive("c1", SHARDS, 0), RedisKeys.grace("c1", SHARDS, 0),
                RedisKeys.stock("c3"), RedisKeys.maxScore("c1", SHARDS, 0),
                RedisKeys.dropFence("c1", SHARDS, 0)).block(WAIT);
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

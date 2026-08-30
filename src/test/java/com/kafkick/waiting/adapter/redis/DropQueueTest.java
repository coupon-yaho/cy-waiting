package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * 매진 큐의 실제 삭제 (7.3 · 5.3.1).
 *
 * <p><b>이 저장소에서 되돌릴 수 없는 유일한 쓰기다.</b> 지운 줄을 되살리는
 * 코드가 없으므로, 지우기 직전에 재고를 다시 보고 살아난 줄은 안 지운다.
 */
@Tag("integration")
@SpringBootTest
class DropQueueTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String COUPON = "drop-me";

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private AllocationRedisPort port;

    @BeforeEach
    void 준비() {
        port = AllocationRedisPort.of(redis, 1);
        redis.delete(RedisKeys.queue(COUPON, 1, 0), RedisKeys.alive(COUPON, 1, 0),
                RedisKeys.stock(COUPON), RedisKeys.admitted(COUPON, 1, 0),
                RedisKeys.grace(COUPON, 1, 0), RedisKeys.maxScore(COUPON, 1, 0),
                RedisKeys.queue(OTHER, 1, 0), RedisKeys.stock(OTHER),
                RedisKeys.dropFence(COUPON, 1, 0), RedisKeys.dropFence(OTHER, 1, 0))
                .block(WAIT);
    }

    /** 다른 쿠폰 키도 준비에서 지운다 — 시험 끝에 지우면 실패한 판이 찌꺼기를 남긴다. */
    private static final String OTHER = "alive-one";

    /** 이 시험들이 쓰는 판 번호. 리더가 리스를 새로 잡을 때 받는 값이다. */
    private static final long FENCE = 100;

    private void 줄을_세운다() {
        redis.opsForZSet().add(RedisKeys.queue(COUPON, 1, 0), "m1", 100).block(WAIT);
        redis.opsForZSet().add(RedisKeys.alive(COUPON, 1, 0), "m1", 200).block(WAIT);
        redis.opsForValue().set(RedisKeys.admitted(COUPON, 1, 0), "50").block(WAIT);
        redis.opsForHash().put(RedisKeys.grace(COUPON, 1, 0), "m0", "a:100").block(WAIT);
        redis.opsForValue().set(RedisKeys.maxScore(COUPON, 1, 0), "999").block(WAIT);
    }

    private boolean 있나(String key) {
        return Boolean.TRUE.equals(redis.hasKey(key).block(WAIT));
    }

    @Test
    @DisplayName("매진된_큐를_지운다")
    void 매진된_큐를_지운다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT)).containsExactly(COUPON);

        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).as("줄").isFalse();
        assertThat(있나(RedisKeys.alive(COUPON, 1, 0))).as("생존 신호").isFalse();
    }

    /**
     * <b>임계·유예·바닥값은 남긴다</b> (5.3 표). 임계를 지우면 이미 입장한
     * 사람이 두 번째 토큰을 받고, 바닥값을 지우면 새 score 가 앞으로 가
     * 줄 선 사람이 통째로 추월당한다.
     */
    @Test
    @DisplayName("지우는_것은_줄과_생존_신호뿐이다")
    void 지우는_것은_줄과_생존_신호뿐이다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        port.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT);

        assertThat(있나(RedisKeys.admitted(COUPON, 1, 0))).as("입장 임계").isTrue();
        assertThat(있나(RedisKeys.grace(COUPON, 1, 0))).as("유예 기록").isTrue();
        assertThat(있나(RedisKeys.maxScore(COUPON, 1, 0))).as("시계 바닥값").isTrue();
        // **재고 키는 발급 계층 소유다.** 여기서 지우면 그 뒤로 재고가 영영
        // 미상이 되고 매진 종결이 통째로 꺼진다. 값으로 봐야 잡힌다.
        assertThat(redis.opsForValue().get(RedisKeys.stock(COUPON)).block(WAIT))
                .as("남의 키를 안 건드린다").isEqualTo("0");
    }

    /**
     * <b>지우기 직전에 재고를 다시 본다</b> (5.3.1 · CY-765).
     *
     * <p>수집과 삭제 사이에 재입고되면 살아난 줄을 지운다. 메모리 안의 취소는
     * 다음 스냅샷이 와야 도는데 삭제는 그 전에 나간다.
     */
    @Test
    @DisplayName("재입고됐으면_안_지운다")
    void 재입고됐으면_안_지운다() {
        줄을_세운다();
        // 판이 시작될 때는 매진이었고, 지우기 직전에 재고가 돌아왔다.
        redis.opsForValue().set(RedisKeys.stock(COUPON), "5").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT))
                .as("지운 것만 돌려준다").isEmpty();

        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).as("줄이 살아 있다").isTrue();
        assertThat(있나(RedisKeys.alive(COUPON, 1, 0))).as("생존 신호도").isTrue();
    }

    /**
     * <b>재고를 못 읽으면 안 지운다.</b> 못 읽은 것은 매진이 아니다 (CY-702).
     * 여기서 지우면 재고 키를 잃은 쿠폰의 줄이 통째로 사라진다.
     */
    @Test
    @DisplayName("재고를_못_읽으면_안_지운다")
    void 재고를_못_읽으면_안_지운다() {
        줄을_세운다();
        // 재고 키가 없다.

        assertThat(port.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT)).isEmpty();

        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).as("줄이 살아 있다").isTrue();
    }

    /**
     * <b>재고를 읽는 두 곳이 같은 답을 내야 한다.</b> 재고를 담는 쪽은
     * {@code Long.parseLong} 이라 지수 표기나 소수를 못 읽는데, Lua 의
     * {@code tonumber} 는 읽는다. 갈리면 수집이 "미상" 이라 부른 값으로
     * 삭제가 "매진" 이라 판단해 그 줄을 영영 지운다.
     */
    @Test
    @DisplayName("재고가_수가_아니면_안_지운다")
    void 재고가_수가_아니면_안_지운다() {
        for (String 못_읽는_값 : List.of("몇 개더라", "-1e20", "-0.5", "-inf", "", " ", "0x10",
                "-99999999999999999999")) {
            줄을_세운다();
            redis.opsForValue().set(RedisKeys.stock(COUPON), 못_읽는_값).block(WAIT);

            assertThat(port.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT))
                    .as("못 읽는 값: %s", 못_읽는_값).isEmpty();
            assertThat(있나(RedisKeys.queue(COUPON, 1, 0)))
                    .as("줄이 남아야 한다: %s", 못_읽는_값).isTrue();
        }
    }

    /**
     * 담는 쪽이 읽는 모양은 <b>전부</b> 여기서도 읽혀야 한다.
     *
     * <p>한쪽만 못 읽으면 그 쿠폰이 매진으로 보이면서 영영 안 지워진다 —
     * 죽은 줄이 폴링 예산을 계속 먹는다.
     */
    @Test
    @DisplayName("담는_쪽이_읽는_모양은_다_지운다")
    void 담는_쪽이_읽는_모양은_다_지운다() {
        for (String 읽히는_값 : List.of("0", "-3", "+0", " 0 ")) {
            줄을_세운다();
            redis.opsForValue().set(RedisKeys.stock(COUPON), 읽히는_값).block(WAIT);

            assertThat(port.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT))
                    .as("읽히는 값: %s", 읽히는_값).containsExactly(COUPON);
        }
    }

    /**
     * <b>샤드가 여럿이면 지우지 않는다.</b>
     *
     * <p>재고는 샤드 무관 키라 샤딩을 켜면 줄과 슬롯이 갈린다. 클러스터는 실행
     * 전에 거절하고, 단독 배치는 받아 주지만 샤드 0 만 지운다.
     */
    // 둘 다 조용하다. 그래서 여기서 소리 나게 막고, 재고 세대를 두는 형태로
    // 갈아탄 뒤에 푼다 (5.3.1).
    @Test
    @DisplayName("샤드가_여럿이면_지우지_않는다")
    void 샤드가_여럿이면_지우지_않는다() {
        AllocationRedisPort 샤딩 = AllocationRedisPort.of(redis, 2);

        assertThatThrownBy(() -> 샤딩.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT))
                .hasMessageContaining("샤드");
    }

    /**
     * <b>거절과 고장을 가른다.</b> 포트는 스크립트 오류를 삼켜 "안 지웠다" 로
     * 바꾸므로, 포트를 거쳐서는 둘이 완전히 같아 보인다 — 스크립트가 통째로
     * 깨져도 정리가 조용히 멎고 시험은 초록이다.
     */
    @Test
    @DisplayName("안_지울_때는_터지지_않고_0을_돌려준다")
    void 안_지울_때는_터지지_않고_0을_돌려준다() {
        줄을_세운다();
        RedisScript<Long> script =
                RedisScript.of(new ClassPathResource("redis/drop_queue.lua"), Long.class);
        List<String> keys = List.of(RedisKeys.queue(COUPON, 1, 0),
                RedisKeys.alive(COUPON, 1, 0), RedisKeys.stock(COUPON),
                RedisKeys.dropFence(COUPON, 1, 0));
        List<String> args = List.of(Long.toString(FENCE), "1", "60000");

        // 재고 키가 없다 — 못 읽은 것이라 안 지운다.
        assertThat(redis.execute(script, keys, args).blockFirst(WAIT))
                .as("키가 없을 때").isZero();

        redis.opsForValue().set(RedisKeys.stock(COUPON), "몇 개더라").block(WAIT);
        assertThat(redis.execute(script, keys, args).blockFirst(WAIT))
                .as("수가 아닐 때").isZero();

        redis.opsForValue().set(RedisKeys.stock(COUPON), "5").block(WAIT);
        assertThat(redis.execute(script, keys, args).blockFirst(WAIT))
                .as("재입고됐을 때").isZero();
    }

    /** 한 판에 여럿을 지운다. 하나만 지우는 구현도 한 쿠폰짜리 시험은 통과한다. */
    @Test
    @DisplayName("한_판에_여러_줄을_지운다")
    void 한_판에_여러_줄을_지운다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);
        redis.opsForZSet().add(RedisKeys.queue(OTHER, 1, 0), "m1", 100).block(WAIT);
        redis.opsForValue().set(RedisKeys.stock(OTHER), "0").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of(OTHER, COUPON), FENCE).block(WAIT))
                .containsExactlyInAnyOrder(OTHER, COUPON);

        assertThat(있나(RedisKeys.queue(OTHER, 1, 0))).isFalse();
        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).isFalse();
    }

    /**
     * <b>지역 검사는 울타리가 아니다</b> (5.3.1 · CY-766).
     *
     * <p>검사와 삭제 사이에 리스가 끝나면 옛 리더가 새 리더의 줄을 지운다.
     * 리더 키는 줄과 다른 슬롯이라 스크립트가 같이 못 읽는다 — 그래서 줄과
     * 같은 슬롯에 <b>울타리 표</b>를 두고 그것을 비교한다.
     */
    @Test
    @DisplayName("옛_리더의_삭제는_거부한다")
    void 옛_리더의_삭제는_거부한다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        // 새 리더가 먼저 지웠다 — 울타리가 그 판을 기억한다.
        assertThat(port.dropSoldOutQueues(List.of(COUPON), 200).block(WAIT))
                .containsExactly(COUPON);
        줄을_세운다();

        // 리스가 끝난 줄 모르는 옛 리더가 뒤늦게 같은 명령을 낸다.
        assertThat(port.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT))
                .as("옛 판의 명령은 안 듣는다").isEmpty();
        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).as("줄이 살아 있다").isTrue();
    }

    /**
     * <b>한 번도 안 지운 줄도 막는다</b> (CY-766).
     *
     * <p>표를 지웠을 때만 세우면 아직 안 지운 줄에는 표가 없고, 그건 울타리가
     * 지키려던 바로 그 경우다 — 얼었다 깨어난 옛 리더가 새 리더가 아직 지울
     * 생각이 없는 줄을 지운다. 후보로 올리는 순간 표를 세워야 걸린다.
     */
    @Test
    @DisplayName("새_리더가_세운_표가_옛_판을_막는다")
    void 새_리더가_세운_표가_옛_판을_막는다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        // 새 리더가 유예를 세기 시작했다 — 아직 안 지웠고 표만 세웠다.
        port.claimSoldOutQueues(List.of(COUPON), 200).block(WAIT);
        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).as("표만 세운다").isTrue();

        // 얼었다 깨어난 옛 리더가 자기 유예를 다 세고 지우러 온다.
        assertThat(port.dropSoldOutQueues(List.of(COUPON), FENCE).block(WAIT))
                .as("옛 판은 못 지운다").isEmpty();
        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).as("줄이 살아 있다").isTrue();

        // 새 리더가 유예를 다 세면 지운다.
        assertThat(port.dropSoldOutQueues(List.of(COUPON), 200).block(WAIT))
                .containsExactly(COUPON);
    }

    /**
     * <b>판 번호 0 은 리더가 아니라는 뜻이다.</b> 강등된 노드가 그 값을 들고
     * 나오므로 지우지도, 표를 세우지도 않는다.
     */
    @Test
    @DisplayName("판_번호가_0이면_안_지운다")
    void 판_번호가_0이면_안_지운다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        assertThat(port.dropSoldOutQueues(List.of(COUPON), 0).block(WAIT)).isEmpty();

        assertThat(있나(RedisKeys.queue(COUPON, 1, 0))).as("줄이 살아 있다").isTrue();
        assertThat(있나(RedisKeys.dropFence(COUPON, 1, 0))).as("표도 안 세운다").isFalse();
    }

    /** 큰 판 번호도 자리 수 그대로 남는다. 지수 표기로 굳으면 비교가 흔들린다. */
    @Test
    @DisplayName("큰_판_번호도_그대로_남는다")
    void 큰_판_번호도_그대로_남는다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        port.claimSoldOutQueues(List.of(COUPON), 1_756_500_123_456_789L).block(WAIT);

        assertThat(redis.opsForValue().get(RedisKeys.dropFence(COUPON, 1, 0)).block(WAIT))
                .isEqualTo("1756500123456789");
    }

    /** 표에는 수명이 있다. 없으면 쿠폰이 끝난 뒤에도 남아 새 리더를 막는다. */
    @Test
    @DisplayName("표에는_수명이_있다")
    void 표에는_수명이_있다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        port.claimSoldOutQueues(List.of(COUPON), 200).block(WAIT);

        assertThat(redis.getExpire(RedisKeys.dropFence(COUPON, 1, 0)).block(WAIT))
                .as("주인도 만료도 없는 키를 안 남긴다").isPositive();
    }

    /** 같은 리더가 다시 내는 것은 막지 않는다. 막으면 재시도가 영영 안 된다. */
    @Test
    @DisplayName("같은_판의_재시도는_지운다")
    void 같은_판의_재시도는_지운다() {
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);
        port.dropSoldOutQueues(List.of(COUPON), 200).block(WAIT);
        줄을_세운다();

        assertThat(port.dropSoldOutQueues(List.of(COUPON), 200).block(WAIT))
                .containsExactly(COUPON);
    }

    /** 한 쿠폰이 살아나도 나머지는 지운다. 판 하나가 통째로 멎으면 안 된다. */
    @Test
    @DisplayName("살아난_쿠폰만_건너뛴다")
    void 살아난_쿠폰만_건너뛴다() {
        String 산것 = OTHER;
        redis.opsForZSet().add(RedisKeys.queue(산것, 1, 0), "m1", 100).block(WAIT);
        redis.opsForValue().set(RedisKeys.stock(산것), "7").block(WAIT);
        줄을_세운다();
        redis.opsForValue().set(RedisKeys.stock(COUPON), "0").block(WAIT);

        // **순서를 안 본다.** 구현이 동시에 여러 쿠폰을 태우므로 순서가
        // 보장되지 않는다 — 지금은 원소가 하나라 우연히 결정적일 뿐이다.
        assertThat(port.dropSoldOutQueues(List.of(산것, COUPON), FENCE).block(WAIT))
                .containsExactlyInAnyOrder(COUPON);

        assertThat(있나(RedisKeys.queue(산것, 1, 0))).as("살아난 쪽은 그대로").isTrue();
    }
}

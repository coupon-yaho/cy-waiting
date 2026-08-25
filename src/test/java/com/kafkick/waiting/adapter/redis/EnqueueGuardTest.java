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
 * 등록의 생존 신호와 길이 상한.
 *
 * <p>상한은 <b>2차 방어</b>다. 1차는 도메인이 보지만 낡은 스냅샷으로 판정하므로
 * 여기서 한 번 더 본다.
 */
@Tag("integration")
@SpringBootTest
class EnqueueGuardTest extends RedisContainerSupport {

    private static final String NOW = "1800000000";

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String COUPON = "guard";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String ALIVE = RedisKeys.alive(COUPON, 1, 0);
    private static final String ADMITTED = RedisKeys.admitted(COUPON, 1, 0);

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> script;

    @BeforeEach
    void 준비() {
        script = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE, ADMITTED).block(WAIT);
        for (int i = 0; i < 10; i++) {
            redis.delete(RedisKeys.alive(COUPON, 1, 0)).block(WAIT);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Object> enqueue(String memberId, String aliveTtl, String cap) {
        return (List<Object>) redis.execute(
                        script,
                        List.of(QUEUE, MAX_SCORE, RedisKeys.alive(COUPON, 1, 0), ADMITTED),
                        List.of(memberId, "86400", aliveTtl, cap, NOW))
                .blockFirst(WAIT);
    }

    @Test
    @DisplayName("돌려준_score가_ZSET에_든_값과_정확히_같다")
    void 돌려준_score가_ZSET에_든_값과_정확히_같다() {
        // **tostring 은 못 쓴다.** Lua 5.1 은 수를 %.14g 로 접는데 마이크로초
        // score 는 16자리다. 접히면 돌려준 값과 실제 자리가 어긋나고, 그 값을
        // 토큰에 담아 요청 경로에서 비교하는 순간 앞사람을 추월한다.
        String returned = enqueue("m1", "30", "-1").get(0).toString();

        Double stored = redis.opsForZSet().score(QUEUE, "m1").block(WAIT);

        assertThat(returned).doesNotContain("e+");
        // **한 문장으로 단언한다.** null 검사를 따로 두면 약한 단언이 되고,
        // 서식 인자에 stored 를 넣으면 미등록일 때 NPE 가 진짜 원인을 덮는다.
        assertThat(stored)
                .withFailMessage("돌려준 score 와 ZSET 의 값이 다르다 (미등록이면 null): "
                        + returned)
                .isEqualTo((double) Long.parseLong(returned));
    }

    @Test
    @DisplayName("등록하면_생존_신호에_만료_시각이_찍힌다")
    void 등록하면_생존_신호에_만료_시각이_찍힌다() {
        // ZSET 의 score 가 만료 시각이다. 사람마다 키를 만들면 청소가
        // KEYS 에 없는 키를 만지게 되고 클러스터가 거부한다 (RD-1).
        enqueue("m1", "30", "-1");

        assertThat(redis.opsForZSet().score(ALIVE, "m1").block(WAIT))
                .isEqualTo(Long.parseLong(NOW) + 30);
    }

    @Test
    @DisplayName("생존_TTL은_주입받는다")
    void 생존_TTL은_주입받는다() {
        // 폴링 간격에서 나오는 값이라 스크립트에 박으면 둘이 갈라진다.
        enqueue("m2", "90", "-1");

        assertThat(redis.opsForZSet().score(ALIVE, "m2").block(WAIT))
                .isEqualTo(Long.parseLong(NOW) + 90);
    }

    @Test
    @DisplayName("재등록도_생존_신호를_갱신한다")
    void 재등록도_생존_신호를_갱신한다() {
        // 순번은 그대로지만 살아 있다는 신호는 새로 찍혀야 한다.
        // 안 그러면 성실히 새로고침하는 사람이 이탈자로 지워진다.
        enqueue("m3", "30", "-1");
        redis.opsForZSet().add(ALIVE, "m3", Long.parseLong(NOW) - 100).block(WAIT);

        enqueue("m3", "30", "-1");

        assertThat(redis.opsForZSet().score(ALIVE, "m3").block(WAIT))
                .isEqualTo(Long.parseLong(NOW) + 30);
    }

    @Test
    @DisplayName("상한을_넘으면_등록하지_않고_거부_신호를_낸다")
    void 상한을_넘으면_등록하지_않고_거부_신호를_낸다() {
        enqueue("m1", "30", "2");
        enqueue("m2", "30", "2");

        List<Object> rejected = enqueue("m3", "30", "2");

        assertThat(String.valueOf(rejected.get(0))).isEqualTo("-1");
        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(2);
    }

    @Test
    @DisplayName("이미_선_사람은_상한과_무관하게_자리를_지킨다")
    void 이미_선_사람은_상한과_무관하게_자리를_지킨다() {
        // 줄이 길어진 것이 그 사람 잘못이 아닌데 자리를 잃으면 안 된다.
        enqueue("m1", "30", "2");
        enqueue("m2", "30", "2");
        double first = redis.opsForZSet().score(QUEUE, "m1").block(WAIT);

        List<Object> again = enqueue("m1", "30", "1");

        assertThat(String.valueOf(again.get(2))).isEqualTo("1");
        assertThat(redis.opsForZSet().score(QUEUE, "m1").block(WAIT)).isEqualTo(first);
    }

    /**
     * <b>이미 들여보낸 사람은 줄이 아니다.</b> 상한이 세는 것은 기다리는 인원인데
     * ZSET 은 입장자를 안 지운다 — 청소기가 붙기 전까지 유령이 쌓이고, 실제로
     * 기다리는 사람이 0 명인데 신규가 영구 거절된다.
     */
    @Test
    @DisplayName("입장한_사람은_상한에_안_센다")
    void 입장한_사람은_상한에_안_센다() {
        enqueue("m1", "30", "2");
        enqueue("m2", "30", "2");
        // 배분이 임계를 둘 다 넘겨 올렸다. 둘은 이미 나간 사람이다.
        Double 뒷사람 = redis.opsForZSet().score(QUEUE, "m2").block(WAIT);
        redis.opsForValue().set(ADMITTED, "%.0f".formatted(뒷사람)).block(WAIT);

        List<Object> 신규 = enqueue("m3", "30", "2");

        assertThat(신규.get(0).toString()).isNotEqualTo("-1");
    }

    @Test
    @DisplayName("상한_없음_표식이면_제한하지_않는다")
    void 상한_없음_표식이면_제한하지_않는다() {
        for (int i = 0; i < 5; i++) {
            enqueue("m" + i, "30", "-1");
        }

        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(5);
    }

    /**
     * 도메인은 상한 0 을 "배수할 수 없으니 받지 않는다" 로 읽는다. 여기서 상한
     * 없음으로 읽으면 뜻이 정반대가 되고, 배수가 멎은 줄이 무한히 자란다.
     */
    @Test
    @DisplayName("상한이_0이면_빈_줄에도_안_세운다")
    void 상한이_0이면_빈_줄에도_안_세운다() {
        List<Object> result = enqueue("m1", "30", "0");

        assertThat(String.valueOf(result.get(0))).isEqualTo("-1");
        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isZero();
    }

    @Test
    @DisplayName("잘못된_인자는_아무것도_쓰지_않는다")
    void 잘못된_인자는_아무것도_쓰지_않는다() {
        // Lua 는 중간 오류를 되돌리지 않는다. 쓰기 전에 전부 검증한다.
        assertThatThrownBy(() -> enqueue("m1", "0", "-2")).rootCause()
                .hasMessageContaining("alive TTL");
        assertThatThrownBy(() -> enqueue("m1", "30", "-2")).rootCause()
                .hasMessageContaining("큐 길이 상한");

        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isZero();
        assertThat(redis.hasKey(MAX_SCORE).block(WAIT)).isFalse();
        // alive 만 생기는 회귀를 잡는다. 검증이 첫 쓰기 앞이라는 계약은
        // 세 키 전부에 걸린다 — 하나라도 새면 계약이 아니다.
        assertThat(redis.hasKey(RedisKeys.alive(COUPON, 1, 0)).block(WAIT)).isFalse();
    }
}

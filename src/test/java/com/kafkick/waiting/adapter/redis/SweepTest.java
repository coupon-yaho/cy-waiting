package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.queue.GraceRetention;
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
 * 이탈자 청소.
 *
 * <p><b>입장 임계 위에서 앞부분만 훑는다.</b> 2만 명 큐에서 전체를 보면 청소
 * 자체가 부하다. 뒤엣사람은 아직 폴링할 차례가 안 왔을 뿐 죽은 것이 아니다.
 */
@Tag("integration")
@SpringBootTest
class SweepTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String COUPON = "sweep";
    private static final String QUEUE = RedisKeys.queue(COUPON, 1, 0);
    private static final String MAX_SCORE = RedisKeys.maxScore(COUPON, 1, 0);
    private static final String GRACE = RedisKeys.grace(COUPON, 1, 0);
    private static final String ALIVE = RedisKeys.alive(COUPON, 1, 0);
    private static final String ADMITTED = RedisKeys.admitted(COUPON, 1, 0);

    /** 시각을 주입한다 — 실제 시계에 기대면 만료 시험이 흔들린다 (TS-4). */
    private static final long NOW = 1_800_000_000L;

    /** 보관 기간을 확실히 넘긴 시각. <b>손으로 안 적는다</b> — 기간이 늘면 같이 는다. */
    private static final long 만료된_시각 = NOW - GraceRetention.SECONDS - 1;

    /** 아직 안 넘긴 시각. 경계 바로 안쪽이라 기간이 늘어도 신선하다. */
    private static final long 신선한_시각 = NOW - 1;
    /** 보관 기간. <b>손으로 안 적는다</b> — 토큰 수명과의 관계가 도메인에 있다. */
    private static final String RETENTION = String.valueOf(GraceRetention.SECONDS);
    private static final String BUDGET = "1000";

    /** unpack 한계에서 실측한 상한. 기록이 쌍이라 검사 범위 쪽이 먼저 걸린다. */
    private static final int MAX_SCAN = 3_999;
    private static final int MAX_BUDGET = 7_999;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> enqueueScript;
    private RedisScript<List> sweepScript;

    @BeforeEach
    void 준비() {
        enqueueScript = RedisScript.of(new ClassPathResource("redis/enqueue.lua"), List.class);
        sweepScript = RedisScript.of(new ClassPathResource("redis/sweep.lua"), List.class);
        redis.delete(QUEUE, MAX_SCORE, GRACE, ALIVE, ADMITTED).block(WAIT);
    }

    private void enqueue(String memberId) {
        redis.execute(enqueueScript,
                        List.of(QUEUE, MAX_SCORE, ALIVE, ADMITTED, GRACE),
                        List.of(memberId, "86400", "3600", "-1", String.valueOf(NOW), "300"))
                .blockFirst(WAIT);
    }

    @SuppressWarnings("unchecked")
    private List<Object> sweep(String limit, String budget, String cursor) {
        return (List<Object>) redis.execute(
                        sweepScript,
                        List.of(QUEUE, GRACE, ALIVE, ADMITTED),
                        List.of(limit, String.valueOf(NOW), RETENTION, budget, cursor))
                .blockFirst(WAIT);
    }

    private List<Object> sweep(String limit) {
        return sweep(limit, BUDGET, "0");
    }

    private String nextCursor(List<Object> r) {
        return String.valueOf(r.get(3));
    }

    private long swept(List<Object> r) {
        return Long.parseLong(String.valueOf(r.get(0)));
    }

    /** 유예 기록 정리 수. 반환값 두 번째는 생존 신호 정리 수다. */
    private long expired(List<Object> r) {
        return Long.parseLong(String.valueOf(r.get(2)));
    }

    @Test
    @DisplayName("생존_키가_없는_앞부분_항목이_제거된다")
    void 생존_키가_없는_앞부분_항목이_제거된다() {
        enqueue("m0");
        enqueue("m1");
        enqueue("m2");
        double keptScore = redis.opsForZSet().score(QUEUE, "m2").block(WAIT);
        redis.opsForZSet().remove(ALIVE, "m0", "m1").block(WAIT);

        assertThat(swept(sweep("10"))).isEqualTo(2);

        // 크기만 보면 엉뚱한 사람을 지운 구현도 통과한다. 누가 빠지고
        // 누가 남았는지를 직접 본다.
        assertThat(redis.opsForZSet().score(QUEUE, "m0").block(WAIT)).isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "m1").block(WAIT)).isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "m2").block(WAIT)).isEqualTo(keptScore);
    }

    @Test
    @DisplayName("생존_키가_있으면_건드리지_않는다")
    void 생존_키가_있으면_건드리지_않는다() {
        enqueue("m0");
        enqueue("m1");

        assertThat(swept(sweep("10"))).isZero();
        assertThat(redis.opsForZSet().size(QUEUE).block(WAIT)).isEqualTo(2);
    }

    /** 이 사람의 생존 신호를 살려 둔다. */
    private void 살아있다(String memberId) {
        redis.opsForZSet().add(ALIVE, memberId, NOW + 3_600).block(WAIT);
    }

    /**
     * 창 안에 살아 있는 사람을 하나 세운다.
     *
     * <p><b>줄 안이어야 한다.</b> 창 안이 통째로 조용하면 스크립트가 아무것도
     * 안 한다 — 그건 전원 이탈이 아니라 저장소 유실이기 때문이다. 줄 밖에
     * 세우면 그 가드를 못 지난다.
     */
    private void 창_안에_살아있는_사람() {
        enqueue("keeper");
        살아있다("keeper");
    }

    @Test
    @DisplayName("검사_범위_밖은_보지_않는다")
    void 검사_범위_밖은_보지_않는다() {
        // **맨 앞에 살려 둔다.** 창 안이 통째로 조용하면 아무것도 안 한다.
        창_안에_살아있는_사람();
        for (int i = 0; i < 5; i++) {
            enqueue("m" + i);
            redis.opsForZSet().remove(ALIVE, "m" + i).block(WAIT);
        }

        double kept2 = redis.opsForZSet().score(QUEUE, "m2").block(WAIT);
        double kept4 = redis.opsForZSet().score(QUEUE, "m4").block(WAIT);

        // 창은 keeper·m0·m1 이다. 살아 있는 keeper 는 안 걷힌다.
        assertThat(swept(sweep("3"))).isEqualTo(2);

        // 앞 둘만 빠지고 범위 밖은 **순번까지 그대로** 남는다
        assertThat(redis.opsForZSet().score(QUEUE, "m0").block(WAIT)).isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "m1").block(WAIT)).isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "m2").block(WAIT)).isEqualTo(kept2);
        assertThat(redis.opsForZSet().score(QUEUE, "m4").block(WAIT)).isEqualTo(kept4);
    }

    /**
     * <b>임계 아래의 유령이 검사 범위를 막지 않는다</b> (G7.5).
     *
     * <p>차례가 왔는데 안 걷어 간 사람은 큐에 남고, 걷지도 않는다 (7.4.11). 그런
     * 사람이 검사 범위만큼 쌓이면 앞에서 K 명을 보는 방식은 그들만 보고 끝난다 —
     * 살아 있는 구간에는 영영 안 닿는다.
     *
     */
    // 부하 실측에서 드러났다. 이탈 30% 에 크레딧 낭비가 36.9% 였고, 걷은 수가
    // 0 이었다 — 스위퍼가 매 틱 도는데 아무도 안 걷히고 있었다.
    @Test
    @DisplayName("임계_아래_유령이_검사_범위를_막지_않는다")
    void 임계_아래_유령이_검사_범위를_막지_않는다() {
        // 앞 둘은 차례가 온 유령이다 — 신호가 없어도 안 걷힌다.
        enqueue("유령0");
        enqueue("유령1");
        redis.opsForZSet().remove(ALIVE, "유령0").block(WAIT);
        redis.opsForZSet().remove(ALIVE, "유령1").block(WAIT);
        double 임계 = redis.opsForZSet().score(QUEUE, "유령1").block(WAIT);
        redis.opsForValue().set(ADMITTED, String.valueOf((long) 임계)).block(WAIT);
        // 그 뒤에 이탈자와 성실한 사람이 선다.
        enqueue("이탈자");
        redis.opsForZSet().remove(ALIVE, "이탈자").block(WAIT);
        enqueue("성실이");
        살아있다("성실이");
        double 유령0_순번 = redis.opsForZSet().score(QUEUE, "유령0").block(WAIT);
        double 성실이_순번 = redis.opsForZSet().score(QUEUE, "성실이").block(WAIT);

        // **검사 범위를 둘로 준다.** 앞에서 세는 방식이면 유령 둘만 보고 끝난다.
        assertThat(swept(sweep("2"))).as("살아 있는 구간까지 닿는다").isEqualTo(1);

        assertThat(redis.opsForZSet().score(QUEUE, "이탈자").block(WAIT))
                .as("임계 위의 이탈자는 걷힌다").isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "유령0").block(WAIT))
                .as("임계 아래는 순번까지 그대로 둔다 (7.4.11)").isEqualTo(유령0_순번);
        assertThat(redis.opsForZSet().score(QUEUE, "성실이").block(WAIT))
                .as("살아 있는 사람은 순번까지 그대로다").isEqualTo(성실이_순번);
    }

    /**
     * <b>임계를 지수 표기로 만들지 않는다.</b>
     *
     * <p>큐 score 는 마이크로초 시각이라 열여섯 자리다. Lua 의 기본 수→문자열
     * 변환은 유효숫자 열넷까지만 남기므로, 임계를 그대로 이어 붙이면
     * {@code 1.7879388228153e+15} 가 되어 실제 임계보다 <b>위</b>로 접힌다.
     */
    // 그러면 검사 창이 임계 바로 위의 사람들을 건너뛴다 — 하필 곧 차례가 올
    // 사람들이다.
    @Test
    @DisplayName("마이크로초_임계를_정밀하게_읽는다")
    void 마이크로초_임계를_정밀하게_읽는다() {
        // **반올림이 위로 가는 값을 고른다.** 아래로 접히는 값은 창이 넓어질
        // 뿐이라 어떤 사람도 안 빠지고, 그러면 이 시험이 아무것도 안 잡는다.
        // 이 값은 %.14g 에서 …815300 으로 35µs 위로 접힌다.
        long 임계값 = 1_787_938_822_815_265L;
        redis.opsForValue().set(ADMITTED, String.valueOf(임계값)).block(WAIT);
        // 접힌 폭 안쪽에 세운다. 밖에 세우면 접히든 말든 창에 들어온다.
        redis.opsForZSet().add(QUEUE, "이탈자", 임계값 + 10).block(WAIT);
        redis.opsForZSet().add(QUEUE, "성실이", 임계값 + 20).block(WAIT);
        살아있다("성실이");

        assertThat(swept(sweep("100"))).as("임계 바로 위도 창에 든다").isEqualTo(1);

        assertThat(redis.opsForZSet().score(QUEUE, "이탈자").block(WAIT))
                .as("임계 위의 이탈자는 걷힌다").isNull();
        assertThat(redis.opsForZSet().score(QUEUE, "성실이").block(WAIT))
                .as("살아 있는 사람은 순번까지 그대로다").isEqualTo(임계값 + 20);
    }

    /**
     * <b>입장 표시를 든 사람은 큐에서 빼지 않는다.</b>
     *
     * <p>차례가 왔던 사람이 다시 줄을 서면 그 표시가 남은 채로 임계 위에 선다.
     * 거기서 걷으면 표시만 남아, 다음 폴링에 조회가 <b>입장</b>이라고 답한다 —
     * 차례가 안 왔는데 입장이므로 줄 전체를 추월하고 초과 발급이 된다.
     */
    @Test
    @DisplayName("입장_표시를_든_사람은_안_걷는다")
    void 입장_표시를_든_사람은_안_걷는다() {
        enqueue("돌아온사람");
        redis.opsForZSet().remove(ALIVE, "돌아온사람").block(WAIT);
        // 차례가 왔던 표시. 재등록이 이것을 안 지우므로 임계 위에 남는다.
        redis.opsForHash().put(GRACE, "돌아온사람", "a:" + 신선한_시각).block(WAIT);
        enqueue("성실이");
        살아있다("성실이");
        double 돌아온사람_순번 = redis.opsForZSet().score(QUEUE, "돌아온사람").block(WAIT);

        assertThat(swept(sweep("10"))).as("표시를 든 사람은 안 센다").isZero();

        assertThat(redis.opsForZSet().score(QUEUE, "돌아온사람").block(WAIT))
                .as("큐에 남는다 — 빼면 표시만 남아 입장으로 읽힌다")
                .isEqualTo(돌아온사람_순번);
        assertThat(redis.opsForHash().get(GRACE, "돌아온사람").block(WAIT))
                .as("표시도 그대로 둔다").isEqualTo("a:" + 신선한_시각);
    }

    /**
     * <b>창 안이 통째로 조용하면 아무도 안 걷는다.</b>
     *
     * <p>줄 밖에 살아 있는 사람이 하나라도 있으면 열리는 가드는 창을 임계 위로
     * 옮긴 뒤로 뜻이 약해졌다. 창이 곧 "곧 차례가 올 사람들" 을 가리키므로,
     * 매진이 길어져 그 구간의 신호가 일제히 멎은 판에서 K 명이 통째로 걷힌다.
     */
    @Test
    @DisplayName("창_안이_전부_조용하면_안_걷는다")
    void 창_안이_전부_조용하면_안_걷는다() {
        for (int i = 0; i < 4; i++) {
            enqueue("대기자" + i);
            redis.opsForZSet().remove(ALIVE, "대기자" + i).block(WAIT);
        }
        // 창 밖에 한 명만 살아 있다. 전역 가드는 이걸로 열린다.
        enqueue("멀리있는사람");
        살아있다("멀리있는사람");
        double 대기자0_순번 = redis.opsForZSet().score(QUEUE, "대기자0").block(WAIT);

        assertThat(swept(sweep("4"))).as("창 안에 살아 있는 신호가 없다").isZero();

        assertThat(redis.opsForZSet().score(QUEUE, "대기자0").block(WAIT))
                .as("순번까지 그대로").isEqualTo(대기자0_순번);
    }

    /**
     * <b>임계가 깨졌으면 앞줄을 안 걷는다.</b>
     *
     * <p>이 스크립트에서 {@code -1} 은 보수적인 값이 아니라 <b>가장 공격적인</b>
     * 값이다 — 창이 큐 전체로 열리고 임계 검사도 전원에 대해 참이 된다. 깨진
     * 값을 그리로 접으면 차례가 왔던 사람까지 통째로 걷힌다 (불변식 4).
     */
    // 없는 것과 깨진 것은 다르다. 없으면 새 쿠폰이라 -1 이 맞고, 깨졌으면
    // 앞줄 제거만 접는다 — 정리까지 멈추면 해시가 한 방향으로만 자란다.
    @Test
    @DisplayName("임계가_깨졌으면_앞줄을_안_걷는다")
    void 임계가_깨졌으면_앞줄을_안_걷는다() {
        for (String 깨진_값 : List.of("nan", "inf", "-inf", "없는수")) {
            redis.delete(QUEUE, GRACE, ALIVE, ADMITTED).block(WAIT);
            enqueue("이탈자");
            redis.opsForZSet().remove(ALIVE, "이탈자").block(WAIT);
            enqueue("성실이");
            살아있다("성실이");
            redis.opsForHash().put(GRACE, "낡은기록", "d:" + 만료된_시각).block(WAIT);
            double 이탈자_순번 = redis.opsForZSet().score(QUEUE, "이탈자").block(WAIT);
            redis.opsForValue().set(ADMITTED, 깨진_값).block(WAIT);

            List<Object> 결과 = sweep("100");

            assertThat(swept(결과)).as("%s — 앞줄은 안 걷는다", 깨진_값).isZero();
            assertThat(redis.opsForZSet().score(QUEUE, "이탈자").block(WAIT))
                    .as("%s — 순번까지 그대로", 깨진_값).isEqualTo(이탈자_순번);
            // **정리까지 멈추지는 않는다.** 멈추면 해시가 한 방향으로만 자란다.
            assertThat(expired(결과)).as("%s — 낡은 기록은 걷는다", 깨진_값).isOne();
        }
    }

    @Test
    @DisplayName("검사_범위가_인자로_주어진다")
    void 검사_범위가_인자로_주어진다() {
        // 부하와 정확도의 맞바꿈이라 배포 없이 조절할 수 있어야 한다 (P-1).
        창_안에_살아있는_사람();
        for (int i = 0; i < 5; i++) {
            enqueue("m" + i);
            redis.opsForZSet().remove(ALIVE, "m" + i).block(WAIT);
        }

        // 창이 keeper 하나면 걷을 사람이 없고, 둘이면 m0 하나가 걷힌다.
        // **앞의 판이 이미 걷었다.** 다섯을 보면 남은 m1~m4 가 걷힌다.
        assertThat(swept(sweep("1"))).as("창에 살아 있는 사람만 든다").isZero();
        assertThat(swept(sweep("2"))).as("창이 넓어지면 하나 걷는다").isOne();
        assertThat(swept(sweep("5"))).as("남은 넷").isEqualTo(4);
    }

    @Test
    @DisplayName("제거된_사람이_유예_기록에_남는다")
    void 제거된_사람이_유예_기록에_남는다() {
        // 제거와 기록이 갈리면 자리도 잃고 재방문자로도 식별 안 되는
        // 사람이 생긴다. 같은 스크립트 안에서 한다.
        창_안에_살아있는_사람();
        enqueue("m0");
        redis.opsForZSet().remove(ALIVE, "m0").block(WAIT);

        sweep("10");

        // 값에 종류를 싣는다. 조회가 쓰는 입장 표시와 같은 해시를 나눠 쓴다.
        assertThat(redis.opsForHash().get(GRACE, "m0").block(WAIT))
                .isEqualTo("d:" + NOW);
    }

    @Test
    @DisplayName("만료된_유예_기록이_정리된다")
    void 만료된_유예_기록이_정리된다() {
        redis.opsForHash().put(GRACE, "old", String.valueOf(만료된_시각)).block(WAIT);
        redis.opsForHash().put(GRACE, "fresh", String.valueOf(신선한_시각)).block(WAIT);

        assertThat(expired(sweep("10"))).isOne();
        assertThat(redis.opsForHash().hasKey(GRACE, "old").block(WAIT)).isFalse();
        assertThat(redis.opsForHash().hasKey(GRACE, "fresh").block(WAIT)).isTrue();
    }

    @Test
    @DisplayName("유예_기록이_무한히_쌓이지_않는다")
    void 유예_기록이_무한히_쌓이지_않는다() {
        // 만료가 없으면 이 해시가 영원히 자란다 (RD-7).
        for (int i = 0; i < 50; i++) {
            redis.opsForHash().put(GRACE, "old" + i, String.valueOf(만료된_시각)).block(WAIT);
        }

        sweep("10");

        assertThat(redis.opsForHash().size(GRACE).block(WAIT)).isZero();
    }

    @Test
    @DisplayName("값이_깨진_유예_기록도_정리된다")
    void 값이_깨진_유예_기록도_정리된다() {
        // 숫자가 아니면 언제 것인지 알 수 없다. 남겨 두면 영원히 안 지워진다.
        redis.opsForHash().put(GRACE, "broken", "언제인지모름").block(WAIT);

        assertThat(expired(sweep("10"))).isOne();
        assertThat(redis.opsForHash().hasKey(GRACE, "broken").block(WAIT)).isFalse();
    }

    @Test
    @DisplayName("잘못된_인자는_아무것도_바꾸지_않는다")
    void 잘못된_인자는_아무것도_바꾸지_않는다() {
        enqueue("m0");
        double before = redis.opsForZSet().score(QUEUE, "m0").block(WAIT);
        redis.opsForZSet().remove(ALIVE, "m0").block(WAIT);

        assertThatThrownBy(() -> sweep("0")).rootCause().hasMessageContaining("검사 범위");

        assertThat(redis.opsForZSet().score(QUEUE, "m0").block(WAIT)).isEqualTo(before);
        assertThat(redis.opsForHash().size(GRACE).block(WAIT)).isZero();
    }

    @Test
    @DisplayName("정리_예산이_한_번의_실행을_묶는다")
    void 정리_예산이_한_번의_실행을_묶는다() {
        // COUNT 는 힌트지 상한이 아니다. 받은 것 중 예산만큼만 지워야
        // 한 번의 실행이 유계다.
        for (int i = 0; i < 40; i++) {
            redis.opsForHash().put(GRACE, "old" + i, String.valueOf(만료된_시각)).block(WAIT);
        }

        assertThat(expired(sweep("10", "5", "0"))).isEqualTo(5);
        assertThat(redis.opsForHash().size(GRACE).block(WAIT)).isEqualTo(35);
    }

    @Test
    @DisplayName("커서를_이어_넘기면_전부_정리된다")
    void 커서를_이어_넘기면_전부_정리된다() {
        // 한 번에 다 안 지우는 대신 다음 틱이 이어받는다. 커서가 돌지
        // 않으면 같은 앞부분만 계속 보고 뒤는 영영 안 지워진다.
        for (int i = 0; i < 40; i++) {
            redis.opsForHash().put(GRACE, "old" + i, String.valueOf(만료된_시각)).block(WAIT);
        }

        String cursor = "0";
        for (int round = 0; round < 20; round++) {
            cursor = nextCursor(sweep("10", "5", cursor));
            if (Boolean.TRUE.equals(redis.opsForHash().size(GRACE).block(WAIT) == 0L)) {
                break;
            }
        }

        assertThat(redis.opsForHash().size(GRACE).block(WAIT)).isZero();
    }

    @Test
    @DisplayName("잘못된_커서는_아무것도_바꾸지_않는다")
    void 잘못된_커서는_아무것도_바꾸지_않는다() {
        // 커서 검증이 쓰기 뒤에 있으면 앞의 쓰기가 남는다 — Lua 는
        // 롤백하지 않는다.
        enqueue("m0");
        double before = redis.opsForZSet().score(QUEUE, "m0").block(WAIT);
        redis.opsForZSet().remove(ALIVE, "m0").block(WAIT);
        redis.opsForHash().put(GRACE, "keep", String.valueOf(NOW)).block(WAIT);

        assertThatThrownBy(() -> sweep("10", "5", "abc")).rootCause()
                .hasMessageContaining("커서");

        assertThat(redis.opsForZSet().score(QUEUE, "m0").block(WAIT)).isEqualTo(before);
        assertThat(redis.opsForHash().hasKey(GRACE, "keep").block(WAIT)).isTrue();
    }

    @Test
    @DisplayName("만료된_생존_신호도_예산_안에서_걷는다")
    void 만료된_생존_신호도_예산_안에서_걷는다() {
        // 한 번에 다 지우려 하면 그 자체가 오래 걸린다.
        for (int i = 0; i < 20; i++) {
            redis.opsForZSet().add(ALIVE, "gone" + i, NOW - 100).block(WAIT);
        }

        List<Object> result = sweep("1", "5", "0");

        assertThat(Long.parseLong(String.valueOf(result.get(1)))).isEqualTo(5);
        assertThat(redis.opsForZSet().size(ALIVE).block(WAIT)).isEqualTo(15);
    }

    /**
     * <b>천장이 없으면 부하가 오른 날 스위퍼가 통째로 멎는다.</b> 인자가 쌍이라
     * 기록 쓰기가 먼저 걸리고, 같은 자리가 매 틱 반복되면 큐가 영구 정지한다.
     *
     * <p>실측 상한은 검사 범위 3,999 · 예산 7,999 다. 호출부가 우회 못 하게
     * 스크립트 안에서 막는다.
     */
    @Test
    @DisplayName("검사_범위가_천장을_넘으면_거부한다")
    void 검사_범위가_천장을_넘으면_거부한다() {
        assertThatThrownBy(() -> sweep(String.valueOf(MAX_SCAN + 1)))
                .hasRootCauseMessage("검사 범위는 %d 이하여야 한다: %d".formatted(MAX_SCAN, MAX_SCAN + 1));

        // 경계는 통과한다. 한 칸 안쪽만 막으면 실사용 폭이 조용히 줄어든다.
        // 빈 큐라 한 명도 안 걷힌다 — 반환 모양까지 본다.
        assertThat(swept(sweep(String.valueOf(MAX_SCAN)))).isZero();
    }

    @Test
    @DisplayName("예산이_천장을_넘으면_거부한다")
    void 예산이_천장을_넘으면_거부한다() {
        assertThatThrownBy(() -> sweep("10", String.valueOf(MAX_BUDGET + 1), "0"))
                .hasRootCauseMessage(
                        "정리 예산은 %d 이하여야 한다: %d".formatted(MAX_BUDGET, MAX_BUDGET + 1));

        assertThat(swept(sweep("10", String.valueOf(MAX_BUDGET), "0"))).isZero();
    }
}

package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.springframework.data.domain.Range;
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
 * 입장 임계는 <b>개수가 아니라 score 값</b>이다 (D-8).
 *
 * <p>개수를 올리는 방식은 떠난 사람의 자리까지 소모하고, 리더가 겹치면 두 번
 * 적용돼 두 배가 입장한다. score 임계는 두 번 적용돼도 값이 같아서, 펜싱을
 * 안 쓰는 근거가 여기서 나온다 (A-7).
 */
@Tag("integration")
@SpringBootTest
class AllocationApplyTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final String QUEUE = RedisKeys.queue("c1", 1, 0);
    private static final String ADMITTED = RedisKeys.admitted("c1", 1, 0);

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private RedisScript<List> apply;

    @BeforeEach
    void 준비() {
        apply = RedisScript.of(new ClassPathResource("redis/allocation_apply.lua"), List.class);
        redis.delete(QUEUE, ADMITTED).block(WAIT);
    }

    private void 줄_세운다(long... scores) {
        for (long score : scores) {
            redis.opsForZSet().add(QUEUE, "m" + score, score).block(WAIT);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Object> 배분(long admit) {
        return (List<Object>) redis.execute(apply, List.of(QUEUE, ADMITTED),
                List.of(String.valueOf(admit))).blockLast(WAIT);
    }

    private long 임계() {
        return Long.parseLong(String.valueOf(배분(0).get(0)));
    }

    /**
     * 운영에서 실제로 나오는 자리.
     *
     * <p>score 는 레디스 시각의 마이크로초라 <b>16자리</b>다. 두 자리 수로만
     * 재면 문자열로 만들 때 접히는 결함을 통째로 못 본다.
     */
    private static final long 마이크로초 = 1_700_000_000_123_456L;

    @Test
    @DisplayName("크레딧만큼의_사람이_임계_안에_들어온다")
    void 크레딧만큼의_사람이_임계_안에_들어온다() {
        줄_세운다(10, 20, 30, 40, 50);

        List<Object> 결과 = 배분(3);

        assertThat(String.valueOf(결과.get(0))).isEqualTo("30");
        assertThat(Long.parseLong(String.valueOf(결과.get(1)))).isEqualTo(3);
    }

    @Test
    @DisplayName("이미_들어온_사람은_다시_안_센다")
    void 이미_들어온_사람은_다시_안_센다() {
        // 앞에서부터 세면 통과한 사람 자리에 크레딧을 낭비한다. 그만큼 실제로
        // 들어오는 사람이 준다.
        줄_세운다(10, 20, 30, 40, 50);
        배분(2);

        List<Object> 결과 = 배분(2);

        assertThat(String.valueOf(결과.get(0))).isEqualTo("40");
        assertThat(Long.parseLong(String.valueOf(결과.get(1)))).isEqualTo(2);
    }

    @Test
    @DisplayName("크레딧이_큐보다_크면_마지막_사람까지만_들인다")
    void 크레딧이_큐보다_크면_마지막_사람까지만_들인다() {
        // 무한대를 쓰면 이후 도착자까지 임계 아래로 들어와 줄을 서지 않고
        // 통과한다 (D-10).
        줄_세운다(10, 20, 30);

        List<Object> 결과 = 배분(100);
        assertThat(String.valueOf(결과.get(0))).isEqualTo("30");

        줄_세운다(40);
        assertThat(임계()).isEqualTo(30);
    }

    @Test
    @DisplayName("두_번_적용해도_두_배가_안_들어온다")
    void 두_번_적용해도_두_배가_안_들어온다() {
        // 리더가 겹쳐 같은 판이 두 번 적용되는 상황이다. 개수 기반이면 여기서
        // 두 배가 입장했다.
        줄_세운다(10, 20, 30, 40, 50);
        배분(2);

        assertThat(임계()).isEqualTo(20);
    }

    @Test
    @DisplayName("크레딧이_없으면_임계가_안_움직인다")
    void 크레딧이_없으면_임계가_안_움직인다() {
        줄_세운다(10, 20, 30);
        배분(2);

        List<Object> 결과 = 배분(0);

        assertThat(String.valueOf(결과.get(0))).isEqualTo("20");
        assertThat(Long.parseLong(String.valueOf(결과.get(1)))).isZero();
    }

    @Test
    @DisplayName("큐가_비면_임계가_안_움직인다")
    void 큐가_비면_임계가_안_움직인다() {
        줄_세운다(10, 20);
        배분(2);
        redis.delete(QUEUE).block(WAIT);

        List<Object> 결과 = 배분(5);

        assertThat(String.valueOf(결과.get(0))).isEqualTo("20");
        assertThat(Long.parseLong(String.valueOf(결과.get(1)))).isZero();
    }

    @Test
    @DisplayName("임계는_뒤로_가지_않는다")
    void 임계는_뒤로_가지_않는다() {
        // **들어온 사람이 큐에서 빠지면 남은 최대 score 가 임계보다 작아진다.**
        // 그때 계산값을 그대로 쓰면 이미 통과한 사람이 다시 대기가 된다 —
        // 순번 역행이다. 계산값이 현재와 같은 상황으로는 이걸 못 잰다.
        줄_세운다(10, 20, 30, 40);
        배분(4);
        assertThat(임계()).isEqualTo(40);

        // 30·40 이 차례가 되어 큐에서 빠졌다. 남은 최대는 20 이다.
        redis.opsForZSet().remove(QUEUE, "m30", "m40").block(WAIT);
        List<Object> 결과 = 배분(5);

        assertThat(String.valueOf(결과.get(0))).isEqualTo("40");
        assertThat(Long.parseLong(String.valueOf(결과.get(1)))).isZero();
    }

    @Test
    @DisplayName("마이크로초_자리가_접히지_않는다")
    void 마이크로초_자리가_접히지_않는다() {
        // Lua 는 수를 열넷째 자리까지만 찍는다. 마이크로초 score 는 열여섯 자리라
        // 과학 표기로 접히며 최대 100μs 가 반올림된다.
        //
        // **올림 쪽으로 접히면** 그 사이 도착자가 줄을 안 서고 통과하고,
        // **내림 쪽으로 접히면** 이미 통과한 사람이 다시 대기가 된다.
        줄_세운다(마이크로초, 마이크로초 + 1, 마이크로초 + 2, 마이크로초 + 3);

        List<Object> 결과 = 배분(2);

        assertThat(String.valueOf(결과.get(0))).isEqualTo(String.valueOf(마이크로초 + 1));
        assertThat(Long.parseLong(String.valueOf(결과.get(1)))).isEqualTo(2);
        // 임계 아래에 정확히 둘만 있어야 한다. 접히면 넷이 다 들어온다.
        assertThat(redis.opsForZSet().count(QUEUE,
                Range.closed(Double.NEGATIVE_INFINITY, (double) (마이크로초 + 1))).block(WAIT))
                .isEqualTo(2);
    }

    @Test
    @DisplayName("마이크로초_자리에서도_이어서_들인다")
    void 마이크로초_자리에서도_이어서_들인다() {
        // 접힌 임계를 다시 읽으면 그 위에서부터 세는 기준도 함께 어긋난다.
        줄_세운다(마이크로초, 마이크로초 + 1, 마이크로초 + 2, 마이크로초 + 3);
        배분(2);

        List<Object> 결과 = 배분(1);

        assertThat(String.valueOf(결과.get(0))).isEqualTo(String.valueOf(마이크로초 + 2));
        assertThat(Long.parseLong(String.valueOf(결과.get(1)))).isEqualTo(1);
    }

    @Test
    @DisplayName("깨진_임계는_낮추지_않고_거절한다")
    void 깨진_임계는_낮추지_않고_거절한다() {
        // **없는 것과 깨진 것은 다르다.** 둘 다 없는 것으로 접으면 큐 맨 앞부터
        // 다시 세어 임계가 뒤로 가고, 이미 통과한 사람이 대기로 되돌아간다.
        // 그건 순번 역행이라 타협할 수 없다.
        //
        // 그 쿠폰의 배분은 멎지만 소리를 내므로 고칠 수 있다. 조용히 역행하는
        // 것보다 낫다.
        줄_세운다(10, 20, 30);
        배분(2);
        redis.opsForValue().set(ADMITTED, "열").block(WAIT);

        assertThatThrownBy(() -> 배분(3)).rootCause().hasMessageContaining("임계가 수가 아니다");
        assertThat(redis.opsForValue().get(ADMITTED).block(WAIT)).isEqualTo("열");
    }

    @Test
    @DisplayName("무한대_임계는_거절한다")
    void 무한대_임계는_거절한다() {
        // 무한대는 비교를 통과하면서 임계를 영원히 못 올리게 만든다. 깨진
        // 값보다 나쁘다 — 오류도 안 나고 그냥 멎는다.
        줄_세운다(10, 20, 30);
        redis.opsForValue().set(ADMITTED, "inf").block(WAIT);

        assertThatThrownBy(() -> 배분(2)).rootCause().hasMessageContaining("임계가 수가 아니다");
    }

    @Test
    @DisplayName("임계가_없으면_맨_앞부터_들인다")
    void 임계가_없으면_맨_앞부터_들인다() {
        // 새 쿠폰이다. 없는 것은 정상이라 거절하면 안 된다.
        줄_세운다(10, 20, 30);

        assertThat(String.valueOf(배분(2).get(0))).isEqualTo("20");
    }

    @Test
    @DisplayName("들일_인원이_잘못되면_거절한다")
    void 들일_인원이_잘못되면_거절한다() {
        줄_세운다(10);

        assertThatThrownBy(() -> 배분(-1)).rootCause().hasMessageContaining("이하의 정수");
        assertThatThrownBy(() -> redis.execute(apply, List.of(QUEUE, ADMITTED), List.of("1.5"))
                .blockLast(WAIT)).rootCause().hasMessageContaining("이하의 정수");
        // 무한대는 정수 검사만으로는 안 걸린다. 그 뒤 다른 곳에서 엉뚱한
        // 메시지로 터져 원인을 못 찾는다.
        assertThatThrownBy(() -> redis.execute(apply, List.of(QUEUE, ADMITTED), List.of("inf"))
                .blockLast(WAIT)).rootCause().hasMessageContaining("이하의 정수");
    }
}

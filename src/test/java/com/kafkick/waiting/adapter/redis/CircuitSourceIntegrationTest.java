package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.control.AllocationRound;
import com.kafkick.waiting.control.ControlPlaneProperties;
import com.kafkick.waiting.domain.admission.CircuitState;
import com.kafkick.waiting.control.GatewayRegistry;
import com.kafkick.waiting.control.Leadership;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 배분이 <b>어느 서킷을 읽는지</b>를 잰다 (CY-791).
 *
 * <p>값이 아니라 <b>출처</b>를 재는 자리다. 조각마다 시험이 있어도 배선이
 * 리더의 로컬 서킷으로 되돌아가면 전부 초록인 채로 통과한다 — 실제로 그
 * 되돌림이 한 번 조용히 들어왔다. {@code AllocationRoundTest} 는 공급자를 직접
 * 주입하므로 무엇을 물고 있든 맞게 돈다.
 */
@Tag("integration")
@SpringBootTest(properties = "waiting.scheduler.enabled=true")
class CircuitSourceIntegrationTest extends RedisContainerSupport {

    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final String COUPON = "cy791-source";

    @Autowired
    private ReactiveStringRedisTemplate redis;

    /** <b>실제로 배선된 판을 돌린다.</b> 손으로 조립하면 이 시험의 뜻이 사라진다. */
    @Autowired
    private AllocationRound wired;

    @Autowired
    private GatewayRegistry registry;

    @Autowired
    private Leadership leadership;

    @BeforeEach
    void 준비() {
        redis.delete(RedisKeys.queue(COUPON, 1, 0), RedisKeys.admitted(COUPON, 1, 0),
                RedisKeys.stock(COUPON), RedisKeys.maxScore(COUPON, 1, 0)).block(WAIT);
        redis.opsForZSet().add(RedisKeys.queue(COUPON, 1, 0), "m1", 100).block(WAIT);
        redis.opsForValue().set(RedisKeys.stock(COUPON), "1000").block(WAIT);
        redis.opsForSet().add(RedisKeys.ACTIVE_COUPONS, COUPON).block(WAIT);

        redis.delete(RedisKeys.LEADER).block(WAIT);
        leadership.renew().block(WAIT);
        assertThat(leadership.isLeader()).as("전제 — 이 노드가 리더다").isTrue();
    }

    /**
     * <b>{@code release()} 를 부르지 않는다.</b> 그것은 종료 절차라 standing 을
     * 영구히 닫아, 이 컨텍스트를 쓰는 뒤 시험이 영영 리더가 못 된다.
     */
    @AfterEach
    void 정리() {
        redis.opsForSet().remove(RedisKeys.ACTIVE_COUPONS, COUPON).block(WAIT);
        닫혔다고_알린다();
    }

    /**
     * 푸는 방향은 연속 관측 뒤에 <b>한 계단씩</b> 움직인다.
     *
     * <p>그래서 OPEN 에서 CLOSED 까지는 두 계단, 곧 설정값의 두 배만큼 알려야
     * 한다. 한 번만 돌리면 HALF_OPEN 에 멈추고, 컨텍스트를 함께 쓰는 뒤 시험이
     * 그 상태를 읽는다.
     */
    private void 닫혔다고_알린다() {
        int 필요 = ControlPlaneProperties.defaults().capacity().rampDownTicks();
        for (int i = 0; i < 필요 * 2; i++) {
            registry.circuitObserved(1, 0, 0);
        }
        assertThat(registry.circuit()).as("전제 — 완전히 풀렸다")
                .isEqualTo(CircuitState.CLOSED);
    }

    /**
     * 입장 임계. <b>개수가 아니라 score 다</b> (D-8) — 줄 선 사람의 score 까지
     * 올라왔으면 그 사람은 들어간 것이다.
     */
    private double 입장_임계() {
        String raw = redis.opsForValue().get(RedisKeys.admitted(COUPON, 1, 0)).block(WAIT);
        return raw == null ? 0 : Double.parseDouble(raw);
    }

    /**
     * <b>클러스터가 열렸다고 말하면 배분이 멈춘다.</b>
     *
     * <p>이 노드의 로컬 서킷은 닫혀 있다 — 호출이 없으니 그렇다. 그런데도
     * 멈춰야 한다. 배선이 로컬로 되돌아가면 여기가 빨개진다.
     */
    @Test
    @DisplayName("클러스터가_열렸다고_하면_로컬이_닫혀도_안_흘린다")
    void 클러스터가_열렸다고_하면_로컬이_닫혀도_안_흘린다() {
        registry.circuitObserved(1, 1, 0);

        wired.run().block(WAIT);

        assertThat(입장_임계()).as("전역 크레딧이 0 이라 임계가 안 올라간다").isLessThan(100);
    }

    /**
     * <b>클러스터가 닫혔다고 말하면 평소대로 흘린다.</b>
     *
     * <p>한쪽만 재면 두 공급자가 우연히 같은 값일 때 통과한다. 반대 방향도
     * 같이 못 박아야 출처를 잰 것이 된다.
     */
    @Test
    @DisplayName("클러스터가_닫혔으면_평소대로_흘린다")
    void 클러스터가_닫혔으면_평소대로_흘린다() {
        // **한 번으로는 안 풀린다.** 푸는 방향은 연속 관측을 요구한다 — 표
        // 하나가 늦거나 시체가 만료되는 것만으로 전면 개방이 일어나면 안 된다.
        닫혔다고_알린다();

        wired.run().block(WAIT);

        assertThat(입장_임계()).as("하한만큼은 흘러야 한다").isGreaterThanOrEqualTo(100);
    }

}

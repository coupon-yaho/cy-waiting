package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 팩토리는 <b>도달 가능한 상황 하나씩</b>만 만든다.
 *
 * <p>대개 상태도 하나지만 {@code offWithQueue} 는 둘이다 — 런타임을 못 박지 않고
 * {@code (credit, waiting)} 에서 유도하기 때문이다. 그 경계가 생성자의 I3·I3' 과
 * 같은 자리인지는 여기서만 잴 수 있다.
 */
class CouponStateFactoryTest {

    @Test
    @DisplayName("idle_팩토리는_credit_0과_waiting_0을_만든다")
    void idle_팩토리는_credit_0과_waiting_0을_만든다() {
        CouponState s = CouponState.idle(500);

        assertThat(s.runtime()).isEqualTo(RuntimeState.IDLE);
        assertThat(s.credit()).isZero();
        assertThat(s.waiting()).isZero();
        assertThat(s.remainingStock()).isEqualTo(500);
    }

    @Test
    @DisplayName("queueing_팩토리는_대기자가_있는_상태를_만든다")
    void queueing_팩토리는_대기자가_있는_상태를_만든다() {
        CouponState s = CouponState.queueing(100, 500, 3000);

        assertThat(s.runtime()).isEqualTo(RuntimeState.QUEUEING);
        assertThat(s.waiting()).isEqualTo(3000);
    }

    @Test
    @DisplayName("draining_팩토리는_몫이_대기자_이상인_상태를_만든다")
    void draining_팩토리는_몫이_대기자_이상인_상태를_만든다() {
        CouponState s = CouponState.draining(3000, 500, 3000);

        assertThat(s.runtime()).isEqualTo(RuntimeState.DRAINING);
        assertThat(s.credit()).isGreaterThanOrEqualTo(s.waiting());
    }

    @Test
    @DisplayName("closed_팩토리는_재고가_0인_상태를_만든다")
    void closed_팩토리는_재고가_0인_상태를_만든다() {
        CouponState s = CouponState.closed(3000);

        assertThat(s.runtime()).isEqualTo(RuntimeState.CLOSED);
        assertThat(s.remainingStock()).isZero();
    }

    @Test
    @DisplayName("off_팩토리는_대기열이_꺼진_상태를_만든다")
    void off_팩토리는_대기열이_꺼진_상태를_만든다() {
        CouponState s = CouponState.off(500);

        assertThat(s.mode()).isEqualTo(QueueMode.OFF);
        assertThat(s.runtime()).isEqualTo(RuntimeState.IDLE);
    }

    @Test
    @DisplayName("unknown_팩토리는_스냅샷에_없는_쿠폰을_나타낸다")
    void unknown_팩토리는_스냅샷에_없는_쿠폰을_나타낸다() {
        // 미지 쿠폰은 404 로 끊는다. 상태를 만들어 두는 것은 판정이
        // null 을 다루지 않게 하려는 것이지 통과시키려는 게 아니다.
        CouponState s = CouponState.unknown();

        assertThat(s.runtime()).isEqualTo(RuntimeState.CLOSED);
        assertThat(s.remainingStock()).isZero();
        assertThat(s.credit()).isZero();
    }

    @Test
    @DisplayName("always_팩토리는_한산해도_줄을_세우는_상태를_만든다")
    void always_팩토리는_한산해도_줄을_세우는_상태를_만든다() {
        CouponState s = CouponState.always(500);

        assertThat(s.mode()).isEqualTo(QueueMode.ALWAYS);
        assertThat(s.runtime()).isEqualTo(RuntimeState.IDLE);
        assertThat(s.waiting()).isZero();
    }

    @Test
    @DisplayName("offWithQueue_는_경계에서_DRAINING을_고른다")
    void offWithQueue_는_경계에서_DRAINING을_고른다() {
        // **여기가 없으면 경계가 갈려도 스위트가 초록이다.** 생성자 쪽 단언은
        // runtime 을 인자로 받으니 동어반복이라, 경계를 참으로 만드는 유일한
        // 주체인 팩토리에 대고 재야 한다.
        //
        // 갈리면 credit 이 대기자와 정확히 같아지는 순간 — 줄이 다 빠지기 직전,
        // 가장 흔한 정상 전이 — 에 팩토리가 예외를 던진다.
        assertThat(CouponState.offWithQueue(100, 500, 100).runtime())
                .isEqualTo(RuntimeState.DRAINING);
    }

    @Test
    @DisplayName("offWithQueue_는_경계_바로_아래에서_QUEUEING을_고른다")
    void offWithQueue_는_경계_바로_아래에서_QUEUEING을_고른다() {
        // 경계의 반대쪽 짝이다. 이쪽이 없으면 "항상 DRAINING" 으로 바꿔도 안 깨진다.
        assertThat(CouponState.offWithQueue(99, 500, 100).runtime())
                .isEqualTo(RuntimeState.QUEUEING);
    }

    @Test
    @DisplayName("offWithQueue_는_줄이_비었으면_거부한다")
    void offWithQueue_는_줄이_비었으면_거부한다() {
        // **상태는 I4 가 이미 막는다.** 가드가 바꾸는 것은 메시지다 — 없으면
        // I4 가 "waiting 이 0 이면 IDLE 또는 CLOSED" 를 내서, 부른 쪽이 무엇을
        // 잘못했는지를 안 말한다. 이름이 "줄이 있는 OFF" 이므로 비었으면
        // off 를 쓰라고 그 자리에서 말해야 한다.
        assertThatThrownBy(() -> CouponState.offWithQueue(500, 10_000, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offWithQueue");
    }

    /**
     * <b>모드를 인자로 받는 팩토리가 필요한 이유.</b> 줄이 있다고 모드를 바꿔
     * 실으면 운영자 설정이 한 틱을 못 넘긴다 — 항상 대기 쿠폰이 적응형으로
     * 돌아가고, 꺼 둔 쿠폰의 우회도 조용히 멈춘다.
     */
    @Test
    @DisplayName("withQueue_는_받은_모드를_그대로_싣는다")
    void withQueue_는_받은_모드를_그대로_싣는다() {
        assertThat(CouponState.withQueue(QueueMode.ALWAYS, 10, 500, 100).mode())
                .isEqualTo(QueueMode.ALWAYS);
        assertThat(CouponState.withQueue(QueueMode.OFF, 10, 500, 100).mode())
                .isEqualTo(QueueMode.OFF);
        assertThat(CouponState.withQueue(QueueMode.ADAPTIVE, 10, 500, 100).mode())
                .isEqualTo(QueueMode.ADAPTIVE);
    }

    /** 런타임 경계는 {@code offWithQueue} 와 같은 자리다. 갈리면 정상 전이가 막힌다. */
    @Test
    @DisplayName("withQueue_의_경계는_offWithQueue_와_같다")
    void withQueue_의_경계는_offWithQueue_와_같다() {
        assertThat(CouponState.withQueue(QueueMode.ADAPTIVE, 100, 500, 100).runtime())
                .isEqualTo(RuntimeState.DRAINING);
        assertThat(CouponState.withQueue(QueueMode.ADAPTIVE, 99, 500, 100).runtime())
                .isEqualTo(RuntimeState.QUEUEING);
    }

    @Test
    @DisplayName("withQueue_는_빈_줄을_거부한다")
    void withQueue_는_빈_줄을_거부한다() {
        // 줄이 비었으면 IDLE 이고, IDLE 에 credit 이 실리면 I1 이 막는다.
        assertThatThrownBy(() -> CouponState.withQueue(QueueMode.ADAPTIVE, 10, 500, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("withQueue_는_빈_재고를_거부한다")
    void withQueue_는_빈_재고를_거부한다() {
        // 재고가 없는데 줄이 남았으면 매진이다. 여기서 만들면 아무것도 못 받을
        // 줄에 사람을 계속 세우는 상태가 되고, 발행 경로에는 그 길이 없다.
        assertThatThrownBy(() -> CouponState.withQueue(QueueMode.ADAPTIVE, 10, 0, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("closed");
    }

    @Test
    @DisplayName("noQueue_는_받은_모드로_IDLE을_만든다")
    void noQueue_는_받은_모드로_IDLE을_만든다() {
        CouponState s = CouponState.noQueue(QueueMode.OFF, 500);

        assertThat(s.mode()).isEqualTo(QueueMode.OFF);
        assertThat(s.runtime()).isEqualTo(RuntimeState.IDLE);
        assertThat(s.credit()).isZero();
        assertThat(s.waiting()).isZero();
    }
}

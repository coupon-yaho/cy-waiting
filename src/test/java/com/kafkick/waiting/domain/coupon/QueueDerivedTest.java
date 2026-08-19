package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** credit 이 0 이면 나눗셈이 터진다. 한산한 쿠폰이 정확히 그 상태다. */
class QueueDerivedTest {

    @Test
    @DisplayName("credit이_0인데_대기자가_있으면_큐_깊이는_무한이다")
    void credit이_0인데_대기자가_있으면_큐_깊이는_무한이다() {
        // 배수 속도가 0 이면 영원히 안 빠진다. 예외가 아니라 무한이 맞다.
        // CLOSED 가 이 조합의 유일한 도달 경로다 — 매진됐는데 갇힌 사람이 있다.
        assertThat(CouponState.closed(3000).queueDepthSec()).isInfinite();
    }

    @Test
    @DisplayName("한산한_쿠폰은_credit이_0이어도_깊이가_0이다")
    void 한산한_쿠폰은_credit이_0이어도_깊이가_0이다() {
        // I1 과 I4 가 겹쳐 IDLE 은 credit 0 · waiting 0 이다. 나눗셈에 닿지 않는다.
        assertThat(CouponState.idle(500).queueDepthSec()).isZero();
    }

    @Test
    @DisplayName("큐가_비어_있으면_깊이는_0이다")
    void 큐가_비어_있으면_깊이는_0이다() {
        assertThat(CouponState.closed(0).queueDepthSec()).isZero();
    }

    @Test
    @DisplayName("큐_깊이는_대기자를_초당_배수량으로_나눈_값이다")
    void 큐_깊이는_대기자를_초당_배수량으로_나눈_값이다() {
        assertThat(CouponState.queueing(100, 500, 3000).queueDepthSec()).isEqualTo(30.0);
    }

    @Test
    @DisplayName("큐_용량은_허용_최대_ETA와_credit의_곱이다")
    void 큐_용량은_허용_최대_ETA와_credit의_곱이다() {
        assertThat(CouponState.queueing(100, 500, 3000).queueCapacity(60)).isEqualTo(6000);
    }

    @Test
    @DisplayName("credit이_0이면_큐_용량도_0이다")
    void credit이_0이면_큐_용량도_0이다() {
        // 배수할 수 없는데 줄을 받으면 갇힌 사람만 늘어난다.
        assertThat(CouponState.idle(500).queueCapacity(60)).isZero();
    }
}

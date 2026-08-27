package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.coupon.Tunables;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 운영 값의 문턱이 <b>보호 장치와 어긋나지 않는가</b> (6.8.7).
 *
 * <p>값 하나를 튜너블로 빼면 그 값과 짝을 이루던 상수가 따로 남습니다. 둘이
 * 갈리면 운영자가 넣은 값이 존재할 수 없는 상태를 만듭니다.
 */
class TunableBoundsTest {

    /**
     * <b>자리를 놓게 하는 시한보다 길게 못 잡습니다.</b> 그보다 길면 존재할 수
     * 없는 동시 건수를 상한으로 삼는 셈입니다 — 자리는 시한에서 강제로
     * 반납되므로 그 인원은 절대 안 모입니다.
     */
    @Test
    @DisplayName("걸림_시간의_상한은_시한을_안_넘는다")
    void 걸림_시간의_상한은_시한을_안_넘는다() {
        assertThat(Tunables.MAX_INFLIGHT_SECONDS)
                .isLessThanOrEqualTo(AdmissionGatewayFilter.MAX_IN_FLIGHT.toSeconds());
    }

    /**
     * <b>서킷의 느림 임계보다 커야 합니다.</b> 작으면 느려진 뒷단의 요청이 서킷에
     * 집계되기 전에 격벽이 먼저 끊고, 그러면 서킷이 영영 안 열려 회복 경로 자체가
     * 사라집니다 (F3).
     */
    @Test
    @DisplayName("걸림_시간의_하한은_서킷_느림_임계보다_뒤다")
    void 걸림_시간의_하한은_서킷_느림_임계보다_뒤다() {
        // application.yml 의 slow-call-duration-threshold 와 같은 값이다.
        // 갈리면 GatewayWiringTest 가 실배선에서 잡는다.
        assertThat(Tunables.MIN_INFLIGHT_SECONDS).isGreaterThanOrEqualTo(2);
    }

    /**
     * <b>보호를 끄는 값은 못 넣습니다.</b> 0 이면 한산 통과가 통째로 막혀 피크
     * 전량이 큐 등록으로 가고, 1 에 가까우면 차례가 온 사람이 밀립니다.
     */
    @Test
    @DisplayName("한산_몫의_문턱이_양쪽을_막는다")
    void 한산_몫의_문턱이_양쪽을_막는다() {
        assertThat(Tunables.MIN_IDLE_RATIO).isGreaterThan(0);
        assertThat(Tunables.MAX_IDLE_RATIO).isLessThan(1);
    }
}

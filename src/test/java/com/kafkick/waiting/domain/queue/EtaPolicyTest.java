package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 예상 대기 시간.
 *
 * <p><b>순간 배수율로 나누지 않는다.</b> GC 스파이크 한 번에 표시 시간이 두 배가
 * 되면 사용자는 그것을 서비스가 망가진 신호로 읽는다.
 */
class EtaPolicyTest {

    @Test
    @DisplayName("ETA는_EWMA_credit으로_나눈다")
    void ETA는_EWMA_credit으로_나눈다() {
        // 순간 credit 이 100 으로 튀어도 평활화 값 50 으로 나눈다
        assertThat(EtaPolicy.etaSec(1000, 50)).isCloseTo(20, within(0.001));
    }

    @Test
    @DisplayName("배수율이_0_이하면_모름이다")
    void 배수율이_0_이하면_모름이다() {
        // 무한을 그대로 내보내면 표시 계층이 터진다. 모른다고 말한다.
        // 모름은 NaN 이라 자기 자신과도 같지 않다 — 값 비교로는 못 잰다.
        assertThat(EtaPolicy.etaSec(1000, 0)).isNaN();
        assertThat(EtaPolicy.etaSec(1000, -1)).isNaN();
    }

    @Test
    @DisplayName("앞에_아무도_없으면_0초다")
    void 앞에_아무도_없으면_0초다() {
        assertThat(EtaPolicy.etaSec(0, 0)).isZero();
        assertThat(EtaPolicy.etaSec(0, 50)).isZero();
    }

    @Test
    @DisplayName("표시_ETA는_거친_버킷이다")
    void 표시_ETA는_거친_버킷이다() {
        // ±1.5초 오차가 눈에 보이면 안 된다. 초 단위로 보여 주면
        // 1초씩 줄다 멈추는 게 보이고, 그때마다 신뢰를 잃는다.
        assertThat(EtaPolicy.bucket(0)).isEqualTo(EtaDisplay.ALMOST_THERE);
        assertThat(EtaPolicy.bucket(29)).isEqualTo(EtaDisplay.ALMOST_THERE);
        assertThat(EtaPolicy.bucket(30)).isEqualTo(EtaDisplay.ABOUT_A_MINUTE);
        assertThat(EtaPolicy.bucket(89)).isEqualTo(EtaDisplay.ABOUT_A_MINUTE);
        assertThat(EtaPolicy.bucket(90)).isEqualTo(EtaDisplay.ABOUT_FIVE_MINUTES);
        assertThat(EtaPolicy.bucket(449)).isEqualTo(EtaDisplay.ABOUT_FIVE_MINUTES);
        assertThat(EtaPolicy.bucket(450)).isEqualTo(EtaDisplay.OVER_TEN_MINUTES);
    }

    @Test
    @DisplayName("음수_순위는_거부한다")
    void 음수_순위는_거부한다() {
        assertThatThrownBy(() -> EtaPolicy.etaSec(-1, 50))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("배수율을_모르면_가장_넓은_구간이다")
    void 배수율을_모르면_가장_넓은_구간이다() {
        // **계산 중을 안 내린다.** 그 표시는 떠날지 기다릴지 판단할 근거를 안 주고,
        // 화면에 그것만 떠 있으면 서비스가 멈춘 것으로 읽힌다.
        //
        // 짧게 말했다가 오래 기다리게 하는 쪽이 훨씬 나쁘다. 넉넉히 말했다가
        // 일찍 들어가는 것은 반대다.
        assertThat(EtaPolicy.bucket(EtaPolicy.etaSec(1000, 0)))
                .isEqualTo(EtaDisplay.OVER_TEN_MINUTES);
        assertThat(EtaPolicy.bucket(EtaPolicy.UNKNOWN))
                .isEqualTo(EtaDisplay.OVER_TEN_MINUTES);
    }

    @Test
    @DisplayName("모름과_폴링이_같은_값을_같은_뜻으로_읽는다")
    void 모름과_폴링이_같은_값을_같은_뜻으로_읽는다() {
        // **표현이 갈리면 한쪽이 모름을 '아주 가까움' 으로 읽는다.** 그 값이 나오는
        // 조건이 배수가 멈춘 순간이라, 하필 그때 폴링이 가장 짧아진다.
        PollIntervalPolicy 폴링 = PollIntervalPolicy.of(0);
        long 모를_때 = 폴링.intervalSec(EtaPolicy.UNKNOWN, () -> 0.5);
        long 아주_멀_때 = 폴링.intervalSec(100_000, () -> 0.5);

        // **값까지 못 박는다.** 서로 같은지만 보면 둘이 함께 1초로 무너져도 통과한다.
        assertThat(모를_때).isEqualTo(아주_멀_때).isEqualTo(30);
    }

    @Test
    @DisplayName("말이_안_되는_값도_가장_넓은_구간이다")
    void 말이_안_되는_값도_가장_넓은_구간이다() {
        // **모름만 막으면 절반이다.** 음수는 첫 구간에 걸려 "곧 입장" 이 되는데,
        // 그건 이 변경이 없애려던 바로 그 방향이다. 표시도 폴링도 같이 본다.
        assertThat(EtaPolicy.bucket(-1)).isEqualTo(EtaDisplay.OVER_TEN_MINUTES);
        assertThat(EtaPolicy.bucket(Double.NEGATIVE_INFINITY))
                .isEqualTo(EtaDisplay.OVER_TEN_MINUTES);

        PollIntervalPolicy 폴링 = PollIntervalPolicy.of(0);
        assertThat(폴링.intervalSec(-1, () -> 0.5))
                .isEqualTo(폴링.intervalSec(100_000, () -> 0.5));
    }

    @Test
    @DisplayName("경계마다_어느_구간인지_못_박는다")
    void 경계마다_어느_구간인지_못_박는다() {
        // 경계만 늘리고 구간을 안 늘리면 마지막 경계에서 배열 밖을 짚는다.
        // 다만 밟히는지만 보면 경계가 밀려도 안 걸린다 — 값까지 적는다.
        assertThat(EtaPolicy.bucket(0)).isEqualTo(EtaDisplay.ALMOST_THERE);
        assertThat(EtaPolicy.bucket(29)).isEqualTo(EtaDisplay.ALMOST_THERE);
        assertThat(EtaPolicy.bucket(30)).isEqualTo(EtaDisplay.ABOUT_A_MINUTE);
        assertThat(EtaPolicy.bucket(89)).isEqualTo(EtaDisplay.ABOUT_A_MINUTE);
        assertThat(EtaPolicy.bucket(90)).isEqualTo(EtaDisplay.ABOUT_FIVE_MINUTES);
        assertThat(EtaPolicy.bucket(449)).isEqualTo(EtaDisplay.ABOUT_FIVE_MINUTES);
        assertThat(EtaPolicy.bucket(450)).isEqualTo(EtaDisplay.OVER_TEN_MINUTES);
        assertThat(EtaPolicy.bucket(100_000)).isEqualTo(EtaDisplay.OVER_TEN_MINUTES);
    }

    @Test
    @DisplayName("순번이_0_이면_모르는_것이_아니다")
    void 순번이_0_이면_모르는_것이_아니다() {
        // 앞에 아무도 없으면 배수율과 무관하게 곧 들어간다.
        assertThat(EtaPolicy.bucket(EtaPolicy.etaSec(0, 0)))
                .isEqualTo(EtaDisplay.ALMOST_THERE);
    }

    /**
     * <b>모른다는 것을 0 으로 말하면 안 된다.</b> 와이어는 숫자 하나뿐이라
     * NaN 을 그대로 실을 수 없는데, {@code (long) Math.max(0, NaN)} 은 0 이
     * 되어 "곧 입장" 이 된다 — 하필 배수가 멎은 구간에서 그렇게 나간다.
     */
    @Test
    @DisplayName("모르면_가장_넓은_구간의_초를_준다")
    void 모르면_가장_넓은_구간의_초를_준다() {
        assertThat(EtaPolicy.reportSec(EtaPolicy.UNKNOWN)).isEqualTo(450);
        assertThat(EtaPolicy.reportSec(-1)).isEqualTo(450);
        // 아주 작은 배수율에 큰 순번이면 넘친다. 그대로 캐스팅하면 Long.MAX_VALUE 다.
        assertThat(EtaPolicy.reportSec(Double.POSITIVE_INFINITY)).isEqualTo(450);
    }

    @Test
    @DisplayName("아는_값은_그대로_준다")
    void 아는_값은_그대로_준다() {
        assertThat(EtaPolicy.reportSec(0)).isZero();
        assertThat(EtaPolicy.reportSec(42.7)).isEqualTo(42);
        assertThat(EtaPolicy.reportSec(1_000)).isEqualTo(1_000);
    }
}

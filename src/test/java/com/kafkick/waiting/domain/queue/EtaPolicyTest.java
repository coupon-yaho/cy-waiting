package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
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
    @DisplayName("credit이_0이면_계산_중을_반환한다")
    void credit이_0이면_계산_중을_반환한다() {
        // 무한을 그대로 내보내면 표시 계층이 터진다. 모른다고 말한다.
        assertThat(EtaPolicy.etaSec(1000, 0)).isEqualTo(EtaPolicy.UNKNOWN);
        assertThat(EtaPolicy.etaSec(1000, -1)).isEqualTo(EtaPolicy.UNKNOWN);
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
    @DisplayName("계산_중은_별도_버킷이다")
    void 계산_중은_별도_버킷이다() {
        // "10분 이상" 과 "모른다" 를 뭉치면 사용자가 떠날지 판단할 수 없다.
        assertThat(EtaPolicy.bucket(EtaPolicy.UNKNOWN)).isEqualTo(EtaDisplay.CALCULATING);
    }

    @Test
    @DisplayName("음수_순위는_거부한다")
    void 음수_순위는_거부한다() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> EtaPolicy.etaSec(-1, 50))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 스위퍼가 <b>돌면 안 되는 구간</b>을 정한다 (7.4.8·7.4.9 · 5.4절).
 *
 * <p>이 판단이 스위퍼와 분리돼 있는 이유는, 잘못 쓸면 되돌릴 수 없기 때문입니다.
 * 성실히 줄 선 사람을 이탈자로 판정하면 재입장은 새 score 이고 그건 순번 역행입니다.
 */
class SweepGateTest {

    private static final String COUPON = "c1";

    /** 틱 1초, 생존 신호 90초 → 재개 유예 150틱. */
    private static final int 재개_유예 = 150;

    private SweepGate 게이트() {
        return SweepGate.of(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl());
    }

    /** 유예만큼 틱을 흘린다. */
    private void 유예를_흘린다(SweepGate gate, Map<String, CouponState> coupons) {
        for (int i = 0; i < 재개_유예; i++) {
            gate.sweepable(coupons, false);
        }
    }

    private Map<String, CouponState> 줄이_선_쿠폰() {
        return Map.of(COUPON, CouponStates.queueing(10, 1_000, 100));
    }

    private Map<String, CouponState> 매진() {
        return Map.of(COUPON, CouponStates.closed(100));
    }

    /** 평시에는 쓴다. 안 그러면 이탈자가 영영 줄에 남아 크레딧을 허공에 발행한다. */
    @Test
    @DisplayName("평시에는_쓸어낸다")
    void 평시에는_쓸어낸다() {
        assertThat(게이트().sweepable(줄이_선_쿠폰(), false)).containsExactly(COUPON);
    }

    /**
     * <b>낡음 중에는 안 씁니다</b> (7.4.8).
     *
     * <p>장애 중에는 폴링이 실패해 생존 신호 갱신이 끊깁니다. 그 상태로 쓸면
     * 성실히 줄 선 사람이 이탈자로 판정되고, 재입장은 새 score 입니다 —
     * <b>장애가 아니라 우리 청소 로직이 RC5 를 깹니다.</b>
     */
    @Test
    @DisplayName("재료가_낡으면_안_쓴다")
    void 재료가_낡으면_안_쓴다() {
        assertThat(게이트().sweepable(줄이_선_쿠폰(), true)).isEmpty();
    }

    /**
     * <b>회복 직후 한 틱을 건너뜁니다</b> (7.4.9).
     *
     * <p>낡음이 풀린 그 순간은 밀렸던 폴링이 아직 안 왔습니다. 바로 쓸면
     * 장애 중 안 쓴 것이 회복 첫 틱에 한꺼번에 나갑니다.
     */
    @Test
    @DisplayName("회복_뒤_유예만큼_건너뛴다")
    void 회복_뒤_유예만큼_건너뛴다() {
        SweepGate gate = 게이트();
        gate.sweepable(줄이_선_쿠폰(), true);

        // **한 틱이 아니다.** 생존 신호는 마지막 폴링에서 90초 살고 그 폴링은
        // 최대 60초 뒤에 온다. 그 합보다 짧게 재개하면 아직 신호를 못 채운
        // 사람을 이탈자로 판정한다.
        for (int i = 0; i < 재개_유예 - 1; i++) {
            assertThat(gate.sweepable(줄이_선_쿠폰(), false))
                    .as("%d 번째 틱".formatted(i + 1)).isEmpty();
        }

        assertThat(gate.sweepable(줄이_선_쿠폰(), false)).as("유예 뒤").containsExactly(COUPON);
    }

    /**
     * <b>매진 중에는 안 씁니다</b> (5.4절).
     *
     * <p>7.1 이 매진 조회를 게이트웨이에서 종결하면서 그 쿠폰의 폴링은 생존
     * 신호를 갱신하지 않습니다. 갱신처가 거기 하나뿐이라, 매진으로 보이는 동안
     * <b>줄 선 전원의 신호가 일제히 멎습니다.</b> 장애 문단과 같은 사슬입니다.
     */
    @Test
    @DisplayName("매진_중에는_안_쓴다")
    void 매진_중에는_안_쓴다() {
        assertThat(게이트().sweepable(매진(), false)).isEmpty();
    }

    /**
     * <b>매진이 풀려도 한 틱을 건너뜁니다.</b>
     *
     * <p>재입고된 그 순간도 폴링이 아직 안 왔습니다 — 낡음 회복과 같은 이유입니다.
     */
    @Test
    @DisplayName("매진이_풀려도_유예만큼_건너뛴다")
    void 매진이_풀려도_유예만큼_건너뛴다() {
        SweepGate gate = 게이트();
        gate.sweepable(매진(), false);

        assertThat(gate.sweepable(줄이_선_쿠폰(), false)).as("재입고 첫 틱").isEmpty();

        유예를_흘린다(gate, 줄이_선_쿠폰());

        assertThat(gate.sweepable(줄이_선_쿠폰(), false)).as("유예 뒤").containsExactly(COUPON);
    }

    /**
     * <b>재개 유예를 값으로 못 박습니다.</b>
     *
     * <p>생존 신호 수명이나 폴링 간격이 바뀌면 이 값도 같이 움직여야 합니다 —
     * 리터럴로 두면 관계가 깨진 채로 통과합니다.
     */
    @Test
    @DisplayName("재개_유예가_신호_수명과_폴링_간격의_합이다")
    void 재개_유예가_신호_수명과_폴링_간격의_합이다() {
        SweepGate gate = SweepGate.of(Duration.ofSeconds(1), PollIntervalPolicy.aliveTtl());
        gate.sweepable(매진(), false);

        long 필요 = PollIntervalPolicy.aliveTtl().plus(PollIntervalPolicy.maxInterval())
                .toSeconds();
        for (int i = 0; i < 필요 - 1; i++) {
            assertThat(gate.sweepable(줄이_선_쿠폰(), false))
                    .as("%d 번째 틱".formatted(i + 1)).isEmpty();
        }

        assertThat(gate.sweepable(줄이_선_쿠폰(), false)).containsExactly(COUPON);
    }

    /** 쿠폰마다 따로 셉니다. 한 쿠폰의 매진이 다른 쿠폰의 청소를 멈추면 안 됩니다. */
    @Test
    @DisplayName("쿠폰마다_따로_센다")
    void 쿠폰마다_따로_센다() {
        SweepGate gate = 게이트();

        List<String> 쓸_것 = gate.sweepable(Map.of(
                COUPON, CouponStates.closed(100),
                "c2", CouponStates.queueing(10, 1_000, 100)), false);

        assertThat(쓸_것).containsExactly("c2");
    }

    /** 줄이 빈 쿠폰은 쓸 것이 없습니다. 왕복을 아낍니다. */
    @Test
    @DisplayName("줄이_비면_쓸_것이_없다")
    void 줄이_비면_쓸_것이_없다() {
        assertThat(게이트().sweepable(Map.of(COUPON, CouponStates.idle(1_000)), false))
                .isEmpty();
    }

    /**
     * <b>스위퍼는 이 판단 없이 만들 수 없습니다.</b>
     *
     * <p>계획이 산문으로 "매진 중에는 멈춘다" 를 적어 둬도 기계적 장치가 없으면
     * 다음 사람이 그 줄을 빠뜨립니다. 생성자가 필수 인자로 받게 해서, 빠뜨리면
     * <b>컴파일이 안 되게</b> 합니다.
     */
    @Test
    @DisplayName("판단_없이는_스위퍼를_못_만든다")
    void 판단_없이는_스위퍼를_못_만든다() {
        assertThatThrownBy(() -> QueueSweeper.of(null, (ids, limit) -> null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("gate");
    }
}

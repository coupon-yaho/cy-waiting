package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * 불변식은 문서가 아니라 생성자가 지킨다.
 *
 * <p>이전 구현이 무너진 이유가 <b>픽스처가 존재할 수 없는 상태를 만들 수 있었던
 * 것</b>이다. {@code (IDLE, credit=1000)} 같은 조합을 찍어낼 수 있었고, 그 상태에서는
 * 버그가 드러나지 않았다.
 */
class CouponStateTest {

    // 규칙의 근거는 바깥 인스턴스 누수인데, 테스트 인스턴스는 실행 후 버려져 해당 없다.
    @Nested
    @DisplayName("I1 — 한산한 쿠폰은 배분받은 몫이 없다")
    class I1 {

        @Test
        @DisplayName("IDLE_상태에서_credit이_0이_아니면_생성에_실패한다")
        void IDLE_상태에서_credit이_0이_아니면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 1000, 500, 0, 1.0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I1]");
        }

        @Test
        @DisplayName("IDLE_이고_credit이_0이면_생성된다")
        void IDLE_이고_credit이_0이면_생성된다() {
            CouponState s = new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, 500, 0, 1.0);

            assertThat(s.runtime()).isEqualTo(RuntimeState.IDLE);
            assertThat(s.credit()).isZero();
        }
    }

    // 규칙의 근거는 바깥 인스턴스 누수인데, 테스트 인스턴스는 실행 후 버려져 해당 없다.
    @Nested
    @DisplayName("I2 — 종결된 쿠폰에는 재고가 없다")
    class I2 {

        @Test
        @DisplayName("CLOSED_인데_재고가_남아_있으면_생성에_실패한다")
        void CLOSED_인데_재고가_남아_있으면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.CLOSED, 0, 10, 5, 1.0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I2]");
        }
    }

    // 규칙의 근거는 바깥 인스턴스 누수인데, 테스트 인스턴스는 실행 후 버려져 해당 없다.
    @Nested
    @DisplayName("I3 — 배수 중이면 남은 대기자를 다 뺄 수 있다")
    class I3 {

        @Test
        @DisplayName("DRAINING_인데_credit이_대기자보다_적으면_생성에_실패한다")
        void DRAINING_인데_credit이_대기자보다_적으면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.DRAINING, 10, 500, 100, 1.0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I3]");
        }

        @Test
        @DisplayName("QUEUEING_인데_다_뺄_수_있으면_생성에_실패한다")
        void QUEUEING_인데_다_뺄_수_있으면_생성에_실패한다() {
            // **반대 방향도 막아야 불변식이다.** 한쪽만 보면 같은 (credit,
            // waiting) 조합이 DRAINING 으로도 QUEUEING 으로도 만들어진다.
            // 판정기는 runtime != IDLE 을 ENQUEUE_BACKLOG 로 보므로, 이번 틱에
            // 다 뺄 수 있는 줄인데도 계속 줄을 세운다.
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.QUEUEING, 500, 10_000, 100, 1.0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I3']");
        }

        @Test
        @DisplayName("경계는_같을_때다_생성자는_DRAINING만_받는다")
        void 경계는_같을_때다_생성자는_DRAINING만_받는다() {
            // 같으면 다 뺄 수 있다. 그러니 QUEUEING 이 아니다 — 경계를 어디에
            // 두는지가 두 방향에서 같아야 한 조합이 한 상태만 갖는다.
            assertThatCode(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.DRAINING, 100, 500, 100, 1.0))
                    .doesNotThrowAnyException();
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.QUEUEING, 100, 500, 100, 1.0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I3']");
        }
    }

    // 규칙의 근거는 바깥 인스턴스 누수인데, 테스트 인스턴스는 실행 후 버려져 해당 없다.
    @Nested
    @DisplayName("I4 — 줄이 비었으면 큐 상태일 수 없다")
    class I4 {

        @Test
        @DisplayName("대기자가_0인데_QUEUEING이면_생성에_실패한다")
        void 대기자가_0인데_QUEUEING이면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.QUEUEING, 100, 500, 0, 1.0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I4]");
        }

        @Test
        @DisplayName("대기자가_0이고_CLOSED면_생성된다")
        void 대기자가_0이고_CLOSED면_생성된다() {
            CouponState s = new CouponState(QueueMode.ADAPTIVE, RuntimeState.CLOSED, 0, 0, 0, 1.0);

            assertThat(s.runtime()).isEqualTo(RuntimeState.CLOSED);
            assertThat(s.waiting()).isZero();
        }
    }

    // 규칙의 근거는 바깥 인스턴스 누수인데, 테스트 인스턴스는 실행 후 버려져 해당 없다.
    @Nested
    @DisplayName("I6 — 폴링 배수는 1 미만으로 내려가지 않는다")
    class I6 {

        @Test
        @DisplayName("pollScale이_1미만이면_1로_정규화된다")
        void pollScale이_1미만이면_1로_정규화된다() {
            // 거부가 아니라 정규화다. 1 미만은 폴링을 더 자주 하라는 뜻이 되는데
            // 그건 예산을 늘리는 방향이라 의미가 없다.
            assertThat(new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, 500, 0, 0.3).pollScale())
                    .isEqualTo(1.0);
        }

        @Test
        @DisplayName("pollScale이_1이상이면_그대로_둔다")
        void pollScale이_1이상이면_그대로_둔다() {
            assertThat(new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, 500, 0, 2.5).pollScale())
                    .isEqualTo(2.5);
        }
    }

    // 규칙의 근거는 바깥 인스턴스 누수인데, 테스트 인스턴스는 실행 후 버려져 해당 없다.
    @Nested
    @DisplayName("IDLE 은 줄이 없다")
    class IdleHasNoQueue {

        @Test
        @DisplayName("IDLE인데_대기자가_있으면_생성에_실패한다")
        void IDLE인데_대기자가_있으면_생성에_실패한다() {
            // I4 의 대우로는 이 조합이 안 막힌다. 그대로 두면 판정 8번이
            // 통과시켜 줄 선 사람을 추월한다.
            assertThatThrownBy(
                            () -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, 500, 5000, 1.0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I1']");
        }
    }

    // 규칙의 근거는 바깥 인스턴스 누수인데, 테스트 인스턴스는 실행 후 버려져 해당 없다.
    @Nested
    @DisplayName("pollScale 유한값")
    class PollScaleFinite {

        @Test
        @DisplayName("pollScale이_NaN이면_생성에_실패한다")
        void pollScale이_NaN이면_생성에_실패한다() {
            // NaN 은 비교가 전부 false 라 Math.max 를 그냥 통과한다.
            assertThatThrownBy(
                            () -> new CouponState(
                                    QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, 500, 0, Double.NaN))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    // 규칙의 근거는 바깥 인스턴스 누수인데, 테스트 인스턴스는 실행 후 버려져 해당 없다.
    @Nested
    @DisplayName("필수 값")
    class RequiredFields {

        @Test
        @DisplayName("mode가_null이면_생성에_실패한다")
        void mode가_null이면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(null, RuntimeState.IDLE, 0, 500, 0, 1.0))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("runtime이_null이면_생성에_실패한다")
        void runtime이_null이면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, null, 0, 500, 0, 1.0))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    // 규칙의 근거는 바깥 인스턴스 누수인데, 테스트 인스턴스는 실행 후 버려져 해당 없다.
    @Nested
    @DisplayName("음수 방어")
    class NegativeValues {

        @Test
        @DisplayName("재고가_음수면_생성에_실패한다")
        void 재고가_음수면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, -1, 0, 1.0))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("대기자가_음수면_생성에_실패한다")
        void 대기자가_음수면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, 500, -1, 1.0))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("credit이_음수면_생성에_실패한다")
        void credit이_음수면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.QUEUEING, -1, 500, 10, 1.0))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}

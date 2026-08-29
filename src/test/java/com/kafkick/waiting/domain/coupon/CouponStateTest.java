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
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 1000, 500, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I1]");
        }

        @Test
        @DisplayName("IDLE_이고_credit이_0이면_생성된다")
        void IDLE_이고_credit이_0이면_생성된다() {
            CouponState s = new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, 500, 0);

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
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.CLOSED, 0, 10, 5))
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
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.DRAINING, 10, 500, 100))
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
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.QUEUEING, 500, 10_000, 100))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I3']");
        }

        @Test
        @DisplayName("경계는_같을_때다_생성자는_DRAINING만_받는다")
        void 경계는_같을_때다_생성자는_DRAINING만_받는다() {
            // 같으면 다 뺄 수 있다. 그러니 QUEUEING 이 아니다 — 경계를 어디에
            // 두는지가 두 방향에서 같아야 한 조합이 한 상태만 갖는다.
            assertThatCode(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.DRAINING, 100, 500, 100))
                    .doesNotThrowAnyException();
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.QUEUEING, 100, 500, 100))
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
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.QUEUEING, 100, 500, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I4]");
        }

        @Test
        @DisplayName("대기자가_0이고_CLOSED면_생성된다")
        void 대기자가_0이고_CLOSED면_생성된다() {
            CouponState s = new CouponState(QueueMode.ADAPTIVE, RuntimeState.CLOSED, 0, 0, 0);

            assertThat(s.runtime()).isEqualTo(RuntimeState.CLOSED);
            assertThat(s.waiting()).isZero();
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
                            () -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, 500, 5000))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("[I1']");
        }
    }

    // 규칙의 근거는 바깥 인스턴스 누수인데, 테스트 인스턴스는 실행 후 버려져 해당 없다.
    @Nested
    @DisplayName("필수 값")
    class RequiredFields {

        @Test
        @DisplayName("mode가_null이면_생성에_실패한다")
        void mode가_null이면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(null, RuntimeState.IDLE, 0, 500, 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("runtime이_null이면_생성에_실패한다")
        void runtime이_null이면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, null, 0, 500, 0))
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
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, -1, 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("대기자가_음수면_생성에_실패한다")
        void 대기자가_음수면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, 500, -1))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("credit이_음수면_생성에_실패한다")
        void credit이_음수면_생성에_실패한다() {
            assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.QUEUEING, -1, 500, 10))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
    /**
     * <b>재고를 못 읽은 것은 매진이 아니다.</b> 매진으로 읽으면 게이트웨이가
     * 그 쿠폰을 종결하고 정리가 큐를 지운다 — 자동으로 안 낫는 오판이
     * 되돌릴 수 없는 삭제가 된다 (3.1).
     */
    @Test
    @DisplayName("재고_미상은_매진이_아니다")
    void 재고_미상은_매진이_아니다() {
        CouponState 미상 = CouponState.unknownStock(QueueMode.ADAPTIVE, 7, 100);

        assertThat(미상.stockKnown()).as("모른다는 것이 값으로 남는다").isFalse();
        assertThat(미상.soldOut()).as("종결도 삭제도 이 값에 달렸다").isFalse();
    }

    /** 읽은 0 은 그대로 매진이다. 미상을 들이면서 이것이 흔들리면 R3 이 죽는다. */
    @Test
    @DisplayName("읽은_재고_0은_그대로_매진이다")
    void 읽은_재고_0은_그대로_매진이다() {
        assertThat(CouponState.closed(QueueMode.ADAPTIVE, 5).soldOut()).isTrue();
        assertThat(CouponState.noQueue(QueueMode.ADAPTIVE, 0).soldOut()).isTrue();
        assertThat(CouponState.withQueue(QueueMode.ADAPTIVE, 3, 10, 20).soldOut()).isFalse();
    }

    /** 미상을 뜻하는 한 값 말고는 음수를 안 받는다. 열어 두면 오타가 값이 된다. */
    @Test
    @DisplayName("뜻_없는_음수_재고는_거부한다")
    void 뜻_없는_음수_재고는_거부한다() {
        assertThatThrownBy(() -> new CouponState(QueueMode.ADAPTIVE, RuntimeState.IDLE, 0, -2, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>미상이면 종결로 못 간다.</b> I2 가 종결에 재고 0 을 요구하므로 미상은
     * 애초에 그 자리에 못 선다 — 삭제로 가는 길이 자료형에서 막힌다.
     */
    @Test
    @DisplayName("미상은_종결_상태가_될_수_없다")
    void 미상은_종결_상태가_될_수_없다() {
        assertThatThrownBy(() -> new CouponState(
                QueueMode.ADAPTIVE, RuntimeState.CLOSED, 0, CouponState.STOCK_UNKNOWN, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("I2");
    }
}

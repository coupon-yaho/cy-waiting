package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 통과 상한 — 이 제품의 존재 이유(R1).
 *
 * <p>한산한 쿠폰은 {@code credit} 이 0 이다(I1). 상한을 {@code credit} 으로 재면
 * 한산한 쿠폰일수록 반드시 큐로 가는 역전이 생긴다.
 */
class AdmissionCapTest {

    @Test
    @DisplayName("경합_쿠폰의_몫은_credit을_노드수로_나눈_값이다")
    void 경합_쿠폰의_몫은_credit을_노드수로_나눈_값이다() {
        CouponState s = CouponState.queueing(1000, 500, 3000);

        assertThat(s.contendedCap(10)).isEqualTo(100);
    }

    @Test
    @DisplayName("노드수가_0이어도_나눗셈이_터지지_않는다")
    void 노드수가_0이어도_나눗셈이_터지지_않는다() {
        CouponState s = CouponState.queueing(1000, 500, 3000);

        assertThat(s.contendedCap(0)).isEqualTo(1000);
    }

    @Test
    @DisplayName("한산한_쿠폰은_전역_여유를_상한으로_쓴다")
    void 한산한_쿠폰은_전역_여유를_상한으로_쓴다() {
        // credit 은 0 인데 상한은 0 이 아니어야 한다. 이 한 줄이 R1 이다.
        CouponState s = CouponState.idle(500);

        assertThat(s.credit()).isZero();
        assertThat(s.idleCap(new SnapshotMeta(1000, 10), 0.7)).isPositive();
    }

    @Test
    @DisplayName("한산한_쿠폰의_상한은_노드몫에_유휴비율을_곱한_값이다")
    void 한산한_쿠폰의_상한은_노드몫에_유휴비율을_곱한_값이다() {
        CouponState s = CouponState.idle(500);

        // globalCredit 1000 / 노드 10 = 100, × 0.7 = 70
        assertThat(s.idleCap(new SnapshotMeta(1000, 10), 0.7)).isEqualTo(70);
    }

    @Test
    @DisplayName("credit이_노드수보다_작으면_총합이_credit을_넘지_않는다")
    void credit이_노드수보다_작으면_총합이_credit을_넘지_않는다() {
        // credit 10 을 노드 20 이 나눠 가지면 정수 나눗셈으로 전부 0 이 된다.
        // max(1, ...) 로 올리면 20 이 나가 credit 의 두 배가 된다 — 초과 배분이다.
        CouponState s = CouponState.queueing(10, 500, 3000);

        long total = IntStream.range(0, 20).mapToLong(node -> s.contendedCap(20, node)).sum();

        assertThat(total).isLessThanOrEqualTo(10);
    }

    @Test
    @DisplayName("나머지가_있어도_총합이_credit을_넘지_않는다")
    void 나머지가_있어도_총합이_credit을_넘지_않는다() {
        // 임의 조합에서도 성립해야 한다. 초과 배분은 타협 불가다.
        for (long credit : new long[] {0, 1, 7, 10, 99, 1000, 100_000}) {
            for (int nodes : new int[] {1, 3, 7, 20, 100}) {
                CouponState s = credit == 0
                        ? CouponState.idle(500)
                        : CouponState.queueing(credit, 500, 3000);
                long total = IntStream.range(0, nodes)
                        .mapToLong(node -> s.contendedCap(nodes, node))
                        .sum();
                assertThat(total)
                        .withFailMessage("credit=%d nodes=%d total=%d", credit, nodes, total)
                        .isLessThanOrEqualTo(credit);
            }
        }
    }
}

package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 크레딧 초과 배분 속성 테스트 — 타협 불가 기준이다.
 *
 * <p>예시 몇 개로는 못 잡는다. 정수 나눗셈의 나머지 처리는 특정
 * {@code (credit, N)} 조합에서만 어긋난다.
 */
class CreditAllocationPropertyTest {

    /** 무작위지만 재현 가능해야 한다. 실패를 다시 못 만들면 고칠 수 없다. */
    private static final long SEED = 20260819L;

    private static final int TRIALS = 100_000;

    @Test
    @DisplayName("무작위_10만회에서_배분_총합이_credit을_넘지_않는다")
    void 무작위_10만회에서_배분_총합이_credit을_넘지_않는다() {
        Random rnd = new Random(SEED);
        int violations = 0;

        for (int t = 0; t < TRIALS; t++) {
            long credit = rnd.nextLong(0, 200_000);
            int nodes = rnd.nextInt(1, 200);

            CouponState s = credit == 0
                    ? CouponState.idle(500)
                    : CouponState.queueing(credit, 500, credit + 1);

            long total = 0;
            for (int node = 0; node < nodes; node++) {
                total += s.contendedCap(nodes, node);
            }

            if (total > credit) {
                violations++;
            }
        }

        assertThat(violations)
                .withFailMessage("초과 배분 %d 건 (시드 %d)", violations, SEED)
                .isZero();
    }

    @Test
    @DisplayName("무작위_10만회에서_배분이_credit에_최대한_가깝다")
    void 무작위_10만회에서_배분이_credit에_최대한_가깝다() {
        // 넘지 않는 것만으로는 부족하다. 전부 0 을 주면 그것도 "안 넘는다".
        // 나머지를 앞쪽 노드에 나눠 주므로 총합은 credit 과 같아야 한다.
        Random rnd = new Random(SEED);
        int shortfalls = 0;

        for (int t = 0; t < TRIALS; t++) {
            long credit = rnd.nextLong(1, 200_000);
            int nodes = rnd.nextInt(1, 200);

            CouponState s = CouponState.queueing(credit, 500, credit + 1);

            long total = 0;
            for (int node = 0; node < nodes; node++) {
                total += s.contendedCap(nodes, node);
            }

            // 노드가 credit 보다 많으면 뒤쪽은 0 이지만 총합은 여전히 credit 이다
            if (total != credit) {
                shortfalls++;
            }
        }

        assertThat(shortfalls)
                .withFailMessage("총합이 credit 과 다른 경우 %d 건", shortfalls)
                .isZero();
    }
}

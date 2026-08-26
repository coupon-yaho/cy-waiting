package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 동시에 몇 건이 뒷단에 걸려 있는지를 셉니다.
 *
 * <p><b>레이트 리밋으로는 못 막는 것이 있습니다.</b> 초당 100건이어도 각각 10초가
 * 걸리면 동시 1,000건입니다. 리미터는 초당 건수를, 격벽은 동시 건수를 셉니다 —
 * 세는 단위가 다릅니다.
 *
 * <p>느려진 뒷단 한 대가 게이트웨이의 커넥션을 다 붙잡으면, 한산한 쿠폰의 통과
 * 경로까지 같이 죽습니다.
 */
class BulkheadTest {

    private final Bulkhead bulkhead = Bulkhead.withMaxKeys(10);

    /** 상한 안에서는 그대로 들여보냅니다. */
    @Test
    @DisplayName("상한_안에서는_들여보낸다")
    void 상한_안에서는_들여보낸다() {
        assertThat(bulkhead.tryEnter("c1", 2)).isTrue();
        assertThat(bulkhead.tryEnter("c1", 2)).isTrue();
    }

    /** 상한을 넘으면 막습니다. 이 자리가 없으면 커넥션이 무한히 쌓입니다. */
    @Test
    @DisplayName("상한을_넘으면_막는다")
    void 상한을_넘으면_막는다() {
        bulkhead.tryEnter("c1", 2);
        bulkhead.tryEnter("c1", 2);

        assertThat(bulkhead.tryEnter("c1", 2)).isFalse();
    }

    /**
     * <b>나가면 자리가 돌아옵니다.</b> 안 돌려주면 격벽이 한 번 차고 나서 영영
     * 안 열리고, 그 쿠폰은 뒷단이 멀쩡해져도 계속 막힙니다.
     */
    @Test
    @DisplayName("나가면_자리가_돌아온다")
    void 나가면_자리가_돌아온다() {
        bulkhead.tryEnter("c1", 1);

        bulkhead.exit("c1");

        assertThat(bulkhead.tryEnter("c1", 1)).isTrue();
    }

    /**
     * <b>핫 쿠폰이 콜드 쿠폰의 통로를 막으면 안 됩니다.</b> 하나로 세면 몰리는
     * 쿠폰이 자리를 다 쓰고, 한산한 쿠폰이 그 뒤에 밀립니다 — R1 이 뒤집힙니다.
     */
    @Test
    @DisplayName("쿠폰마다_따로_센다")
    void 쿠폰마다_따로_센다() {
        bulkhead.tryEnter("핫", 1);

        assertThat(bulkhead.tryEnter("콜드", 1)).isTrue();
    }

    /**
     * <b>안 들어간 것을 내보내면 안 됩니다.</b> 자리가 음수로 내려가면 그만큼
     * 상한이 늘어나고, 격벽이 있으나 마나가 됩니다.
     */
    @Test
    @DisplayName("안_들어간_것을_내보내도_음수가_안_된다")
    void 안_들어간_것을_내보내도_음수가_안_된다() {
        bulkhead.exit("c1");
        bulkhead.exit("c1");

        assertThat(bulkhead.tryEnter("c1", 1)).isTrue();
        assertThat(bulkhead.tryEnter("c1", 1)).isFalse();
    }

    /**
     * <b>상한이 0 이면 아무도 못 들어갑니다.</b> 크레딧이 0 인 구간이 실제로
     * 있으므로, 그때 전면 차단이 되지 않게 부르는 쪽이 폴백을 정해야 합니다.
     */
    @Test
    @DisplayName("상한이_0이면_아무도_못_들어간다")
    void 상한이_0이면_아무도_못_들어간다() {
        assertThat(bulkhead.tryEnter("c1", 0)).isFalse();
    }

    /**
     * <b>맵이 무한히 자라면 안 됩니다.</b> 쿠폰 식별자는 밖에서 오는 값이라
     * 가짓수에 상한이 없습니다. 리미터와 같은 방식으로 막습니다.
     */
    @Test
    @DisplayName("키가_상한을_넘으면_새_쿠폰을_안_받는다")
    void 키가_상한을_넘으면_새_쿠폰을_안_받는다() {
        for (int i = 0; i < 10; i++) {
            assertThat(bulkhead.tryEnter("c" + i, 5)).isTrue();
        }

        assertThat(bulkhead.tryEnter("넘친다", 5)).isFalse();
    }

    /**
     * <b>비면 자리를 내줍니다.</b> 안 그러면 끝난 쿠폰이 맵을 차지한 채 남아,
     * 새 캠페인이 열릴 때 그 쿠폰이 못 들어갑니다.
     */
    @Test
    @DisplayName("비면_맵에서_빠진다")
    void 비면_맵에서_빠진다() {
        for (int i = 0; i < 10; i++) {
            bulkhead.tryEnter("c" + i, 5);
        }
        bulkhead.exit("c0");

        assertThat(bulkhead.tryEnter("새것", 5)).isTrue();
    }

    /** 지금 몇 건이 걸려 있는지. 지표가 이 값을 읽습니다. */
    @Test
    @DisplayName("걸려_있는_건수를_센다")
    void 걸려_있는_건수를_센다() {
        bulkhead.tryEnter("c1", 3);
        bulkhead.tryEnter("c1", 3);
        bulkhead.tryEnter("c2", 3);

        assertThat(bulkhead.inFlight()).isEqualTo(3);
    }

    /** 키 상한이 0 이하면 아무것도 못 담는다. 그건 설정 실수다. */
    @Test
    @DisplayName("키_상한이_0이하면_거부한다")
    void 키_상한이_0이하면_거부한다() {
        assertThatThrownBy(() -> Bulkhead.withMaxKeys(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

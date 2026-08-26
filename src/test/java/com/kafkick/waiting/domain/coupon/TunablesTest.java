package com.kafkick.waiting.domain.coupon;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 장애 중에 배포 없이 되돌릴 수 있어야 롤백 전략이 성립합니다.
 *
 * <p><b>잘못된 값이 게이트웨이를 멈추면 안 됩니다.</b> 운영자가 장애 중에 손으로
 * 넣는 값이라 오타가 납니다. 그때 기동이 막히면 되돌릴 수단 자체가 사라집니다.
 */
class TunablesTest {

    /** 값을 안 적었을 때 도는 값. 키가 없어도 게이트웨이는 돌아야 합니다. */
    @Test
    @DisplayName("빈_값이면_기본값이_된다")
    void 빈_값이면_기본값이_된다() {
        Tunables t = Tunables.parse(null);

        assertThat(t).isEqualTo(Tunables.defaults());
    }

    /** 적은 값이 그대로 실려야 합니다. 안 실리면 되돌릴 수단이 없는 것과 같습니다. */
    @Test
    @DisplayName("적은_값이_그대로_실린다")
    void 적은_값이_그대로_실린다() {
        Tunables t = Tunables.parse("{\"idleCreditRatio\":0.5,\"inFlightSeconds\":5}");

        assertThat(t.idleCreditRatio()).isEqualTo(0.5);
        assertThat(t.inFlightSeconds()).isEqualTo(5);
    }

    /**
     * <b>깨진 JSON 이 기동을 막으면 안 됩니다.</b> 장애 중에 손으로 넣는 값이라
     * 오타가 나는데, 그때 멈추면 되돌릴 수단 자체가 사라집니다.
     */
    @Test
    @DisplayName("깨진_값이면_기본값으로_떨어진다")
    void 깨진_값이면_기본값으로_떨어진다() {
        assertThat(Tunables.parse("{깨졌다")).isEqualTo(Tunables.defaults());
        assertThat(Tunables.parse("")).isEqualTo(Tunables.defaults());
    }

    /**
     * <b>범위를 벗어난 값도 마찬가지입니다.</b> 한산 비율이 1 을 넘으면 한산 통과가
     * 노드 예산을 넘어서고, 0 이면 그 경로가 통째로 막힙니다.
     */
    @Test
    @DisplayName("범위를_벗어나면_그_값만_기본값이_된다")
    void 범위를_벗어나면_그_값만_기본값이_된다() {
        Tunables t = Tunables.parse("{\"idleCreditRatio\":2.0,\"inFlightSeconds\":5}");

        // 한 값이 틀렸다고 나머지까지 버리지 않는다. 그러면 오타 하나가
        // 운영자가 방금 고친 다른 값도 되돌린다.
        assertThat(t.idleCreditRatio()).isEqualTo(Tunables.defaults().idleCreditRatio());
        assertThat(t.inFlightSeconds()).isEqualTo(5);
    }

    /** 모르는 키는 무시합니다. 새 값을 먼저 넣어 두고 배포하는 순서가 가능해야 합니다. */
    @Test
    @DisplayName("모르는_키는_무시한다")
    void 모르는_키는_무시한다() {
        Tunables t = Tunables.parse("{\"아직없는값\":1,\"inFlightSeconds\":7}");

        assertThat(t.inFlightSeconds()).isEqualTo(7);
    }

    /**
     * <b>통째로 바뀌어야 합니다.</b> 필드별로 갈아 끼우면 낡은 타임아웃과 새 격벽
     * 상한 같은 조합이 한순간 존재하고, 그 조합은 아무도 검증한 적이 없습니다.
     */
    @Test
    @DisplayName("값들이_한_벌로_움직인다")
    void 값들이_한_벌로_움직인다() {
        Tunables 하나 = Tunables.parse("{\"idleCreditRatio\":0.5,\"inFlightSeconds\":5}");
        Tunables 둘 = Tunables.parse("{\"idleCreditRatio\":0.5,\"inFlightSeconds\":5}");

        assertThat(하나).isEqualTo(둘);
    }
}

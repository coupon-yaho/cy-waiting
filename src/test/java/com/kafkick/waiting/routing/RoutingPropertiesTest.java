package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 라우팅 노브.
 *
 * <p><b>두 전략을 다 만든다</b> (R-9). 어느 쪽이 나은지는 실측으로 정할 문제라,
 * 코드에 하나만 박아 두면 그 측정을 할 수가 없다.
 */
@Tag("unit")
class RoutingPropertiesTest {

    /** 목적지 제한. 켜진 설정은 이것이 없으면 못 선다. */
    private static final List<String> 허용 = List.of(".internal");

    /** 목적지와 짝이다. 포트를 안 막으면 허용한 망 안의 아무 서비스나 대상이 된다. */
    private static final List<Integer> 허용_포트 = List.of(9000);

    private static RoutingProperties 값(String strategy) {
        return new RoutingProperties(true, null, strategy, null, null, null, null, null,
                허용, 허용_포트);
    }

    @Test
    @DisplayName("안_적으면_기본값이_선다")
    void 안_적으면_기본값이_선다() {
        RoutingProperties p = new RoutingProperties(true, null, null, null, null, null, null, null,
                허용, 허용_포트);

        assertThat(p.serviceId()).isEqualTo("coupon-service");
        // **라운드로빈이 기본이다.** 게이트웨이 둘에서 잰 값이 그쪽을 가리켰다
        // (CY-916). 이 줄이 배포에 실제로 서는 전략을 못 박는다.
        assertThat(p.strategy()).isEqualTo(RoutingProperties.ROUND_ROBIN);
        assertThat(p.inFlightTtl()).isEqualTo(Duration.ofSeconds(30));
        assertThat(p.coldStartRamp()).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    @DisplayName("두_전략을_다_받는다")
    void 두_전략을_다_받는다() {
        assertThat(값(RoutingProperties.P2C).strategy()).isEqualTo("p2c");
        assertThat(값(RoutingProperties.ROUND_ROBIN).strategy()).isEqualTo("round-robin");
    }

    /**
     * <b>모르는 전략을 기본값으로 접지 않는다.</b> 오타 하나로 다른 전략이 돌면
     * 그 배포의 측정이 통째로 다른 것을 잰 것이 된다.
     */
    @Test
    @DisplayName("모르는_전략은_거절한다")
    void 모르는_전략은_거절한다() {
        assertThatThrownBy(() -> 값("p2c-v2")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("수명과_램프의_범위를_본다")
    void 수명과_램프의_범위를_본다() {
        assertThatThrownBy(() -> new RoutingProperties(true, null, null, Duration.ZERO,
                null, null, null, null, 허용, 허용_포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RoutingProperties(true, null, null,
                Duration.ofSeconds(-1), null, null, null, null,
                허용, 허용_포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RoutingProperties(true, null, null, null,
                Duration.ofSeconds(-1), null, null, null,
                허용, 허용_포트))
                .isInstanceOf(IllegalArgumentException.class);
        // 램프 0 은 되돌리기를 안 하겠다는 뜻이다. 끄는 길을 막지 않는다.
        assertThat(new RoutingProperties(true, null, null, null, Duration.ZERO,
                null, null, null,
                허용, 허용_포트).coldStartRamp())
                .isZero();
    }

    @Test
    @DisplayName("빈_이름은_기본값으로_본다")
    void 빈_이름은_기본값으로_본다() {
        assertThat(new RoutingProperties(true, "  ", "  ", null, null, null, null, null,
                허용, 허용_포트).serviceId())
                .isEqualTo("coupon-service");
    }

    /**
     * <b>느려진 한 대가 커넥션을 독식하지 못하게 한다</b> (G9.13). 상한이 0 이면
     * 라우팅이 통째로 막히고, 그건 상한을 둔 이유와 반대다.
     */
    @Test
    @DisplayName("상한이_양수가_아니면_거절한다")
    void 상한이_양수가_아니면_거절한다() {
        assertThatThrownBy(() -> new RoutingProperties(true, null, null, null, null, 0, null, null,
                허용, 허용_포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RoutingProperties(true, null, null, null, null, -1, null, null,
                허용, 허용_포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("상한을_안_적으면_기본값이다")
    void 상한을_안_적으면_기본값이다() {
        assertThat(new RoutingProperties(true, null, null, null, null, null, null, null,
                허용, 허용_포트).perInstanceCap())
                .isEqualTo(200);
    }

    /** <b>하한 자체는 받는다.</b> 안 받으면 하한을 적은 배포가 뜨지도 못하고 죽는다. */
    @Test
    @DisplayName("하한_값은_받는다")
    void 하한_값은_받는다() {
        RoutingProperties 하한 =
                new RoutingProperties(true, null, null, null, null, 1, 1, null,
                허용, 허용_포트);

        assertThat(하한.perInstanceCap()).isEqualTo(1);
        assertThat(하한.outlierFailures()).isEqualTo(1);
    }

    /**
     * <b>0 이면 어쩌다 난 오류 한 건에 인스턴스가 빠진다.</b> 그 몫이 남은 대로
     * 몰려 멀쩡한 대까지 밀려 넘어진다.
     */
    @Test
    @DisplayName("연속_실패_임계가_양수가_아니면_거절한다")
    void 연속_실패_임계가_양수가_아니면_거절한다() {
        assertThatThrownBy(
                () -> new RoutingProperties(true, null, null, null, null, null, 0, null,
                허용, 허용_포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                () -> new RoutingProperties(true, null, null, null, null, null, -1, null,
                허용, 허용_포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>배제 시간이 0 이면 배제가 아니다.</b> 뺀 그 자리에서 다시 후보가 되어,
     * 앓는 대를 빼는 장치가 이름만 남는다.
     */
    @Test
    @DisplayName("배제_시간이_양수가_아니면_거절한다")
    void 배제_시간이_양수가_아니면_거절한다() {
        assertThatThrownBy(() -> new RoutingProperties(
                true, null, null, null, null, null, null, Duration.ZERO,
                허용, 허용_포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RoutingProperties(
                true, null, null, null, null, null, null, Duration.ofSeconds(-1),
                허용, 허용_포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>켤 때는 목적지를 적어야 한다</b> (CY-887). 뒷단이 보고한 주소가 곧 연결
     * 대상이라, 목록이 비었다는 것이 "아무 데나 보내도 된다" 로 읽히면 그 배포가
     * 그대로 통로가 된다.
     */
    @Test
    @DisplayName("켤_때_목적지가_비면_안_뜬다")
    void 켤_때_목적지가_비면_안_뜬다() {
        assertThatThrownBy(() -> new RoutingProperties(true, null, null, null,
                null, null, null, null, List.of(), 허용_포트))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowed-destinations");
        assertThatThrownBy(() -> new RoutingProperties(true, null, null, null,
                null, null, null, null, null, 허용_포트))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowed-destinations");
        // 공백만 든 항목도 빈 것으로 본다. 안 그러면 레코드를 지나 뒤에서 터진다.
        assertThatThrownBy(() -> new RoutingProperties(true, null, null, null,
                null, null, null, null, List.of(" "), 허용_포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>포트도 짝으로 막는다</b> (CY-887). 호스트만 보면 허용한 망 안의 아무
     * 서비스나 연결 대상이 된다.
     */
    @Test
    @DisplayName("켤_때_포트가_비면_안_뜬다")
    void 켤_때_포트가_비면_안_뜬다() {
        assertThatThrownBy(() -> new RoutingProperties(true, null, null, null,
                null, null, null, null, 허용, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowed-ports");
        assertThatThrownBy(() -> new RoutingProperties(true, null, null, null,
                null, null, null, null, 허용, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowed-ports");
    }

    /** 꺼진 배포까지 요구하면 라우팅과 무관한 배포가 이 설정 때문에 안 뜬다. */
    @Test
    @DisplayName("꺼져_있으면_안_적어도_뜬다")
    void 꺼져_있으면_안_적어도_뜬다() {
        RoutingProperties p = new RoutingProperties(false, null, null, null,
                null, null, null, null, null, null);

        assertThat(p.allowedDestinations()).isEmpty();
        assertThat(p.allowedPorts()).isEmpty();
    }
}

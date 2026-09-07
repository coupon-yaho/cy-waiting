package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 뒷단이 보고한 주소를 실제로 연결해도 되는가.
 *
 * <p><b>모양 검사와 다른 질문이다.</b> {@link InstanceAddress} 는 읽을 수 있는
 * 주소인지만 보고, 여기는 그 주소가 우리 뒷단의 자리인지를 본다. 보고에 쓸 수
 * 있는 쪽이 게이트웨이를 임의 주소로 향하게 하는 것을 막는 유일한 자리다.
 */
@Tag("unit")
class AllowedDestinationsTest {

    private static InstanceAddress 주소(String raw) {
        return InstanceAddress.parse(raw).orElseThrow();
    }

    @Test
    @DisplayName("접미사가_맞으면_받는다")
    void 접미사가_맞으면_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of(".internal"));

        assertThat(허용.permits(주소("coupon-be-3.internal:9000"))).isTrue();
    }

    /** 접미사는 라벨 경계에서 끊긴다. 안 그러면 `evil-internal` 이 `.internal` 로 통과한다. */
    @Test
    @DisplayName("라벨_경계를_안_지키면_거절한다")
    void 라벨_경계를_안_지키면_거절한다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of(".internal"));

        assertThat(허용.permits(주소("evil-internal:9000"))).isFalse();
        assertThat(허용.permits(주소("coupon.internal.evil.com:9000"))).isFalse();
    }

    /** 접미사 자체와 같은 이름도 그 망의 자리다. `.internal` 은 `internal` 을 받는다. */
    @Test
    @DisplayName("접미사와_같은_이름도_받는다")
    void 접미사와_같은_이름도_받는다() {
        assertThat(AllowedDestinations.of(List.of(".internal")).permits(주소("internal:9000")))
                .isTrue();
    }

    @Test
    @DisplayName("대소문자를_안_가린다")
    void 대소문자를_안_가린다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of(".Internal"));

        assertThat(허용.permits(주소("Coupon-BE.INTERNAL:9000"))).isTrue();
    }

    @Test
    @DisplayName("대역_안의_주소를_받는다")
    void 대역_안의_주소를_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.1.0/24"));

        assertThat(허용.permits(주소("10.0.1.7:8080"))).isTrue();
        assertThat(허용.permits(주소("10.0.2.7:8080"))).isFalse();
    }

    /**
     * <b>이름이 대역을 통과하면 안 된다.</b> 통과시키려면 이름을 풀어야 하고,
     * 그 조회 결과는 다음 순간 달라질 수 있다 — 검사 때와 연결 때가 갈린다.
     */
    @Test
    @DisplayName("이름은_대역으로_안_통과한다")
    void 이름은_대역으로_안_통과한다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.1.0/24"));

        assertThat(허용.permits(주소("10.0.1.7.nip.io:8080"))).isFalse();
    }

    /**
     * <b>빈 목록은 전부 허용이 아니다.</b> 안 적었다는 것이 아무 데나 보내도 된다는
     * 뜻이 되면, 설정을 빠뜨린 배포가 그대로 통로가 된다.
     */
    @Test
    @DisplayName("빈_목록은_만들_수_없다")
    void 빈_목록은_만들_수_없다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>못 읽는 항목은 기동에서 끊는다.</b> 조용히 버리면 오타 하나로 허용 범위가
     * 좁아지고, 그건 라우팅이 조용히 비는 것으로 나타난다.
     */
    @Test
    @DisplayName("못_읽는_항목은_거절한다")
    void 못_읽는_항목은_거절한다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("10.0.1.0/99")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 여러 항목 중 하나만 맞아도 받는다. */
    @Test
    @DisplayName("항목이_여럿이면_하나만_맞아도_받는다")
    void 항목이_여럿이면_하나만_맞아도_받는다() {
        AllowedDestinations 허용 =
                AllowedDestinations.of(List.of(".internal", "10.0.1.0/24"));

        assertThat(허용.permits(주소("a.internal:1"))).isTrue();
        assertThat(허용.permits(주소("10.0.1.9:1"))).isTrue();
        assertThat(허용.permits(주소("example.com:1"))).isFalse();
    }
}

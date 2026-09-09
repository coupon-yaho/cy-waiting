package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
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

    /** 목적지 제한이 없는 상태. 이름으로 남겨야 인자를 빠뜨린 것과 안 헷갈린다. */
    private static final AllowedDestinations 무제한 = AllowedDestinations.unrestricted();

    /** 시험이 쓰는 포트. 목적지와 짝으로 막지 않으면 호스트 제한이 반쪽이다. */
    private static final List<Integer> 포트 = List.of(9000, 8080, 1);

    private static InstanceAddress 주소(String raw) {
        return InstanceAddress.parse(raw).orElseThrow();
    }

    @Test
    @DisplayName("접미사가_맞으면_받는다")
    void 접미사가_맞으면_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of(".internal"), 포트);

        assertThat(허용.permits(주소("coupon-be-3.internal:9000"))).isTrue();
    }

    /** 접미사는 라벨 경계에서 끊긴다. 안 그러면 `evil-internal` 이 `.internal` 로 통과한다. */
    @Test
    @DisplayName("라벨_경계를_안_지키면_거절한다")
    void 라벨_경계를_안_지키면_거절한다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of(".internal"), 포트);

        assertThat(허용.permits(주소("evil-internal:9000"))).isFalse();
        assertThat(허용.permits(주소("coupon.internal.evil.com:9000"))).isFalse();
    }

    /** 접미사 자체와 같은 이름도 그 망의 자리다. `.internal` 은 `internal` 을 받는다. */
    @Test
    @DisplayName("접미사와_같은_이름도_받는다")
    void 접미사와_같은_이름도_받는다() {
        assertThat(AllowedDestinations.of(List.of(".internal"), 포트).permits(주소("internal:9000")))
                .isTrue();
    }

    @Test
    @DisplayName("대소문자를_안_가린다")
    void 대소문자를_안_가린다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of(".Internal"), 포트);

        assertThat(허용.permits(주소("Coupon-BE.INTERNAL:9000"))).isTrue();
    }

    @Test
    @DisplayName("대역_안의_주소를_받는다")
    void 대역_안의_주소를_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.1.0/24"), 포트);

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
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.1.0/24"), 포트);

        assertThat(허용.permits(주소("10.0.1.7.nip.io:8080"))).isFalse();
    }

    /**
     * <b>빈 목록은 전부 허용이 아니다.</b> 안 적었다는 것이 아무 데나 보내도 된다는
     * 뜻이 되면, 설정을 빠뜨린 배포가 그대로 통로가 된다.
     */
    @Test
    @DisplayName("빈_목록은_만들_수_없다")
    void 빈_목록은_만들_수_없다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of(), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(null, 포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>못 읽는 항목은 기동에서 끊는다.</b> 조용히 버리면 오타 하나로 허용 범위가
     * 좁아지고, 그건 라우팅이 조용히 비는 것으로 나타난다.
     */
    @Test
    @DisplayName("못_읽는_항목은_거절한다")
    void 못_읽는_항목은_거절한다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("10.0.1.0/99"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of(""), 포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 여러 항목 중 하나만 맞아도 받는다. */
    @Test
    @DisplayName("항목이_여럿이면_하나만_맞아도_받는다")
    void 항목이_여럿이면_하나만_맞아도_받는다() {
        AllowedDestinations 허용 =
                AllowedDestinations.of(List.of(".internal", "10.0.1.0/24"), 포트);

        assertThat(허용.permits(주소("a.internal:1"))).isTrue();
        assertThat(허용.permits(주소("10.0.1.9:1"))).isTrue();
        assertThat(허용.permits(주소("example.com:1"))).isFalse();
    }

    /** 접미사에 점을 안 붙이면 그 이름만 받는다. 하위 이름까지 여는 것은 다른 뜻이다. */
    @Test
    @DisplayName("점_없는_항목은_그_이름만_받는다")
    void 점_없는_항목은_그_이름만_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("coupon-be"), 포트);

        assertThat(허용.permits(주소("coupon-be:9000"))).isTrue();
        assertThat(허용.permits(주소("a.coupon-be:9000"))).isFalse();
    }

    /** 대역 표기가 숫자가 아니거나 주소가 아니면 기동에서 끊는다. */
    @Test
    @DisplayName("대역_표기가_망가지면_거절한다")
    void 대역_표기가_망가지면_거절한다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("not-an-ip/24"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("10.0.1.0/x"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("10.0.1.0/-1"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 비트가 바이트 경계에 딱 맞으면 나머지 마스크를 안 본다. */
    @Test
    @DisplayName("바이트_경계_대역도_본다")
    void 바이트_경계_대역도_본다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.0.0/8"), 포트);

        assertThat(허용.permits(주소("10.9.9.9:1"))).isTrue();
        assertThat(허용.permits(주소("11.0.0.1:1"))).isFalse();
    }

    /** 앞 바이트부터 갈리면 나머지를 안 본다. */
    @Test
    @DisplayName("앞_바이트가_다르면_거절한다")
    void 앞_바이트가_다르면_거절한다() {
        assertThat(AllowedDestinations.of(List.of("10.0.1.0/24"), 포트).permits(주소("11.0.1.7:1")))
                .isFalse();
    }

    /** 비트가 바이트 경계에 안 맞으면 마지막 바이트를 마스크로 본다. */
    @Test
    @DisplayName("경계에_안_맞는_대역도_본다")
    void 경계에_안_맞는_대역도_본다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.1.0/25"), 포트);

        assertThat(허용.permits(주소("10.0.1.127:1"))).as("경계 안").isTrue();
        assertThat(허용.permits(주소("10.0.1.128:1"))).as("경계 밖").isFalse();
    }

    /** 목록에 빈 자리가 오면 기동에서 끊는다. yaml 의 빈 항목이 그렇게 온다. */
    @Test
    @DisplayName("빈_자리가_있으면_거절한다")
    void 빈_자리가_있으면_거절한다() {
        List<String> 빈_자리가_섞인_목록 = Arrays.asList(".internal", null);

        assertThatThrownBy(() -> AllowedDestinations.of(빈_자리가_섞인_목록, 포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>맨 주소는 그 한 대를 뜻한다.</b> 접미사로 넣으면 이름끼리만 견주므로 그
     * 주소를 보고한 뒷단이 도리어 거절되고, 증상은 "라우팅 후보 0" 이다.
     */
    @Test
    @DisplayName("맨_주소도_받는다")
    void 맨_주소도_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.1.5"), 포트);

        assertThat(허용.permits(주소("10.0.1.5:9000"))).isTrue();
        assertThat(허용.permits(주소("10.0.1.6:9000"))).isFalse();
    }

    /**
     * <b>한 낱말짜리 hex 이름이 대역을 통과하면 안 된다.</b> {@code InetAddress} 는
     * {@code beef} 를 DNS 로 풀어 주는데, 그러면 검사한 순간과 연결하는 순간이 갈린다.
     */
    @Test
    @DisplayName("hex_이름은_대역으로_안_통과한다")
    void hex_이름은_대역으로_안_통과한다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.1.0/24"), 포트);

        assertThat(허용.permits(주소("beef:9000"))).isFalse();
        assertThat(허용.permits(주소("1234:9000"))).isFalse();
    }

    /** 이름 항목이 라벨 규칙을 어기면 기동에서 끊는다. 안 끊으면 영영 안 맞는다. */
    @Test
    @DisplayName("못_맞을_이름은_거절한다")
    void 못_맞을_이름은_거절한다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("*.internal"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("a..b"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("a.b."), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("."), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("..internal"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>구체적으로 보이는 표기로 통째로 열리면 안 된다.</b> v4 의 {@code 0.0.0.0/1}
     * 은 눈에 띄지만 v6 에는 그런 신호가 없다 — {@code 2000::/3} 하나로 공인
     * 유니캐스트 전부가 열리는데 표기는 대역 하나처럼 읽힌다.
     */
    @Test
    @DisplayName("한_바이트보다_넓은_대역은_거절한다")
    void 한_바이트보다_넓은_대역은_거절한다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("2000::/3"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("0.0.0.0/1"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 내부망이 쓰는 폭은 그대로 받는다. 한 칸 좁히면 설정이 통째로 막힌다. */
    @Test
    @DisplayName("내부망_한_바이트_대역은_받는다")
    void 내부망_한_바이트_대역은_받는다() {
        assertThat(AllowedDestinations.of(List.of("10.0.0.0/8"), 포트)
                .permits(주소("10.7.1.5:8080"))).isTrue();
        assertThat(AllowedDestinations.of(List.of("fd00::/8"), 포트)
                .permits(주소("[fd00::5]:8080"))).isTrue();
    }

    /**
     * <b>미지정은 대역 안이어도 목적지가 아니다.</b> "아무 대나" 를 뜻하는 주소라
     * 어느 한 대도 가리키지 않는다.
     */
    @Test
    @DisplayName("미지정_주소는_대역_안이어도_거절한다")
    void 미지정_주소는_대역_안이어도_거절한다() {
        AllowedDestinations 허용 =
                AllowedDestinations.of(List.of("0.0.0.0/8", "fd00::/8"), 포트);

        assertThat(허용.permits(주소("0.0.0.0:8080"))).isFalse();
        assertThat(허용.permits(주소("[::]:8080"))).isFalse();
        assertThat(허용.permits(주소("[fd00::5]:8080"))).as("한 대는 그대로 받는다").isTrue();
    }

    /**
     * <b>루프백은 목적지다.</b> 같은 호스트의 뒷단이 실제 배치 모양이고 하네스도 그
     * 모양으로 돈다. 자기 자신을 부르는 것을 막는 것은 포트 목록이다.
     */
    @Test
    @DisplayName("루프백은_적어_두면_받는다")
    void 루프백은_적어_두면_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("127.0.0.0/8"), 포트);

        assertThat(허용.permits(주소("127.0.0.1:8080"))).isTrue();
        assertThat(허용.permits(주소("127.0.0.1:7000"))).as("포트가 다르면 거절한다")
                .isFalse();
    }

    /**
     * <b>한 대를 가리키지 않는 주소는 목적지가 아니다.</b> 링크 로컬은 어느 링크인지가
     * 빠져 있고, 멀티캐스트는 애초에 한 대가 아니다.
     */
    @Test
    @DisplayName("한_대가_아닌_주소는_거절한다")
    void 한_대가_아닌_주소는_거절한다() {
        AllowedDestinations 허용 = AllowedDestinations.of(
                List.of("fe80::/32", "ff02::/32", "169.254.0.0/16"), 포트);

        assertThat(허용.permits(주소("[fe80::1]:8080"))).isFalse();
        assertThat(허용.permits(주소("[ff02::1]:8080"))).isFalse();
        assertThat(허용.permits(주소("169.254.1.1:8080"))).isFalse();
    }

    /**
     * <b>v6 의 한 바이트는 v4 의 한 바이트가 아니다.</b> 같은 `/8` 이 v6 에서는 비교가
     * 안 되게 넓다. 사설 대역의 정본 표기 `fc00::/7` 은 그 자신이 내부망이라 받는다.
     */
    @Test
    @DisplayName("v6_하한은_v4_보다_좁다")
    void v6_하한은_v4_보다_좁다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("2000::/8"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("::/8"), 포트))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(AllowedDestinations.of(List.of("fc00::/7"), 포트)
                .permits(주소("[fd00::5]:8080"))).as("사설 대역 정본 표기는 받는다").isTrue();
        assertThat(AllowedDestinations.of(List.of("2001:db8::/32"), 포트)
                .permits(주소("[2001:db8::5]:8080"))).as("좁게 적으면 받는다").isTrue();
    }

    /**
     * <b>주소로 못 읽히는 숫자 항목은 거절한다.</b> 접미사로 받으면 영영 아무것도 안
     * 맞고, 더 나쁘게는 보고 쪽의 `010.0.0.5` 가 그 접미사에 걸려 통과한다 — 그 값은
     * 연결할 때 `10.0.0.5` 로 풀려 검사한 값과 갈린다.
     */
    @Test
    @DisplayName("주소로_못_읽는_숫자_항목은_거절한다")
    void 주소로_못_읽는_숫자_항목은_거절한다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("10.0.1.256"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("10.0.1.5.6"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>v6 표기로 적은 v4 매핑 대역이 조용히 v4 규칙으로 선다.</b>
     * {@code ::ffff:0.0.0.0/1} 은 v6 항목처럼 읽히는데 실제로는 v4 절반이 열린다.
     */
    @Test
    @DisplayName("v4_매핑_표기는_거절한다")
    void v4_매핑_표기는_거절한다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("::ffff:0.0.0.0/1"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("::ffff:10.0.0.1"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 포트의 두 끝을 다 못 박는다. 한쪽만 재면 부등호를 옮겨도 초록이다. */
    @Test
    @DisplayName("포트의_두_끝을_받는다")
    void 포트의_두_끝을_받는다() {
        assertThat(AllowedDestinations.of(List.of("10.0.0.0/8"), List.of(1, 65535))
                .permits(주소("10.0.0.5:65535"))).isTrue();
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("10.0.0.0/8"), List.of(65536)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("10.0.0.0/8"), List.of(0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>대역 자리에 온 것은 대역으로 읽는다.</b> 이름으로 넘기면 오류 문구가 갈려,
     * 설정을 고치는 사람이 어느 규칙에 걸렸는지 못 본다.
     */
    @Test
    @DisplayName("빗금으로_시작하면_대역_오류다")
    void 빗금으로_시작하면_대역_오류다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("/24"), 포트))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("대역을 못 읽는다");
    }

    /** 무제한은 이름으로만 만든다. 인자를 빠뜨려 조용히 되는 것과는 다르다. */
    @Test
    @DisplayName("무제한은_다_받는다")
    void 무제한은_다_받는다() {
        assertThat(AllowedDestinations.unrestricted().permits(주소("evil.example.com:1")))
                .isTrue();
    }

    /**
     * <b>앞자리 0 을 받으면 안 된다.</b> {@code 010} 을 8진수로 읽는 파서가 있어,
     * 받아 주면 검사한 값과 연결하는 값이 갈린다.
     */
    @Test
    @DisplayName("앞자리_0_표기는_주소가_아니다")
    void 앞자리_0_표기는_주소가_아니다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.1.0/24"), 포트);

        assertThat(허용.permits(주소("010.0.1.5:8080"))).isFalse();
    }

    /**
     * <b>주소를 쓰다 만 것을 이름으로 받지 않는다.</b> {@code 10.0.1} 은 라벨이 다
     * 유효해 접미사로 들어가고, 그 접미사는 영영 아무것도 안 맞는다 — 증상은 기동
     * 성공에 후보 0 이다.
     */
    @Test
    @DisplayName("덜_적힌_주소는_거절한다")
    void 덜_적힌_주소는_거절한다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("10.0.1"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("10"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 빈 목록은 막으면서 전 대역을 받으면, 같은 결과가 표기 하나로 조용히 선다. */
    @Test
    @DisplayName("전_대역은_적을_수_없다")
    void 전_대역은_적을_수_없다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("0.0.0.0/0"), 포트))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>이름과 주소는 서로를 안 덮는다.</b> 접미사만 적어 두고 뒷단이 파드 주소를
     * 보고하면 전면 거절이다 — 이 조합이 실제 사고의 모양이다.
     */
    @Test
    @DisplayName("접미사만_적으면_맨_주소는_거절한다")
    void 접미사만_적으면_맨_주소는_거절한다() {
        assertThat(AllowedDestinations.of(List.of(".internal"), 포트).permits(주소("10.0.1.7:8080")))
                .isFalse();
    }

    /**
     * <b>호스트만 보면 반쪽이다</b> (CY-887). 허용한 망 안의 아무 포트나 고를 수
     * 있으면 같은 망의 다른 서비스가 그대로 연결 대상이 된다.
     */
    @Test
    @DisplayName("허용_밖_포트는_거절한다")
    void 허용_밖_포트는_거절한다() {
        AllowedDestinations 허용 =
                AllowedDestinations.of(List.of("10.0.1.0/24"), List.of(8080));

        assertThat(허용.permits(주소("10.0.1.7:8080"))).isTrue();
        assertThat(허용.permits(주소("10.0.1.7:6379"))).as("같은 망의 다른 서비스").isFalse();
    }

    /** 포트가 비면 못 켠다. 안 적은 것이 전부 허용이면 호스트 제한이 반쪽이 된다. */
    @Test
    @DisplayName("포트가_비면_만들_수_없다")
    void 포트가_비면_만들_수_없다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of(".internal"), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of(".internal"), null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of(".internal"), List.of(0)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of(".internal"), List.of(65536)))
                .isInstanceOf(IllegalArgumentException.class);
        // yaml 의 빈 항목이 이렇게 온다.
        assertThatThrownBy(() -> AllowedDestinations.of(List.of(".internal"),
                Arrays.asList((Integer) null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>v6 대역으로 v6 뒷단을 받는다</b> (CY-888).
     *
     * <p>주소가 여기까지 오게 됐으므로 대역도 받는다. 안 받으면 v6 로 보고한 대가
     * 후보에서 빠지는데 크레딧에는 들어가, 그 예산으로 통과한 요청이 갈 곳이 없다.
     */
    @Test
    @DisplayName("v6_대역_안의_뒷단을_받는다")
    void v6_대역_안의_뒷단을_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("fd00::/8"), 포트);

        assertThat(허용.permits(주소("[fd00::1]:9000"))).isTrue();
        assertThat(허용.permits(주소("[fd00:1234::abcd]:8080"))).isTrue();
    }

    /** 대역 밖은 막는다. 받아 두기만 하면 v6 를 전면 개방한 것과 같다. */
    @Test
    @DisplayName("v6_대역_밖의_뒷단은_막는다")
    void v6_대역_밖의_뒷단은_막는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("fd00::/8"), 포트);

        assertThat(허용.permits(주소("[2001:db8::1]:9000"))).isFalse();
    }

    /**
     * <b>바이트 수가 다르면 안 견준다.</b> 앞 바이트가 겹치는 짝을 골라야 그 검사가
     * 실제로 돈다 — `fd00::/8` 의 첫 바이트가 253 이고 `253.1.2.3` 도 253 이다.
     * 길이를 안 보면 v6 만 허용한 배포가 우리 망도 아닌 v4 뒷단을 받는다.
     */
    @Test
    @DisplayName("v6_대역은_앞_바이트가_같은_v4_뒷단도_안_받는다")
    void v6_대역은_앞_바이트가_같은_v4_뒷단도_안_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("fd00::/8"), 포트);

        assertThat(허용.permits(주소("253.1.2.3:9000"))).isFalse();
    }

    /** 반대 방향도 같다. v4 대역이 v6 뒷단을 받으면 안 된다. */
    @Test
    @DisplayName("v4_대역은_v6_뒷단을_안_받는다")
    void v4_대역은_v6_뒷단을_안_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.1.0/24"), 포트);

        assertThat(허용.permits(주소("[fd00::1]:9000"))).isFalse();
    }

    /**
     * <b>v4 매핑 주소는 v4 로 접힌다.</b> 그래서 v4 대역이 받는다 — 검사한 대상과
     * 연결하는 대상이 같으므로 맞는 동작이다. 안 접히면 길이가 갈려 조용히 후보에서
     * 빠지고 크레딧만 남으므로, 접힌다는 사실을 여기서 못 박는다.
     */
    @Test
    @DisplayName("v4_매핑_주소는_v4_대역이_받는다")
    void v4_매핑_주소는_v4_대역이_받는다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("10.0.1.0/24"), 포트);

        assertThat(허용.permits(주소("[::ffff:10.0.1.7]:9000"))).isTrue();
    }

    /**
     * <b>콜론을 빠뜨린 v6 오타는 이름으로 선다.</b> `fd00::/8` 에서 콜론을 흘리면
     * `fd00` 이 접미사로 조용히 들어가고 영영 아무것도 안 맞는다. 반쪽 v4 를 막는
     * 검사의 v6 짝이 없다 — 지금 동작을 못 박아 다음 판단의 입력으로 둔다.
     */
    @Test
    @DisplayName("콜론_빠진_v6_는_이름으로_선다")
    void 콜론_빠진_v6_는_이름으로_선다() {
        AllowedDestinations 허용 = AllowedDestinations.of(List.of("fd00"), 포트);

        assertThat(허용.permits(주소("[fd00::1]:9000")))
                .as("이름으로 서면 v6 뒷단이 통째로 빠진다").isFalse();
    }

    /**
     * <b>설정에는 대괄호를 안 쓴다.</b> 보고 표기를 그대로 베껴 적는 것이 가장 자연스러운
     * 오타인데, 받아 두면 이름으로 읽혀 영영 아무것도 안 맞는 접미사가 된다.
     */
    @Test
    @DisplayName("대괄호를_씌운_항목은_거절한다")
    void 대괄호를_씌운_항목은_거절한다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("[fd00::1]"), List.of(9000)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("[fd00::]/8"), List.of(9000)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** v6 도 전 대역은 못 적는다. 표기 하나로 전면 개방이 조용히 서면 안 된다. */
    @Test
    @DisplayName("v6_전_대역은_거절한다")
    void v6_전_대역은_거절한다() {
        assertThatThrownBy(() -> AllowedDestinations.of(List.of("::/0"), List.of(9000)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

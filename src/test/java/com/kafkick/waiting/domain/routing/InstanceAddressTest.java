package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * 뒷단이 보고에 실어 올리는 자기 주소 (D-C1 · A-11).
 *
 * <p><b>이 값은 밖에서 온다.</b> 뒷단이 레디스에 쓰고 게이트웨이가 읽어 그리로
 * 연결한다 — 그대로 믿으면 게이트웨이가 아무 데나 요청을 보내는 통로가 된다.
 * 모양을 못 지키는 값은 라우팅 후보에서 뺀다.
 */
@Tag("unit")
class InstanceAddressTest {

    @Test
    @DisplayName("호스트와_포트를_읽는다")
    void 호스트와_포트를_읽는다() {
        Optional<InstanceAddress> 주소 = InstanceAddress.parse("10.0.1.7:8080");

        assertThat(주소).isPresent();
        assertThat(주소.orElseThrow().host()).isEqualTo("10.0.1.7");
        assertThat(주소.orElseThrow().port()).isEqualTo(8080);
    }

    @Test
    @DisplayName("이름도_받는다")
    void 이름도_받는다() {
        assertThat(InstanceAddress.parse("coupon-be-3.internal:9000"))
                .map(InstanceAddress::host).contains("coupon-be-3.internal");
    }

    /** 스킴이 붙어 오면 안 받는다. 그 자리에 무엇이든 올 수 있게 되기 때문이다. */
    @Test
    @DisplayName("스킴이_붙으면_안_받는다")
    void 스킴이_붙으면_안_받는다() {
        assertThat(InstanceAddress.parse("http://10.0.1.7:8080")).isEmpty();
        assertThat(InstanceAddress.parse("file:///etc/passwd")).isEmpty();
    }

    /** 경로나 질의가 붙으면 안 받는다. 주소가 아니라 URL 을 받는 셈이 된다. */
    @Test
    @DisplayName("경로가_붙으면_안_받는다")
    void 경로가_붙으면_안_받는다() {
        assertThat(InstanceAddress.parse("10.0.1.7:8080/admin")).isEmpty();
        assertThat(InstanceAddress.parse("10.0.1.7:8080?x=1")).isEmpty();
        assertThat(InstanceAddress.parse("10.0.1.7:8080#f")).isEmpty();
    }

    /** 자격 증명이 붙으면 안 받는다. 프록시가 남의 자격으로 붙는 통로가 된다. */
    @Test
    @DisplayName("자격_증명이_붙으면_안_받는다")
    void 자격_증명이_붙으면_안_받는다() {
        assertThat(InstanceAddress.parse("user:pw@10.0.1.7:8080")).isEmpty();
    }

    @Test
    @DisplayName("포트가_없거나_범위_밖이면_안_받는다")
    void 포트가_없거나_범위_밖이면_안_받는다() {
        assertThat(InstanceAddress.parse("10.0.1.7")).isEmpty();
        assertThat(InstanceAddress.parse("10.0.1.7:0")).isEmpty();
        assertThat(InstanceAddress.parse("10.0.1.7:65536")).isEmpty();
        assertThat(InstanceAddress.parse("10.0.1.7:-1")).isEmpty();
        assertThat(InstanceAddress.parse("10.0.1.7:abc")).isEmpty();
        // 쌍점으로 끝나면 포트가 없는 것이다. 빈 문자열을 파싱하면 예외가 난다.
        assertThat(InstanceAddress.parse("10.0.1.7:")).isEmpty();
    }

    @Test
    @DisplayName("비었거나_없으면_안_받는다")
    void 비었거나_없으면_안_받는다() {
        assertThat(InstanceAddress.parse(null)).isEmpty();
        assertThat(InstanceAddress.parse("")).isEmpty();
        assertThat(InstanceAddress.parse("   ")).isEmpty();
        assertThat(InstanceAddress.parse(":8080")).isEmpty();
    }

    /** 공백이 섞이면 안 받는다. 헤더 주입으로 이어지는 흔한 자리다. */
    @Test
    @DisplayName("공백이_섞이면_안_받는다")
    void 공백이_섞이면_안_받는다() {
        assertThat(InstanceAddress.parse("10.0.1.7 :8080")).isEmpty();
        assertThat(InstanceAddress.parse("10.0.1.7:80 80")).isEmpty();
        assertThat(InstanceAddress.parse("10.0.1.7:8080\nX: y")).isEmpty();
    }

    /** 아주 긴 값은 안 받는다. 로그와 지표로 그대로 흘러 들어간다. */
    @Test
    @DisplayName("너무_길면_안_받는다")
    void 너무_길면_안_받는다() {
        assertThat(InstanceAddress.parse("a".repeat(300) + ":8080")).isEmpty();
    }

    @Test
    @DisplayName("문자열로_되돌리면_같다")
    void 문자열로_되돌리면_같다() {
        assertThat(InstanceAddress.parse("10.0.1.7:8080").orElseThrow())
                .hasToString("10.0.1.7:8080");
    }

    /**
     * <b>라벨마다 본다.</b>
     *
     * <p>전체를 한 덩어리로 보면 양 끝만 맞으면 통과한다 — 가운데가 무엇이든
     * 못 푸는 이름이 라우팅 후보로 올라간다.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "a..b:8080",
        "a.-b:8080",
        "a.b-:8080",
        ".a.b:8080",
        "a.b.:8080",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b:8080"})
    @DisplayName("못_푸는_이름은_안_받는다")
    void 못_푸는_이름은_안_받는다(String raw) {
        assertThat(InstanceAddress.parse(raw)).isEmpty();
    }

    /** <b>양 끝을 받는지도 본다.</b> 안 받으면 65535 로 뜬 뒷단이 조용히 빠진다. */
    @Test
    @DisplayName("포트의_양_끝은_받는다")
    void 포트의_양_끝은_받는다() {
        assertThat(InstanceAddress.parse("10.0.1.7:1"))
                .map(InstanceAddress::port).contains(1);
        assertThat(InstanceAddress.parse("10.0.1.7:65535"))
                .map(InstanceAddress::port).contains(65535);
    }

    /**
     * <b>대괄호 표기로만 v6 를 받는다</b> (CY-888).
     *
     * <p>안 받으면 v6 로 보고한 대가 후보에서 영영 빠지는데, 크레딧에는 들어가
     * 통과 예산이 발행된다 — 그 예산으로 통과한 요청은 보낼 곳이 없다.
     */
    @Test
    @DisplayName("대괄호_표기의_v6_를_읽는다")
    void 대괄호_표기의_v6_를_읽는다() {
        assertThat(InstanceAddress.parse("[2001:db8::1]:8080"))
                .contains(new InstanceAddress("2001:db8::1", 8080));
        assertThat(InstanceAddress.parse("[::1]:80"))
                .map(InstanceAddress::host).contains("::1");
    }

    /**
     * <b>대괄호가 없으면 안 받는다.</b> 마지막 콜론으로 끊으면 {@code ::1:8080} 이
     * 호스트 {@code ::1} 에 포트 8080 으로도, 호스트 {@code ::1:8080} 에 포트 없음으로도
     * 읽힌다 — 어느 쪽인지 정할 근거가 값 안에 없다.
     */
    @Test
    @DisplayName("대괄호_없는_v6_는_안_받는다")
    void 대괄호_없는_v6_는_안_받는다() {
        assertThat(InstanceAddress.parse("2001:db8::1:8080")).isEmpty();
        assertThat(InstanceAddress.parse("::1:80")).isEmpty();
    }

    /**
     * <b>낼 때 대괄호를 다시 씌운다.</b> 스냅샷이 이 문자열로 실려 나가고 받는 쪽이
     * 다시 읽는다 — 왕복이 안 맞으면 그 대가 다음 틱에 후보에서 빠진다.
     */
    @Test
    @DisplayName("v6_는_대괄호를_다시_씌워_낸다")
    void v6_는_대괄호를_다시_씌워_낸다() {
        InstanceAddress v6 = InstanceAddress.parse("[2001:db8::1]:8080").orElseThrow();

        assertThat(v6).hasToString("[2001:db8::1]:8080");
        assertThat(InstanceAddress.parse(v6.toString())).contains(v6);
    }

    /** 대괄호 안이 v6 가 아니면 거절한다. 이름을 그렇게 싸서 넣는 길을 안 낸다. */
    @Test
    @DisplayName("대괄호_안이_v6_가_아니면_거절한다")
    void 대괄호_안이_v6_가_아니면_거절한다() {
        assertThat(InstanceAddress.parse("[be.internal]:8080")).isEmpty();
        assertThat(InstanceAddress.parse("[]:8080")).isEmpty();
        assertThat(InstanceAddress.parse("[2001:db8::1]8080")).isEmpty();
        assertThat(InstanceAddress.parse("[2001:db8::1]:")).isEmpty();
        // 닫는 대괄호가 없으면 어디까지가 호스트인지 못 정한다.
        assertThat(InstanceAddress.parse("[2001:db8::1:8080")).isEmpty();
        // 닫고 끝나면 포트가 없다.
        assertThat(InstanceAddress.parse("[2001:db8::1]")).isEmpty();
    }

    /**
     * <b>정규 생성자도 막는다</b> (CY-887 · CY-888). 목적지 판정이 호스트를 그대로
     * 리터럴로 읽으므로, v6 는 대괄호를 벗긴 모양이라야 하고 이름은 콜론이 없어야 한다.
     */
    @Test
    @DisplayName("정규_생성자도_막는다")
    void 정규_생성자도_막는다() {
        assertThatThrownBy(() -> new InstanceAddress("[2001:db8::1]", 8080))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InstanceAddress("zz::qq", 8080))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InstanceAddress("", 8080))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InstanceAddress(null, 8080))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InstanceAddress("a.b.", 8080))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InstanceAddress("be.internal", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InstanceAddress("be.internal", 65536))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InstanceAddress("a".repeat(256), 8080))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

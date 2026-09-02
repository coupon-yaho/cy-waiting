package com.kafkick.waiting.domain.routing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
}

package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * 전달 헤더를 믿어도 되는 홉.
 *
 * <p>아무나 채워 넣게 두면 키를 무한히 만들어 리미터를 포화시키고, 그때부터
 * 정상 사용자가 막힌다.
 */
class TrustedProxiesTest {

    private ListAppender<ILoggingEvent> 로그;

    private Logger 로거() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger(TrustedProxies.class);
    }

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        로거().addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        로거().detachAppender(로그);
    }

    private List<String> 오류들() {
        return 로그.list.stream()
                .filter(e -> e.getLevel() == Level.ERROR)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    @Test
    @DisplayName("설정이_비면_아무도_안_믿는다")
    void 설정이_비면_아무도_안_믿는다() {
        // 기본은 안 믿는 것이다. 앞단이 헤더를 덮어쓴다는 보장이 있을 때만 연다.
        assertThat(TrustedProxies.of(List.of()).isTrusted("10.0.0.1")).isFalse();
        assertThat(TrustedProxies.of(Collections.emptyList()).isTrusted("10.0.0.1")).isFalse();
    }

    /** 접두 문자열로 보면 이 표기가 아무 주소도 안 잡는다 — 어디에도 안 붙는다. */
    @Test
    @DisplayName("대역_표기를_비트로_읽는다")
    void 대역_표기를_비트로_읽는다() {
        TrustedProxies 신뢰 = TrustedProxies.of(List.of("10.0.0.0/8"));

        assertThat(신뢰.isTrusted("10.1.2.3")).isTrue();
        assertThat(신뢰.isTrusted("10.255.255.255")).isTrue();
        assertThat(신뢰.isTrusted("11.0.0.1")).isFalse();
    }

    @Test
    @DisplayName("바이트_경계가_아닌_대역도_읽는다")
    void 바이트_경계가_아닌_대역도_읽는다() {
        TrustedProxies 신뢰 = TrustedProxies.of(List.of("192.168.16.0/20"));

        assertThat(신뢰.isTrusted("192.168.16.1")).isTrue();
        assertThat(신뢰.isTrusted("192.168.31.255")).isTrue();
        assertThat(신뢰.isTrusted("192.168.32.1")).isFalse();
    }

    @Test
    @DisplayName("대역_없는_표기는_그_주소만이다")
    void 대역_없는_표기는_그_주소만이다() {
        TrustedProxies 신뢰 = TrustedProxies.of(List.of("10.0.0.1"));

        assertThat(신뢰.isTrusted("10.0.0.1")).isTrue();
        assertThat(신뢰.isTrusted("10.0.0.2")).isFalse();
    }

    /**
     * <b>전 대역 한 줄이면 모든 홉이 신뢰 홉이 된다.</b> 그러면 전달 헤더를 아무나
     * 쓸 수 있고, 리미터 키가 클라이언트가 적은 값으로 갈려 상한이 무의미해진다.
     * 오타가 아니라 의도이므로 조용히 뒤집지 않고 던진다.
     */
    @Test
    @DisplayName("너무_넓은_대역은_기동에서_막는다")
    void 너무_넓은_대역은_기동에서_막는다() {
        assertThatThrownBy(() -> TrustedProxies.of(List.of("0.0.0.0/0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("0.0.0.0/0");
    }

    /** v6 는 같은 비트 수가 훨씬 넓다. 패밀리마다 하한이 다르다. */
    @Test
    @DisplayName("v6_도_하한이_따로다")
    void v6_도_하한이_따로다() {
        assertThatThrownBy(() -> TrustedProxies.of(List.of("2000::/3")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("너무 넓다");
        assertThat(TrustedProxies.of(List.of("2001:db8::/32")).isTrusted("2001:db8::1"))
                .as("하한을 지키면 받는다").isTrue();
    }

    /**
     * <b>v4 를 실어 나르는 v6 대역은 하한을 지키고도 인터넷 전부를 연다.</b> NAT64
     * 대역은 96 비트라 하한을 통과하는데, 앞 열두 바이트만 견주므로 번역된 v4 전부가
     * 걸린다.
     */
    @Test
    @DisplayName("v4_를_실어_나르는_대역은_막는다")
    void v4_를_실어_나르는_대역은_막는다() {
        assertThatThrownBy(() -> TrustedProxies.of(List.of("64:ff9b::/96")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("쓸 수 없는").hasMessageContaining("64:ff9b::/96");
        assertThatThrownBy(() -> TrustedProxies.of(List.of("::/96")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("쓸 수 없는");
    }

    /** 사설 대역 한 덩이는 실제 배치 모양이라 받는다. 하한이 그것까지 막으면 못 쓴다. */
    @Test
    @DisplayName("사설_대역_한_덩이는_받는다")
    void 사설_대역_한_덩이는_받는다() {
        assertThat(TrustedProxies.of(List.of("10.0.0.0/8")).isTrusted("10.1.2.3")).isTrue();
    }

    /** 못 읽는 줄 하나가 나머지를 안 버린다. 오타는 그 줄만 좁힌다. */
    @Test
    @DisplayName("못_읽는_줄_옆의_유효한_줄은_산다")
    void 못_읽는_줄_옆의_유효한_줄은_산다() {
        TrustedProxies 신뢰 = TrustedProxies.of(List.of("어쩌구", "10.0.0.0/8"));

        assertThat(신뢰.isTrusted("10.1.2.3")).isTrue();
        // 원문이 없으면 어느 줄이 틀렸는지 못 찾는다. 설정 키도 같이 든다.
        assertThat(오류들()).singleElement().asString()
                .contains("어쩌구").contains("waiting.proxy.cidrs");
    }

    /** 줄바꿈이 든 값은 수집된 로그에서 줄을 위조한다. 한 줄로 눕혀 싣는다. */
    @Test
    @DisplayName("못_읽는_줄의_줄바꿈은_눕혀_싣는다")
    void 못_읽는_줄의_줄바꿈은_눕혀_싣는다() {
        TrustedProxies.of(List.of("어\r\n쩌\u2028구"));

        assertThat(오류들()).singleElement().asString().doesNotContain("\n")
                .doesNotContain("\r").doesNotContain("\u2028");
    }

    /** 쉼표로 여럿을 적으면 둘째부터 공백이 붙어 온다. 안 털면 조용히 좁아진다. */
    @Test
    @DisplayName("양끝_공백은_턴다")
    void 양끝_공백은_턴다() {
        assertThat(TrustedProxies.of(List.of(" 10.0.0.0/8 ")).isTrusted("10.1.2.3")).isTrue();
    }

    /** 빈 값은 안 적은 것과 같다. 기본 배포가 빈 줄 하나를 주는 자리다. */
    @Test
    @DisplayName("빈_줄은_안_적은_것과_같다")
    void 빈_줄은_안_적은_것과_같다() {
        assertThat(TrustedProxies.of(List.of("", "  ")).isTrusted("10.1.2.3")).isFalse();
        assertThat(오류들()).as("기본 배포가 매 기동 오류를 찍으면 안 본다").isEmpty();
    }

    /** 오타 하나가 전 대역을 여는 것보다 아무도 안 믿는 쪽이 낫다. */
    @Test
    @DisplayName("못_읽는_표기는_안_믿는다")
    void 못_읽는_표기는_안_믿는다() {
        assertThat(TrustedProxies.of(List.of("10.0.0.0/aa")).isTrusted("10.0.0.1")).isFalse();
        assertThat(TrustedProxies.of(List.of("10.0.0.0/99")).isTrusted("10.0.0.1")).isFalse();
        assertThat(TrustedProxies.of(List.of("헛소리")).isTrusted("10.0.0.1")).isFalse();
        assertThat(TrustedProxies.of(List.of("10.0.0.0/8")).isTrusted("헛소리")).isFalse();
    }

    /** 이름을 찾으면 그 조회가 요청 경로에 붙는다. */
    @Test
    @DisplayName("이름은_안_찾는다")
    void 이름은_안_찾는다() {
        assertThat(TrustedProxies.of(List.of("localhost")).isTrusted("127.0.0.1")).isFalse();
        // 주소처럼 생긴 이름도 이름이다.
        assertThat(TrustedProxies.of(List.of("dead.beef")).isTrusted("10.0.0.1")).isFalse();
        assertThat(TrustedProxies.of(List.of("10.0.0.0/8")).isTrusted("dead.beef")).isFalse();
    }
}

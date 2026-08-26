package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.stream.Stream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 서킷 설정은 <b>코드가 아니라 설정에 둔다.</b>
 *
 * <p>여기서는 값을 안 정하고 검증만 한다. 코드에도 기본값이 있으면 yml 의 키를
 * 잘못 적어도 조용히 그 값으로 떨어진다 — 실제로 그 오타를 시험이 못 잡았다.
 * 실린 값은 {@code GatewayWiringTest} 가 뜬 컨텍스트에서 본다.
 */
class BackendCircuitPropertiesTest {

    private static final Duration 창 = Duration.ofSeconds(10);
    private static final Duration 느림 = Duration.ofMillis(1500);
    private static final Duration 대기 = Duration.ofSeconds(5);
    private static final Duration 반쯤_상한 = Duration.ofSeconds(30);

    /** 온전한 한 벌. 시험마다 한 자리씩 흠집을 낸다. */
    private static BackendCircuitProperties 값(Float 실패비율, Float 느림비율) {
        return new BackendCircuitProperties(창, 20,
                실패비율 == null ? 50f : 실패비율,
                느림, 느림비율 == null ? 50f : 느림비율,
                대기, 반쯤_상한, 10);
    }

    /** 여덟 자리를 하나씩 비운 한 벌. 한 자리라도 빠지면 그 자리는 무방비다. */
    private static Stream<Arguments> 한_자리씩_빈_설정() {
        return Stream.of(
                Arguments.of("sliding-window-size", (ThrowingCallable) () ->
                        new BackendCircuitProperties(
                                null, 20, 50f, 느림, 50f, 대기, 반쯤_상한, 10)),
                Arguments.of("minimum-number-of-calls", (ThrowingCallable) () ->
                        new BackendCircuitProperties(
                                창, null, 50f, 느림, 50f, 대기, 반쯤_상한, 10)),
                Arguments.of("failure-rate-threshold", (ThrowingCallable) () ->
                        new BackendCircuitProperties(
                                창, 20, null, 느림, 50f, 대기, 반쯤_상한, 10)),
                Arguments.of("slow-call-duration-threshold", (ThrowingCallable) () ->
                        new BackendCircuitProperties(
                                창, 20, 50f, null, 50f, 대기, 반쯤_상한, 10)),
                Arguments.of("slow-call-rate-threshold", (ThrowingCallable) () ->
                        new BackendCircuitProperties(
                                창, 20, 50f, 느림, null, 대기, 반쯤_상한, 10)),
                Arguments.of("wait-duration-in-open-state", (ThrowingCallable) () ->
                        new BackendCircuitProperties(
                                창, 20, 50f, 느림, 50f, null, 반쯤_상한, 10)),
                Arguments.of("max-wait-duration-in-half-open-state", (ThrowingCallable) () ->
                        new BackendCircuitProperties(
                                창, 20, 50f, 느림, 50f, 대기, null, 10)),
                Arguments.of("permitted-number-of-calls-in-half-open-state",
                        (ThrowingCallable) () -> new BackendCircuitProperties(
                                창, 20, 50f, 느림, 50f, 대기, 반쯤_상한, null)));
    }

    /**
     * <b>여덟 자리 전부가 기동을 막아야 한다.</b> 한 자리만 빠져도 그 값은 조용히
     * 라이브러리 기본값으로 떨어지고 기동은 성공한다 — 실제로
     * {@code max-wait-duration-in-half-open-state} 하나가 그렇게 빠져 있었다.
     *
     * <p>메시지에 키까지 본다. {@code NullPointerException} 으로 멎어도 기동은
     * 막히지만, 운영자는 어느 줄을 고쳐야 하는지 못 듣는다.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("한_자리씩_빈_설정")
    @DisplayName("여덟_자리_어느_하나가_비어도_기동을_막는다")
    void 여덟_자리_어느_하나가_비어도_기동을_막는다(String 키, ThrowingCallable 만든다) {
        assertThatThrownBy(만든다)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(키);
    }

    /**
     * <b>반쯤 열린 상태에도 상한이 있어야 한다.</b> 라이브러리 기본값 0 은
     * 무제한이라, 0 으로 적으면 프로브가 응답 없는 뒷단에 매달려 그 상태로
     * 고정된다 — 나가는 조건이 없다.
     */
    @Test
    @DisplayName("반쯤_열린_상태의_상한이_0이면_막는다")
    void 반쯤_열린_상태의_상한이_0이면_막는다() {
        assertThatThrownBy(() -> new BackendCircuitProperties(
                창, 20, 50f, 느림, 50f, 대기, Duration.ZERO, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-wait-duration-in-half-open-state");
    }

    /**
     * <b>비율은 백분율이다.</b> 0.5 로 적으면 반올림돼 0% 가 되고, 그러면 첫
     * 실패에 서킷이 열린다 — 기동은 성공한다.
     */
    @Test
    @DisplayName("비율이_범위를_벗어나면_기동을_막는다")
    void 비율이_범위를_벗어나면_기동을_막는다() {
        assertThatThrownBy(() -> 값(0f, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> 값(101f, null)).isInstanceOf(IllegalArgumentException.class);
        // 느림 비율도 같은 규칙을 탄다. 하나만 걸면 다른 쪽이 조용히 0 이 된다.
        assertThatThrownBy(() -> 값(null, 0f)).isInstanceOf(IllegalArgumentException.class);
    }

    /** 온전한 한 벌은 받아들인다. 거절만 재면 전부 거절해도 통과한다. */
    @Test
    @DisplayName("온전한_설정은_받아들인다")
    void 온전한_설정은_받아들인다() {
        assertThat(값(null, null).slowCallDurationThreshold()).isEqualTo(느림);
    }

    /** 표본 하한이 0 이면 첫 한 건으로 서킷이 열린다. */
    @Test
    @DisplayName("표본_하한이_0이면_기동을_막는다")
    void 표본_하한이_0이면_기동을_막는다() {
        assertThatThrownBy(() -> new BackendCircuitProperties(
                창, 0, 50f, 느림, 50f, 대기, 반쯤_상한, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackendCircuitProperties(
                창, 20, 50f, 느림, 50f, 대기, 반쯤_상한, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>창은 정수 초로 쓰인다.</b> 라이브러리가 {@code int} 초를 받으므로
     * {@code 500ms} 는 0 이 되어 기동이 실패하고, 아주 큰 값은 잘려 조용히 작은
     * 창이 된다 — 뒤가 더 나쁘다. 순간 변동에 서킷이 열린다.
     */
    @Test
    @DisplayName("창이_정수_초가_아니면_기동을_막는다")
    void 창이_정수_초가_아니면_기동을_막는다() {
        assertThatThrownBy(() -> new BackendCircuitProperties(
                Duration.ofMillis(500), 20, 50f, 느림, 50f, 대기, 반쯤_상한, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sliding-window-size");
        // 밀리초로 재면 통과하는 값. 실제로는 1초로 줄어 적은 값과 달라진다.
        assertThatThrownBy(() -> new BackendCircuitProperties(
                Duration.ofSeconds(1).plusNanos(1), 20, 50f, 느림, 50f, 대기, 반쯤_상한, 10))
                .isInstanceOf(IllegalArgumentException.class);
        // int 로 잘려 작은 창이 되는 값. 양수 검사만으로는 안 걸린다.
        assertThatThrownBy(() -> new BackendCircuitProperties(
                Duration.ofSeconds(4_294_967_306L), 20, 50f, 느림, 50f, 대기, 반쯤_상한, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.gateway.IdempotencyKey.Mode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Locale;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 뒷단에 무엇을 실어 보내는가.
 *
 * <p>뒷단 계약이 UUID 인 곳도 있고 아닌 곳도 있다. 강제를 못 고르면 계약이 다른 뒷단에는
 * 붙일 수 없고, 고르게 하면 그 값이 검증 없이 뒷단 키가 된다.
 */
class IdempotencyModeTest {

    private static final String 쿠폰 = "c1";

    private static final String 회원 = "42";

    private static IdempotencyKey 키(Mode 모드) {
        return IdempotencyKey.of(모드, new SimpleMeterRegistry());
    }

    @Test
    @DisplayName("기본은 지금과 같다 — UUID 로 맞춘다")
    void 기본은_uuid() {
        assertThat(IdempotencyKey.passThrough().mode()).isEqualTo(Mode.UUID);
    }

    @Test
    @DisplayName("UUID 모드는 아닌 값을 안 넘긴다")
    void uuid_모드() {
        String 준_값 = UUID.randomUUID().toString().toUpperCase(Locale.ROOT);

        assertThat(키(Mode.UUID).of(쿠폰, 회원, 준_값))
                .as("표기만 다른 재시도를 뒷단이 두 건으로 보면 안 된다")
                .isEqualTo(준_값.toLowerCase(Locale.ROOT));
        assertThat(키(Mode.UUID).of(쿠폰, 회원, "not-a-uuid"))
                .as("모양이 아니면 대체 값이 나간다").isNotEqualTo("not-a-uuid");
    }

    @Test
    @DisplayName("원문 모드는 준 값을 그대로 넘긴다")
    void 원문_모드() {
        assertThat(키(Mode.RAW).of(쿠폰, 회원, "order-2026-0921-77"))
                .as("뒷단이 UUID 를 안 쓰는 곳에 붙는다")
                .isEqualTo("order-2026-0921-77");
    }

    @Test
    @DisplayName("원문 모드의 대체 값은 UUID 모양이 아니다")
    void 원문_모드_대체값() {
        String 값 = 키(Mode.RAW).of(쿠폰, 회원, null);

        assertThat(값).as("UUID 를 안 쓰기로 한 곳에 UUID 를 만들어 보내지 않는다")
                .doesNotMatch("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab].*");
        assertThat(키(Mode.RAW).of(쿠폰, 회원, null))
                .as("같은 시도에 같은 값이어야 한다").isEqualTo(값);
        assertThat(키(Mode.RAW).of(쿠폰, "43", null))
                .as("다른 회원이 같은 값을 받으면 서로의 발급을 지운다").isNotEqualTo(값);
    }

    @Test
    @DisplayName("원문 모드도 길이와 문자를 본다 — 그 값이 뒷단 키가 된다")
    void 원문_모드_검증() {
        assertThat(키(Mode.RAW).of(쿠폰, 회원, "a".repeat(200)))
                .as("긴 값은 안 넘긴다").isNotEqualTo("a".repeat(200));
        assertThat(키(Mode.RAW).of(쿠폰, 회원, "줄\n바꿈"))
                .as("헤더를 가르는 문자는 안 넘긴다").isNotEqualTo("줄\n바꿈");
    }

    @Test
    @DisplayName("끄면 아무 값도 안 낸다 — 게이트웨이가 헤더를 안 건드린다")
    void 끄기() {
        assertThat(키(Mode.OFF).of(쿠폰, 회원, "order-1"))
                .as("클라이언트가 보낸 것이 그대로 간다").isNull();
        assertThat(키(Mode.OFF).of(쿠폰, 회원, null)).isNull();
    }

    @Test
    @DisplayName("끈 모드도 깨진 값은 거절한다 — 안 건드리는 것과 다르다")
    void 끈_모드도_깨진_값은_거절() {
        assertThat(키(Mode.OFF).accepts(List.of("order-1")))
                .as("멀쩡한 한 줄은 그대로 간다").isTrue();
        assertThat(키(Mode.OFF).accepts(List.of()))
                .as("안 보낸 것은 막을 것이 없다").isTrue();
        assertThat(키(Mode.OFF).accepts(List.of("order-1", "order-2")))
                .as("줄이 둘이면 뒷단이 어느 것을 볼지가 그쪽 구현에 달린다").isFalse();
        assertThat(키(Mode.OFF).accepts(List.of("")))
                .as("빈 키를 뒷단이 유효하게 저장하면 전원이 한 레코드로 뭉친다").isFalse();
        assertThat(키(Mode.OFF).accepts(List.of("a".repeat(200))))
                .as("상한이 사라지면 요청 하나가 뒷단 저장소를 부풀린다").isFalse();
    }

    @Test
    @DisplayName("모드를 안 주면 막는다")
    void 모드_없음() {
        assertThatThrownBy(() -> IdempotencyKey.of(null, new SimpleMeterRegistry()))
                .isInstanceOf(NullPointerException.class);
    }
}

package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 끊긴 발급이 두 번 나가지 않게 하는 키.
 *
 * <p><b>게이트웨이가 끊어도 뒷단은 처리했을 수 있다.</b> 사용자가 다시 시도하면
 * 같은 사람이 두 번 발급된다 (불변식 2). 재사용 방지는 발급 계층의 멱등성이
 * 지고(A-10), 게이트웨이는 같은 시도에 같은 키를 실어 그 근거를 준다.
 */
// **뒷단은 UUID 만 받는다.** 그래서 게이트웨이가 값을 바꾸지 않는다 — 클라이언트가
// 준 UUID 를 그대로 넘기고, 도용 방어는 뒷단이 회원과 키의 쌍으로 저장해서 한다.
// 서명해서 덮던 앞 판은 형식이 안 맞아 발급 경로가 통째로 거절됐다 (CY-830).
class IdempotencyKeyTest {

    private final IdempotencyKey keys = IdempotencyKey.passThrough();

    /** 뒷단 계약이 v4 다. 판 자리가 4, 변종 자리가 8~b 여야 한다. */
    private static final String CLIENT = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";

    /** 클라이언트가 준 값을 그대로 넘긴다. 시도를 가르는 것은 클라이언트다 (C-11). */
    @Test
    @DisplayName("클라이언트_값을_그대로_넘긴다")
    void 클라이언트_값을_그대로_넘긴다() {
        assertThat(keys.of("c1", "m1", CLIENT)).isEqualTo(CLIENT);
    }

    /**
     * <b>대문자도 같은 값으로 본다.</b>
     *
     * <p>같은 UUID 를 대소문자만 다르게 재시도하면 뒷단이 두 건으로 본다.
     * 표준 표기로 맞춰 넘긴다.
     */
    @Test
    @DisplayName("대소문자를_맞춰_넘긴다")
    void 대소문자를_맞춰_넘긴다() {
        assertThat(keys.of("c1", "m1", CLIENT.toUpperCase())).isEqualTo(CLIENT);
    }

    /**
     * <b>값을 안 주면 쿠폰과 회원으로 떨어진다</b> (C-11).
     *
     * <p>그때는 두 번 줄 서서 두 번 차례가 온 사람의 두 번째를 잃을 수 있다.
     * 초과 발급은 타협 불가이고(불변식 2) 잃는 쪽은 아니라, 안전한 방향으로
     * 치우친다.
     */
    @Test
    @DisplayName("값을_안_줘도_같은_사람은_같은_키다")
    void 값을_안_줘도_같은_사람은_같은_키다() {
        assertThat(keys.of("c1", "m1", null)).isEqualTo(keys.of("c1", "m1", null));
        assertThat(keys.of("c1", "m1", null)).isNotEqualTo(keys.of("c1", "m2", null));
        assertThat(keys.of("c1", "m1", null)).isNotEqualTo(keys.of("c2", "m1", null));
    }

    /** 빈 값은 안 준 것과 같다. 헤더를 붙이고 비워 보내는 클라이언트가 있다. */
    @Test
    @DisplayName("빈_값은_안_준_것과_같다")
    void 빈_값은_안_준_것과_같다() {
        assertThat(keys.of("c1", "m1", "   ")).isEqualTo(keys.of("c1", "m1", null));
    }

    /**
     * <b>UUID v4 가 아니면 안 준 것으로 본다.</b> 그대로 넘기면 뒷단이 형식으로
     * 거절해 사용자가 발급을 못 받는다. 게이트웨이가 뒷단 계약을 아는 유일한
     * 자리라 여기서 막는다.
     */
    @Test
    @DisplayName("UUID_v4_가_아니면_안_준_것으로_본다")
    void UUID_v4_가_아니면_안_준_것으로_본다() {
        String 떨어진_것 = keys.of("c1", "m1", null);

        assertThat(keys.of("c1", "m1", "attempt-1")).isEqualTo(떨어진_것);
        assertThat(keys.of("c1", "m1", "3f2504e04f8941d39a0c0305e82c3301"))
                .as("하이픈이 없으면 UUID 표기가 아니다").isEqualTo(떨어진_것);
        // 모양만 보면 이것들이 그대로 나가 뒷단에서 죽는다.
        assertThat(keys.of("c1", "m1", "3f2504e0-4f89-11d3-9a0c-0305e82c3301"))
                .as("v1 은 뒷단이 안 받는다").isEqualTo(떨어진_것);
        assertThat(keys.of("c1", "m1", "3f2504e0-4f89-31d3-9a0c-0305e82c3301"))
                .as("v3 도 안 받는다").isEqualTo(떨어진_것);
        assertThat(keys.of("c1", "m1", "3f2504e0-4f89-41d3-1a0c-0305e82c3301"))
                .as("변종 자리가 틀리면 RFC 4122 가 아니다").isEqualTo(떨어진_것);
    }

    /**
     * 내는 값은 늘 UUID <b>v4</b> 다. 뒷단이 그 판만 받는다 (CY-830).
     */
    @Test
    @DisplayName("늘_UUID_v4_를_낸다")
    void 늘_UUID_v4_를_낸다() {
        // 떨어진 자리가 v3 이면 뒷단이 거절한다. 모양만 보는 단언은 그것을
        // 못 잡는다 — 판 자리를 직접 읽는다.
        assertThat(UUID.fromString(keys.of("c1", "m1", null)).version())
                .as("떨어진 키의 판").isEqualTo(4);
        assertThat(UUID.fromString(keys.of("c1", "m1", null)).variant())
                .as("RFC 4122 변종").isEqualTo(2);

        // **되읽어 같은 값인지까지 본다.** 파싱만 보면 대소문자나 표기가
        // 달라져도 통과하고, 그때 뒷단은 두 건으로 센다.
        String 그대로 = keys.of("c1", "m1", CLIENT);
        assertThat(UUID.fromString(그대로)).hasToString(그대로);

        String 떨어진_것 = keys.of("c1", "m1", null);
        assertThat(UUID.fromString(떨어진_것)).hasToString(떨어진_것);
        assertThat(떨어진_것).hasSize(36);
    }

    /**
     * <b>떨어진 키를 남이 못 만든다고 기대하지 않는다.</b>
     *
     * <p>추측 가능한 값이지만, 도용 방어는 뒷단이 회원과 키의 쌍으로 저장해서
     * 한다. 남의 키를 실어도 자기 회원 앞으로만 쓰인다.
     */
    @Test
    @DisplayName("떨어진_키는_회원마다_다르다")
    void 떨어진_키는_회원마다_다르다() {
        assertThat(keys.of("c1", "m1", null)).isNotEqualTo(keys.of("c1", "m2", null));
    }

    /**
     * <b>재기동해도 같은 값이 나온다.</b> 나머지 시험은 같은 JVM 안에서
     * 자기끼리 비교하므로 재료가 바뀌어도 로케일이 바뀌어도 다 통과한다 —
     * 값을 박아야 그것을 잡는다. 근거는 AIJ-0165.
     */
    @Test
    @DisplayName("떨어진_키를_값으로_못_박는다")
    void 떨어진_키를_값으로_못_박는다() {
        assertThat(keys.of("c1", "m1", null))
                .isEqualTo("f833a946-0ff8-42e0-ac22-8d0ed635a47a");
        assertThat(keys.of("c1", "m2", null))
                .isEqualTo("4df40ba4-ebec-4db4-a078-72eb5e9261f7");
    }

    /**
     * 숫자 표기가 다른 로케일에서도 같은 값이 나온다. 노드마다 로케일이
     * 다르면 같은 사람의 재시도가 두 키로 갈라져 이중 발급이 난다.
     */
    @Test
    @DisplayName("로케일이_달라도_같은_키다")
    void 로케일이_달라도_같은_키다() {
        Locale 원래 = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(Locale.Category.FORMAT,
                    Locale.forLanguageTag("hi-IN-u-nu-deva"));
            assertThat(keys.of("c1", "m1", null))
                    .isEqualTo("f833a946-0ff8-42e0-ac22-8d0ed635a47a");
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, 원래);
        }
    }
}

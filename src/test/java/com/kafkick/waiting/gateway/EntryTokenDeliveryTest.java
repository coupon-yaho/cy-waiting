package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkick.waiting.gateway.EntryTokenDelivery.Where;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 입장 토큰을 <b>어디로 내리고 뒷단이 어느 이름으로 받는가</b>.
 *
 * <p>내리는 자리와 올라오는 헤더 이름이 코드에 박혀 있었다. 계약이 다른 곳에 붙이려면
 * 둘 다 골라야 하는데, 헤더 이름은 그 값이 헤더 줄이 되므로 아무 문자나 받을 수 없다.
 */
class EntryTokenDeliveryTest {

    @Test
    @DisplayName("기본은 지금과 같다 — 바디에만 싣고 이름은 Entry-Token")
    void 기본값() {
        EntryTokenDelivery 기본 = new EntryTokenDelivery(null, null, null);

        assertThat(기본.where()).isEqualTo(Where.BODY);
        assertThat(기본.header()).isEqualTo("Entry-Token");
        assertThat(기본.backendHeader())
                .as("안 적으면 받은 이름 그대로 뒷단에 간다").isEqualTo("Entry-Token");
    }

    @Test
    @DisplayName("뒷단 이름을 따로 줄 수 있다")
    void 뒷단_이름() {
        EntryTokenDelivery 설정 =
                new EntryTokenDelivery(Where.HEADER, "X-Gate-Pass", "X-Backend-Pass");

        assertThat(설정.header()).isEqualTo("X-Gate-Pass");
        assertThat(설정.backendHeader()).isEqualTo("X-Backend-Pass");
        assertThat(설정.renames()).as("이름이 다르면 바꿔 실어야 한다").isTrue();
    }

    @Test
    @DisplayName("같은 이름이면 바꿔 싣지 않는다")
    void 같은_이름() {
        assertThat(new EntryTokenDelivery(Where.BODY, "Entry-Token", "Entry-Token").renames())
                .isFalse();
    }

    @Test
    @DisplayName("헤더 이름에 토큰 문자가 아닌 것이 들어가면 막는다")
    void 이름_검증() {
        for (String 나쁜 : new String[] {"Entry Token", "Entry:Token", "엔트리",
                "Entry\nToken", "", "  "}) {
            assertThatThrownBy(() -> new EntryTokenDelivery(Where.BODY, 나쁜, null))
                    .as("'%s' 가 헤더 줄이 되면 헤더가 갈린다", 나쁜)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @DisplayName("헤더로 내릴 때만 노출 목록에 오른다")
    void 노출_목록() {
        assertThat(new EntryTokenDelivery(Where.BODY, null, null).exposed())
                .as("바디로만 내리면 노출할 것이 없다").isEmpty();
        assertThat(new EntryTokenDelivery(Where.HEADER, "X-Gate-Pass", null).exposed())
                .as("안 올리면 교차 출처 스크립트가 못 읽는다")
                .containsExactly("X-Gate-Pass");
        assertThat(new EntryTokenDelivery(Where.BOTH, null, null).exposed())
                .containsExactly("Entry-Token");
    }
}

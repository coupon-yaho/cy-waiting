package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 비밀키를 돌리는 창 (CY-902).
 *
 * <p>키가 하나뿐이면 롤링 배포 중 <b>새 키 파드가 낸 토큰을 옛 키 파드가 거절</b>한다.
 * 그러면 사다리 2번의 토큰 갈래가 거짓이 되어 그 사람이 아래로 떨어지고 큐에 새로
 * 선다 — 순번 역행이자 추월이다 (불변식 3·4).
 */
@Tag("unit")
class EntryTokenRotationTest {

    private static final Instant 지금 = Instant.parse("2026-09-09T00:00:00Z");

    private static final String 새_키 = "rotation-new-secret-0123456789abcdef";

    private static final String 옛_키 = "rotation-old-secret-0123456789abcdef";

    private static final String 남의_키 = "rotation-other-secret-0123456789abc";

    /**
     * <b>배포 중 양쪽이 서로의 토큰을 받는다.</b> 한쪽만 받으면 그 창 동안 절반의
     * 사람이 자기 차례를 잃는다.
     */
    @Test
    @DisplayName("옛_키로_만든_것을_새_키_파드가_받는다")
    void 옛_키로_만든_것을_새_키_파드가_받는다() {
        EntryToken 옛_파드 = EntryToken.of(옛_키);
        EntryToken 새_파드 = EntryToken.of(새_키, List.of(옛_키));

        String 옛_토큰 = 옛_파드.issue("c1", "m1", 지금);

        assertThat(새_파드.verify(옛_토큰, "c1", 지금)).contains("m1");
    }

    /**
     * <b>발급은 늘 현재 키로 한다.</b> 옛 키로 내면 배포가 끝나고 그 키를 뺀 뒤에
     * 방금 낸 토큰이 죽는다.
     */
    @Test
    @DisplayName("발급은_현재_키로만_한다")
    void 발급은_현재_키로만_한다() {
        EntryToken 새_파드 = EntryToken.of(새_키, List.of(옛_키));
        EntryToken 옛_파드 = EntryToken.of(옛_키);

        String 낸_것 = 새_파드.issue("c1", "m1", 지금);

        assertThat(EntryToken.of(새_키).verify(낸_것, "c1", 지금))
                .as("현재 키로 서명했다").contains("m1");
        assertThat(옛_파드.verify(낸_것, "c1", 지금))
                .as("옛 키만 든 쪽은 못 읽는다 — 그래서 그쪽도 새 키를 받아야 한다")
                .isEmpty();
    }

    /** 목록에 없는 키로 만든 것은 그대로 거절한다. 창을 여는 것이 문을 여는 것은 아니다. */
    @Test
    @DisplayName("목록에_없는_키는_거절한다")
    void 목록에_없는_키는_거절한다() {
        EntryToken 파드 = EntryToken.of(새_키, List.of(옛_키));

        String 남의_토큰 = EntryToken.of(남의_키).issue("c1", "m1", 지금);

        assertThat(파드.verify(남의_토큰, "c1", 지금)).isEmpty();
    }

    /**
     * <b>받아 주는 키도 같은 길이 규칙을 지킨다.</b> 여기만 느슨하면 짧은 옛 키를
     * 남겨 두는 것으로 서명이 뜻을 잃는다.
     */
    @Test
    @DisplayName("받아_주는_키도_짧으면_기동을_막는다")
    void 받아_주는_키도_짧으면_기동을_막는다() {
        assertThatThrownBy(() -> EntryToken.of(새_키, List.of("짧다")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EntryToken.of(새_키, java.util.Collections.singletonList(null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 창을 안 열면 지금과 같다. 목록이 비었다고 아무나 받으면 안 된다. */
    @Test
    @DisplayName("창을_안_열면_현재_키만_받는다")
    void 창을_안_열면_현재_키만_받는다() {
        EntryToken 파드 = EntryToken.of(새_키, List.of());

        assertThat(파드.verify(EntryToken.of(옛_키).issue("c1", "m1", 지금), "c1", 지금))
                .isEmpty();
        assertThat(파드.verify(파드.issue("c1", "m1", 지금), "c1", 지금)).contains("m1");
    }

    /**
     * <b>키를 여럿 받아도 끝까지 본다.</b> 첫 키에서 빠져나가면 목록 뒤쪽 키로
     * 만든 토큰이 거절되고, 그 사실이 배포 중에만 드러난다.
     */
    @Test
    @DisplayName("목록_뒤쪽_키로_만든_것도_받는다")
    void 목록_뒤쪽_키로_만든_것도_받는다() {
        EntryToken 파드 = EntryToken.of(새_키, List.of(남의_키, 옛_키));

        assertThat(파드.verify(EntryToken.of(옛_키).issue("c1", "m1", 지금), "c1", 지금))
                .contains("m1");
    }
}

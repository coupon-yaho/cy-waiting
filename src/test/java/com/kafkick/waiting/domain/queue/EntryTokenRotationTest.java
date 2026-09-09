package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Collections;
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

    /** 배포가 끝나는 때. 여기서부터 이 토큰의 수명만큼만 옛 키를 받는다. */
    private static final Instant 배포_끝 = 지금.minusSeconds(10);

    /**
     * <b>배포 중 양쪽이 서로의 토큰을 받는다.</b> 한쪽만 받으면 그 창 동안 절반의
     * 사람이 자기 차례를 잃는다.
     */
    @Test
    @DisplayName("옛_키로_만든_것을_새_키_파드가_받는다")
    void 옛_키로_만든_것을_새_키_파드가_받는다() {
        EntryToken 옛_파드 = EntryToken.of(옛_키);
        EntryToken 새_파드 = EntryToken.of(새_키, List.of(옛_키), 배포_끝);

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
        EntryToken 새_파드 = EntryToken.of(새_키, List.of(옛_키), 배포_끝);
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
        EntryToken 파드 = EntryToken.of(새_키, List.of(옛_키), 배포_끝);

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
        // null 갈래는 도메인 API 를 직접 부를 때만 온다 — 설정 경로는 List.copyOf 가
        // 먼저 막는다. 분기를 덮되 운영에서 보는 예외가 아니라는 것을 적어 둔다.
        assertThatThrownBy(() -> EntryToken.of(새_키, List.of("짧다"), 배포_끝))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EntryToken.of(새_키, Collections.singletonList(null), 배포_끝))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 창을 안 열면 지금과 같다. 목록이 비었다고 아무나 받으면 안 된다. */
    @Test
    @DisplayName("창을_안_열면_현재_키만_받는다")
    void 창을_안_열면_현재_키만_받는다() {
        EntryToken 파드 = EntryToken.of(새_키, List.of(), 배포_끝);

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
        EntryToken 파드 = EntryToken.of(새_키, List.of(남의_키, 옛_키), 배포_끝);

        assertThat(파드.verify(EntryToken.of(옛_키).issue("c1", "m1", 지금), "c1", 지금))
                .contains("m1");
    }

    /**
     * <b>창이 저절로 닫힌다</b> (CY-902 · 보안 리뷰).
     *
     * <p>HMAC 은 대칭이라 검증에 받아 주는 키는 곧 <b>토큰을 찍을 수 있는 키</b>다.
     * "검증에서만 쓴다" 는 권한을 안 줄인다 — 설정에서 빼야만 닫히면 새는 키가
     * 며칠 더 산다. 열어 둘 이유는 그 키로 낸 마지막 토큰의 수명뿐이다.
     */
    @Test
    @DisplayName("수명이_지나면_옛_키를_안_받는다")
    void 수명이_지나면_옛_키를_안_받는다() {
        Instant 배포_끝 = 지금;
        EntryToken 파드 = EntryToken.of(새_키, List.of(옛_키), 배포_끝);
        String 옛_토큰 = EntryToken.of(옛_키).issue("c1", "m1", 지금);

        Instant 창_안 = 배포_끝.plusSeconds(EntryToken.TTL_SEC - 1);
        assertThat(파드.verify(옛_토큰, "c1", 창_안)).as("창 안에서는 받는다").contains("m1");

        // 그 키로 낸 마지막 토큰이 죽고 나면 더 받아 줄 이유가 없다.
        Instant 창_밖 = 배포_끝.plusSeconds(EntryToken.ACCEPT_WINDOW_SEC + 1);
        assertThat(파드.verify(EntryToken.of(옛_키).issue("c1", "m1", 창_밖), "c1", 창_밖))
                .as("창 밖에서는 새로 찍은 것도 안 받는다").isEmpty();
    }

    /** 창이 닫혀도 현재 키는 그대로다. 닫는 것은 옛 키뿐이다. */
    @Test
    @DisplayName("창이_닫혀도_현재_키는_받는다")
    void 창이_닫혀도_현재_키는_받는다() {
        EntryToken 파드 = EntryToken.of(새_키, List.of(옛_키), 지금);
        Instant 나중 = 지금.plusSeconds(EntryToken.ACCEPT_WINDOW_SEC + 100);

        assertThat(파드.verify(파드.issue("c1", "m1", 나중), "c1", 나중)).contains("m1");
    }

    /** 창을 열면서 언제 돌렸는지를 안 적으면 기동을 막는다. 모르면 못 닫는다. */
    @Test
    @DisplayName("배포_끝가_없으면_창을_못_연다")
    void 배포_끝가_없으면_창을_못_연다() {
        assertThatThrownBy(() -> EntryToken.of(새_키, List.of(옛_키), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 현재 키를 옛 키로도 적으면 돌린 것이 아니다. 그 착각을 기동에서 막는다. */
    @Test
    @DisplayName("현재_키를_옛_키로_적으면_막는다")
    void 현재_키를_옛_키로_적으면_막는다() {
        assertThatThrownBy(() -> EntryToken.of(새_키, List.of(새_키), 배포_끝))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EntryToken.of(새_키, List.of(옛_키, 옛_키), 배포_끝))
                .as("중복도 막는다").isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>목록이 자라면 요청당 비용이 그만큼 곱해진다.</b> 검증이 폴링 상한보다
     * 앞이라 인증 없는 요청 하나가 키 수만큼 HMAC 을 돌린다.
     */
    @Test
    @DisplayName("옛_키는_두_개까지만_받는다")
    void 옛_키는_두_개까지만_받는다() {
        assertThatThrownBy(() -> EntryToken.of(새_키,
                List.of(옛_키, 남의_키, "rotation-third-secret-0123456789abc"), 배포_끝))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 옛 키로 받은 횟수를 센다. 누적이라 더 안 오르는 때가 창을 닫아도 되는 때다. */
    @Test
    @DisplayName("옛_키로_받은_횟수를_센다")
    void 옛_키로_받은_횟수를_센다() {
        EntryToken 파드 = EntryToken.of(새_키, List.of(옛_키), 배포_끝);

        파드.verify(EntryToken.of(옛_키).issue("c1", "m1", 지금), "c1", 지금);
        파드.verify(파드.issue("c1", "m2", 지금), "c1", 지금);

        assertThat(파드.acceptedByPrevious()).as("옛 키로 맞은 것만 센다").isEqualTo(1);
    }

    /**
     * <b>받아 준 것만 센다.</b> 서명만 맞고 만료·쿠폰에서 걸린 것을 세면 창을
     * 닫아도 되는 때를 그만큼 늦게 본다 — 그 수가 유일한 신호다.
     */
    @Test
    @DisplayName("거절한_옛_키_토큰은_안_센다")
    void 거절한_옛_키_토큰은_안_센다() {
        EntryToken 파드 = EntryToken.of(새_키, List.of(옛_키), 배포_끝);
        String 옛_토큰 = EntryToken.of(옛_키).issue("c1", "m1", 지금);

        파드.verify(옛_토큰, "다른쿠폰", 지금);
        파드.verify(옛_토큰, "c1", 지금.plusSeconds(EntryToken.TTL_SEC + 1));

        assertThat(파드.acceptedByPrevious()).as("서명만 맞은 것은 안 센다").isZero();
    }
}

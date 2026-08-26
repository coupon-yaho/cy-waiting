package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 순번 조회의 신원 수단.
 *
 * <p>로그인이 없어 {@code X-Member-Id} 는 위조 가능하다. 헤더로 대상을 특정하면
 * <b>헤더 하나로 남의 순번을 본다.</b> 그래서 게이트웨이가 서명한 것만 믿는다.
 */
class QueueTokenTest {

    private static final Instant 지금 = Instant.parse("2026-08-24T00:00:00Z");
    private static final String SECRET = "not-a-real-secret-0123456789abcdef";

    private final QueueToken token = QueueToken.of(SECRET);

    @Test
    @DisplayName("발급한_것을_되읽는다")
    void 발급한_것을_되읽는다() {
        String issued = token.issue("c1", "m1", 지금);

        assertThat(token.verify(issued, "c1", 지금))
                .contains("m1");
    }

    /** 서명이 없으면 아무 문자열이나 남의 자리를 가리킬 수 있다. */
    @Test
    @DisplayName("서명을_고치면_거절한다")
    void 서명을_고치면_거절한다() {
        String issued = token.issue("c1", "m1", 지금);
        String 위조 = issued.substring(0, issued.length() - 2)
                + (issued.endsWith("AA") ? "BB" : "AA");

        assertThat(token.verify(위조, "c1", 지금)).isEmpty();
    }

    @Test
    @DisplayName("내용을_고치면_거절한다")
    void 내용을_고치면_거절한다() {
        String 남의_것 = token.issue("c1", "m2", 지금);
        String 내_것 = token.issue("c1", "m1", 지금);
        // 서명만 떼어 붙인다. 페이로드와 서명을 따로 검사하면 통과한다.
        String 짜깁기 = 남의_것.split("\\.")[0] + "." + 내_것.split("\\.")[1];

        assertThat(token.verify(짜깁기, "c1", 지금)).isEmpty();
    }

    /** 다른 쿠폰의 토큰이 통하면 한 번 받은 토큰으로 모든 줄을 들여다본다. */
    @Test
    @DisplayName("다른_쿠폰의_토큰을_거절한다")
    void 다른_쿠폰의_토큰을_거절한다() {
        String issued = token.issue("c1", "m1", 지금);

        assertThat(token.verify(issued, "c2", 지금)).isEmpty();
    }

    /** 정한 것보다 오래 살면 유출된 토큰의 조회 권한도 그만큼 오래 간다. */
    @Test
    @DisplayName("정한_수명을_안_넘는다")
    void 정한_수명을_안_넘는다() {
        assertThat(token.verify(token.issue("c1", "m1", 지금), "c1",
                지금.plusSeconds(QueueToken.TTL_SEC + 1))).isEmpty();
        // 창 끝에서 받아도 마찬가지다.
        Instant 창_끝 = 지금.plusSeconds(599);
        assertThat(token.verify(token.issue("c1", "m1", 창_끝), "c1",
                창_끝.plusSeconds(QueueToken.TTL_SEC + 1))).isEmpty();
    }

    /**
     * 창 끝에 받은 사람의 토큰이 몇 초만 살면 안 된다. 대기가 그보다 길다.
     */
    @Test
    @DisplayName("창_끝에_받아도_충분히_산다")
    void 창_끝에_받아도_충분히_산다() {
        Instant 창_끝 = 지금.plusSeconds(599);

        String issued = token.issue("c1", "m1", 창_끝);

        // 최소 수명은 한 주기에서 창 하나를 뺀 값이다.
        assertThat(token.verify(issued, "c1", 창_끝.plusSeconds(QueueToken.TTL_SEC - 600)))
                .contains("m1");
    }

    /**
     * 쿠폰 이름에 구분자가 섞이면 한 필드가 둘로 쪼개져 만료 자리에 남의 값이 온다.
     */
    @Test
    @DisplayName("구분자가_섞인_쿠폰은_안_통한다")
    void 구분자가_섞인_쿠폰은_안_통한다() {
        String 섞인_쿠폰 = "c1" + (char) 0x1f + "9999999999";

        assertThat(token.verify(token.issue(섞인_쿠폰, "m1", 지금), 섞인_쿠폰, 지금)).isEmpty();
    }

    /** 서명 자리에 아무 글자나 들어온다. 디코딩부터 터지면 500 이 나간다. */
    @Test
    @DisplayName("서명이_형식부터_틀려도_거절한다")
    void 서명이_형식부터_틀려도_거절한다() {
        String issued = token.issue("c1", "m1", 지금);
        String 앞부분 = issued.substring(0, issued.indexOf('.'));

        assertThat(token.verify(앞부분 + ".!!!!", "c1", 지금)).isEmpty();
    }

    /**
     * 둘은 같은 비밀키를 쓰고 페이로드 형식도 같다. 접두를 서명에 안 넣으면
     * 접두만 바꿔 끼우는 것으로 다른 쓰임의 토큰이 된다 — 줄을 통째로 건너뛴다.
     */
    @Test
    @DisplayName("다른_쓰임의_토큰은_접두를_바꿔도_안_통한다")
    void 다른_쓰임의_토큰은_접두를_바꿔도_안_통한다() {
        String 남의_쓰임 = EntryToken.of(SECRET).issue("c1", "m1", 지금);
        String 접두를_바꾼_것 = 남의_쓰임.replaceFirst("^[a-z]+_", 접두());

        assertThat(token.verify(접두를_바꾼_것, "c1", 지금)).isEmpty();
    }

    private String 접두() {
        String 내_것 = token.issue("c1", "m1", 지금);
        return 내_것.substring(0, 내_것.indexOf('_') + 1);
    }

    @Test
    @DisplayName("구분자가_없으면_거절한다")
    void 구분자가_없으면_거절한다() {
        assertThat(token.verify("qt_abcdef", "c1", 지금)).isEmpty();
    }

    /** 사유를 나누면 어디를 고쳐야 하는지 알려 주는 셈이다. */
    @Test
    @DisplayName("거절_사유를_나누지_않는다")
    void 거절_사유를_나누지_않는다() {
        Optional<String> 서명_틀림 = token.verify("qt_aaa.bbb", "c1", 지금);
        Optional<String> 형식_틀림 = token.verify("아무거나", "c1", 지금);
        Optional<String> 없음 = token.verify(null, "c1", 지금);

        assertThat(서명_틀림).isEmpty();
        assertThat(형식_틀림).isEmpty();
        assertThat(없음).isEmpty();
    }

    /** 약한 키로 조용히 돌면 서명이 있다는 사실이 무의미해진다. */
    @Test
    @DisplayName("짧은_비밀키로는_안_만들어진다")
    void 짧은_비밀키로는_안_만들어진다() {
        assertThatThrownBy(() -> QueueToken.of("짧다"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueueToken.of(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("같은_사람에게_늘_같은_토큰을_준다")
    void 같은_사람에게_늘_같은_토큰을_준다() {
        // 새로고침 연타로 토큰이 갈리면 앞서 받은 토큰이 조용히 죽는다.
        assertThat(token.issue("c1", "m1", 지금))
                .isEqualTo(token.issue("c1", "m1", 지금));
    }

    @Test
    @DisplayName("여러_스레드가_같이_써도_안_깨진다")
    void 여러_스레드가_같이_써도_안_깨진다() {
        // Mac 인스턴스를 공유하면 서명이 섞여 자기 토큰이 자기 검증에 떨어진다.
        assertThat(IntStream.range(0, 500).parallel()
                .allMatch(i -> token.verify(
                        token.issue("c1", "m" + i, 지금), "c1", 지금).isPresent()))
                .isTrue();
    }
}

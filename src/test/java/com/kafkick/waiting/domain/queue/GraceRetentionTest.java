package com.kafkick.waiting.domain.queue;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 두 수명이 서로를 모르면 <b>표시가 토큰보다 먼저 사라진다</b>.
 *
 * <p>그러면 아직 유효한 토큰을 든 사람이 폴링에서 종료를 받고, 다시 서면 그
 * 사이 온 사람들 뒤로 간다 — 순번 역행이다.
 */
class GraceRetentionTest {

    @Test
    @DisplayName("보관_기간이_토큰_수명보다_길다")
    void 보관_기간이_토큰_수명보다_길다() {
        assertThat(GraceRetention.SECONDS).isGreaterThan(EntryToken.TTL_SEC);
    }

    /**
     * <b>여유가 우연이면 안 된다.</b> 토큰 수명을 늘리면 이 값이 따라와야 한다 —
     * 지금은 두 배라, 토큰이 배로 늘어도 관계가 유지된다.
     */
    @Test
    @DisplayName("토큰_수명이_늘어도_관계가_유지된다")
    void 토큰_수명이_늘어도_관계가_유지된다() {
        assertThat(GraceRetention.SECONDS).isGreaterThanOrEqualTo(EntryToken.TTL_SEC * 2);
    }

    /** 재방문자 식별에 필요한 최소치는 따로 있다. 토큰이 짧아도 그 아래로 안 간다. */
    @Test
    @DisplayName("재방문자_식별_최소치_아래로는_안_간다")
    void 재방문자_식별_최소치_아래로는_안_간다() {
        assertThat(GraceRetention.SECONDS).isGreaterThanOrEqualTo(300);
    }
}

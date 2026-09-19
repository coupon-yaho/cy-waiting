package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 레디스가 쓰기를 거부한 것인가 (CY-970).
 *
 * <p>거부와 단절은 대처가 다르다. 앞은 메모리를 줄여야 풀리고 뒤는 연결이 돌아오면 풀린다.
 */
class WriteRefusalTest {

    /** 레디스가 실제로 내는 문장이다. 앞뒤에 스크립트 해시가 붙어 오기도 한다. */
    private static final String OOM = "OOM command not allowed when used memory > 'maxmemory'.";

    @Test
    @DisplayName("상한_메시지를_거부로_읽는다")
    void 상한_메시지를_거부로_읽는다() {
        assertThat(WriteRefusal.refused(new IllegalStateException(OOM))).isTrue();
    }

    /** 스크립트 실행은 원인을 감싸서 온다. 겉만 보면 그 경로가 통째로 안 세어진다. */
    @Test
    @DisplayName("감싸인_원인도_찾는다")
    void 감싸인_원인도_찾는다() {
        Throwable 감싼_것 = new IllegalStateException("스크립트 실행 실패", new IOException(OOM));

        assertThat(WriteRefusal.refused(감싼_것)).isTrue();
    }

    /** 단절을 거부로 세면 메모리를 안 줄여도 될 상황에 줄이러 간다. */
    @Test
    @DisplayName("단절은_거부가_아니다")
    void 단절은_거부가_아니다() {
        assertThat(WriteRefusal.refused(new IOException("Connection reset by peer"))).isFalse();
        assertThat(WriteRefusal.refused(null)).isFalse();
    }

    /** 원인이 제 자신을 가리키면 훑기가 안 끝난다. */
    @Test
    @DisplayName("원인이_도는_예외도_끝난다")
    void 원인이_도는_예외도_끝난다() {
        Throwable 도는_것 = new IllegalStateException("돈다");
        도는_것.initCause(도는_것);

        assertThat(WriteRefusal.refused(도는_것)).isFalse();
    }
}

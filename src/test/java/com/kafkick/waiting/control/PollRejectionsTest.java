package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 이 노드가 조회를 상한으로 거절했다는 표시. <b>하트비트가 실어 보낸 만큼만 내린다</b> — 싣는 사이에 새로 난
 * 거절까지 지우면 그 거절을 클러스터가 영영 모른다.
 */
class PollRejectionsTest {

    @Test
    @DisplayName("거절이_없으면_실을_것이_없다")
    void 거절이_없으면_실을_것이_없다() {
        assertThat(PollRejections.create().mark()).isZero();
    }

    @Test
    @DisplayName("성공한_하트비트가_실은_만큼만_내린다")
    void 성공한_하트비트가_실은_만큼만_내린다() {
        PollRejections 거절 = PollRejections.create();
        거절.rejected();
        long 실은_것 = 거절.mark();
        // 싣고 답을 기다리는 사이 또 거절한다.
        거절.rejected();

        거절.settled(실은_것);
        assertThat(거절.mark()).as("나중 거절은 남는다").isEqualTo(2);

        거절.settled(거절.mark());
        assertThat(거절.mark()).isZero();
    }
}

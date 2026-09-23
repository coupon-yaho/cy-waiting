package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
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

    /**
     * 마지막 거절을 실은 뒤에도 성공한 하트비트 몇 번 동안 계속 싣는다. 한 번만 실으면 표시가 해시에 한 박자만 남아,
     * 리더의 하트비트가 그 틈을 비켜 가면 그 거절을 영영 못 본다. 실패한 하트비트는 줄이지 않는다.
     */
    @Test
    @DisplayName("마지막_거절_뒤에도_몇_번_더_싣는다")
    void 마지막_거절_뒤에도_몇_번_더_싣는다() {
        PollRejections 거절 = PollRejections.create(3);
        거절.rejected();
        List<Boolean> 실은_것 = new ArrayList<>();

        for (int i = 0; i < 6; i++) {
            long 표 = 거절.mark();
            실은_것.add(거절.sending(표));
            if (i == 2) {
                continue;   // 이 하트비트는 실패했다
            }
            거절.settled(표);
        }

        // 거절을 실은 한 번 + 성공한 세 번. 실패한 셋째는 안 센다.
        assertThat(실은_것).containsExactly(true, true, true, true, true, false);
    }
}

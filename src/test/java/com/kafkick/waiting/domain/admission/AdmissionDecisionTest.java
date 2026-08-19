package com.kafkick.waiting.domain.admission;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 판정값은 통과·큐·거절 중 하나로 분류된다.
 *
 * <p>값을 추가하면서 분류를 빠뜨리면 응답 작성이 조용히 기본값으로 떨어진다.
 * 그래서 분류 누락 자체를 테스트가 잡는다 (PK-A3).
 */
class AdmissionDecisionTest {

    @Test
    @DisplayName("모든_판정값은_통과_큐_거절_중_하나로_분류된다")
    void 모든_판정값은_통과_큐_거절_중_하나로_분류된다() {
        for (AdmissionDecision d : AdmissionDecision.values()) {
            int classes = (d.isPass() ? 1 : 0) + (d.isEnqueue() ? 1 : 0) + (d.isReject() ? 1 : 0);
            assertThat(classes)
                    .withFailMessage("%s 가 정확히 한 분류에 속하지 않는다 (분류 수 %d)", d, classes)
                    .isEqualTo(1);
        }
    }

    @Test
    @DisplayName("통과_판정은_넷이다")
    void 통과_판정은_넷이다() {
        assertThat(Arrays.stream(AdmissionDecision.values()).filter(AdmissionDecision::isPass))
                .containsExactlyInAnyOrder(
                        AdmissionDecision.PASS_TOKEN,
                        AdmissionDecision.PASS_BYPASS,
                        AdmissionDecision.PASS_FAIL_OPEN,
                        AdmissionDecision.PASS_UNDER_CAP);
    }

    @Test
    @DisplayName("큐_판정은_다섯이다")
    void 큐_판정은_다섯이다() {
        assertThat(Arrays.stream(AdmissionDecision.values()).filter(AdmissionDecision::isEnqueue))
                .containsExactlyInAnyOrder(
                        AdmissionDecision.ENQUEUE_STALE,
                        AdmissionDecision.ENQUEUE_ALWAYS,
                        AdmissionDecision.ENQUEUE_BACKLOG,
                        AdmissionDecision.ENQUEUE_RATE_COUPON,
                        AdmissionDecision.ENQUEUE_RATE_GLOBAL);
    }

    @Test
    @DisplayName("거절_판정은_넷이다")
    void 거절_판정은_넷이다() {
        assertThat(Arrays.stream(AdmissionDecision.values()).filter(AdmissionDecision::isReject))
                .containsExactlyInAnyOrder(
                        AdmissionDecision.REJECT_SOLD_OUT,
                        AdmissionDecision.REJECT_QUEUE_FULL,
                        AdmissionDecision.REJECT_OVERLOAD,
                        AdmissionDecision.RETRY_TOKEN);
    }
}

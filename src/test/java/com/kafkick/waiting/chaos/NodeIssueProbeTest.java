package com.kafkick.waiting.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 판정식이 틀리면 카오스가 초록인 채로 아무것도 안 잰다. 식 자체를 여기서 못 박는다. */
class NodeIssueProbeTest {

    @Test
    @DisplayName("전원이_503_이어야_되돌려_보낸_것이다")
    void 전원이_503_이어야_되돌려_보낸_것이다() {
        assertThat(NodeIssueProbe.되돌려_보냈다("노드", List.of(503, 503))).isEmpty();
        assertThat(NodeIssueProbe.되돌려_보냈다("노드", List.of(503, 200))).isPresent();
        assertThat(NodeIssueProbe.되돌려_보냈다("노드", List.of(503, 502))).isPresent();
        assertThat(NodeIssueProbe.되돌려_보냈다("노드", List.of())).as("보낸 것이 없다").isPresent();
    }

    @Test
    @DisplayName("닫은_수가_보낸_수와_같아야_등록_실패로_닫은_것이다")
    void 닫은_수가_보낸_수와_같아야_등록_실패로_닫은_것이다() {
        assertThat(NodeIssueProbe.등록_실패로_닫았다("유지", 3, 3)).isEmpty();
        assertThat(NodeIssueProbe.등록_실패로_닫았다("유지", 2, 3)).isPresent();
        assertThat(NodeIssueProbe.등록_실패로_닫았다("유지", 0, 3)).isPresent();
    }

    @Test
    @DisplayName("되돌리는_한계는_차례마다_시한과_여유를_더한다")
    void 되돌리는_한계는_차례마다_시한과_여유를_더한다() {
        assertThat(NodeIssueProbe.되돌리는_한계(Duration.ofMillis(500), 3)).isEqualTo(Duration.ofSeconds(3));
    }

    @Test
    @DisplayName("한계와_같으면_곧바로고_넘으면_아니다")
    void 한계와_같으면_곧바로고_넘으면_아니다() {
        Duration 한계 = Duration.ofSeconds(5);

        assertThat(NodeIssueProbe.곧바로_답했다("유지", 한계, 한계)).isEmpty();
        assertThat(NodeIssueProbe.곧바로_답했다("유지", 한계.plusMillis(1), 한계)).isPresent();
    }
}

package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 클러스터가 뺀 뒷단. 노드마다 따로 센 배제를 과반으로 접는다. <b>분모는 산 노드 수다</b> —
 * 롤아웃 중 새 노드 둘만 실어도 그 둘이 과반이 되면, 옛 노드가 아직 보내는 대의 예산이 지워진다.
 */
class ClusterEjectionTest {

    @Test
    @DisplayName("과반이_뺀_인스턴스만_고른다")
    void 과반이_뺀_인스턴스만_고른다() {
        assertThat(ClusterEjection.majority(5, Map.of("x", 3, "y", 2))).containsExactly("x");
    }

    @Test
    @DisplayName("절반은_과반이_아니다")
    void 절반은_과반이_아니다() {
        assertThat(ClusterEjection.majority(4, Map.of("x", 2))).isEmpty();
    }

    @Test
    @DisplayName("분모는_실은_수가_아니라_산_수다")
    void 분모는_실은_수가_아니라_산_수다() {
        assertThat(ClusterEjection.majority(30, Map.of("x", 2))).isEmpty();
    }

    @Test
    @DisplayName("한_대뿐이면_그_한_대가_전부다")
    void 한_대뿐이면_그_한_대가_전부다() {
        assertThat(ClusterEjection.majority(1, Map.of("x", 1))).containsExactly("x");
    }

    /** 0 은 관측이 망가졌다는 뜻이다. 망가진 관측으로 예산을 깎으면 없는 장애를 만든다. */
    @Test
    @DisplayName("관측이_없으면_아무것도_안_뺀다")
    void 관측이_없으면_아무것도_안_뺀다() {
        assertThat(ClusterEjection.majority(0, Map.of("x", 1))).isEmpty();
    }
}

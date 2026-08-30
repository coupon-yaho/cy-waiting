package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkick.waiting.domain.admission.CircuitState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 클러스터가 본 서킷 (CY-791).
 *
 * <p>배분이 리더 한 대의 로컬 관측을 쓰면 두 방향으로 틀린다 — 리더만 정상이면
 * 나머지가 다 열려 있어도 평소 속도로 돌고, 리더만 열려 있으면 멀쩡한 노드들의
 * 배분까지 0 이 된다. 노드들이 실어 보낸 표를 접어 판단한다.
 */
class ClusterCircuitTest {

    private CircuitState 본다(int alive, int open, int halfOpen) {
        return ClusterCircuit.of(alive, open, halfOpen);
    }

    @Test
    @DisplayName("아무도_안_열렸으면_닫힌_것이다")
    void 아무도_안_열렸으면_닫힌_것이다() {
        assertThat(본다(20, 0, 0)).isEqualTo(CircuitState.CLOSED);
    }

    /** 과반이 열렸으면 뒷단이 죽은 것이다. 배분을 멈춘다. */
    @Test
    @DisplayName("과반이_열렸으면_열린_것이다")
    void 과반이_열렸으면_열린_것이다() {
        assertThat(본다(20, 11, 0)).isEqualTo(CircuitState.OPEN);
    }

    /**
     * <b>소수만 열렸으면 부분 장애다.</b> 그 노드들의 뒷단 경로만 나쁠 수 있다 —
     * 전면 정지는 과하고, 평소 속도는 위험하다. 그 사이를 고른다.
     */
    @Test
    @DisplayName("소수만_열렸으면_조인다")
    void 소수만_열렸으면_조인다() {
        assertThat(본다(20, 1, 0)).isEqualTo(CircuitState.HALF_OPEN);
        assertThat(본다(20, 10, 0)).as("정확히 반은 아직 과반이 아니다")
                .isEqualTo(CircuitState.HALF_OPEN);
    }

    /**
     * <b>반쯤 열린 노드가 있으면 전면 정지가 아니다.</b>
     *
     * <p>반쯤 열림은 뒷단을 지금 시험하는 중이라는 뜻이다. 여기서 배분을 0 으로
     * 만들면 그 시험에 쓸 호출이 하나도 안 나가고, 서킷은 표본을 못 채워 다시
     * 닫힐 길을 잃는다 — <b>진입만 있고 해제가 없는 상태를 우리가 만든다.</b>
     * 뒷단이 살아나도 사람이 파드를 재시작해야 풀린다.
     */
    @Test
    @DisplayName("전_노드가_반쯤_열려도_배분을_안_멈춘다")
    void 전_노드가_반쯤_열려도_배분을_안_멈춘다() {
        assertThat(본다(20, 0, 20)).isEqualTo(CircuitState.HALF_OPEN);
    }

    /**
     * 열린 노드가 과반이어도 시험 중인 노드가 있으면 안 멈춘다.
     *
     * <p>열린 시각이 거의 같으면 대기 시간도 같이 끝나므로, 회복 구간에서 이
     * 조합이 정상이다. 여기서 멈추면 회복이 그 순간마다 끊긴다.
     */
    @Test
    @DisplayName("시험_중인_노드가_있으면_과반이어도_안_멈춘다")
    void 시험_중인_노드가_있으면_과반이어도_안_멈춘다() {
        assertThat(본다(20, 15, 1)).isEqualTo(CircuitState.HALF_OPEN);
    }

    /** 한 대뿐이면 그 한 대가 곧 클러스터다. */
    @Test
    @DisplayName("한_대뿐이면_그_한_대가_전부다")
    void 한_대뿐이면_그_한_대가_전부다() {
        assertThat(본다(1, 1, 0)).isEqualTo(CircuitState.OPEN);
        assertThat(본다(1, 0, 1)).isEqualTo(CircuitState.HALF_OPEN);
        assertThat(본다(1, 0, 0)).isEqualTo(CircuitState.CLOSED);
    }

    /**
     * <b>아직 아무 관측이 없으면 닫힌 것으로 본다.</b> 모른다고 조이면 기동
     * 직후에 배분이 안 돌고, 그 상태가 첫 하트비트까지 이어진다.
     */
    @Test
    @DisplayName("관측이_없으면_닫힌_것으로_본다")
    void 관측이_없으면_닫힌_것으로_본다() {
        assertThat(본다(0, 0, 0)).isEqualTo(CircuitState.CLOSED);
        assertThat(본다(0, 5, 0)).as("말이 안 되는 조합도 안전한 쪽으로")
                .isEqualTo(CircuitState.CLOSED);
    }

    /** 조이는 정도를 순서로 못 박는다. 비대칭 판단이 이 순서를 읽는다. */
    @Test
    @DisplayName("조이는_정도에_순서가_있다")
    void 조이는_정도에_순서가_있다() {
        assertThat(ClusterCircuit.severity(CircuitState.CLOSED))
                .isLessThan(ClusterCircuit.severity(CircuitState.HALF_OPEN));
        assertThat(ClusterCircuit.severity(CircuitState.HALF_OPEN))
                .isLessThan(ClusterCircuit.severity(CircuitState.OPEN));
    }
}

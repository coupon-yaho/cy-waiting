package com.kafkick.waiting.control;

import com.kafkick.waiting.domain.admission.CircuitState;

/**
 * 클러스터가 본 서킷 (CY-791).
 *
 * <p>배분이 리더 한 대의 로컬 관측을 쓰면 두 방향으로 틀린다 — 리더만 정상이면
 * 나머지가 다 열려 있어도 평소 속도로 돌고, 리더만 열려 있으면 멀쩡한 노드들의
 * 배분까지 0 이 된다. 노드들이 하트비트에 실어 보낸 것을 세어 판단한다.
 */
public final class ClusterCircuit {

    private ClusterCircuit() {
    }

    /**
     * 살아 있는 노드 중 <b>닫히지 않았다고 말한 수</b>로 판단한다.
     *
     * @param alive     살아 있는 노드 수
     * @param notClosed 그중 서킷이 닫히지 않았다고 말한 수
     */
    // **관측이 없으면 닫힌 것으로 본다.** 모른다고 조이면 기동 직후에 배분이
    // 안 돌고, 그 상태가 첫 하트비트까지 이어진다 — 없는 장애를 만든다.
    public static CircuitState of(int alive, int notClosed) {
        if (alive < 1 || notClosed < 1) {
            return CircuitState.CLOSED;
        }
        // **과반이라야 전면 정지다.** 소수만 열린 것은 그 노드들의 뒷단 경로만
        // 나쁜 경우일 수 있다. 그때 전 클러스터를 세우면 멀쩡한 노드가 낸
        // 관측 하나로 배분이 멎는다.
        if (notClosed * 2 > alive) {
            return CircuitState.OPEN;
        }
        // 소수는 조인다. 평소 속도로 두면 그 노드들 뒤의 약한 뒷단이 계속 맞는다.
        return CircuitState.HALF_OPEN;
    }
}

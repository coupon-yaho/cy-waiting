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
     * 살아 있는 노드가 낸 표를 접는다.
     *
     * @param alive    살아 있는 노드 수
     * @param open     그중 서킷이 열렸다고 말한 수
     * @param halfOpen 그중 반쯤 열렸다고 말한 수
     */
    // **분모는 표를 낸 수가 아니라 살아 있는 수다.** 표를 안 낸 노드는 안 조이는
    // 쪽으로 접힌다. 롤아웃 중에는 옛 노드가 표를 안 내는데, 그때 표를 낸 소수만
    // 분모로 쓰면 두 대 중 두 대가 열린 것만으로 서른 대 클러스터가 멈춘다.
    public static CircuitState of(int alive, int open, int halfOpen) {
        // **여기는 기동 직후가 아니다.** 기동 직후는 등록부의 초기값이 맡는다.
        // 스크립트가 자기 자신을 먼저 쓰므로 정상이면 alive 는 최소 1 이고,
        // 0 이 왔다는 것은 관측이 망가졌다는 뜻이다. 망가진 관측으로 조이면
        // 없는 장애를 만들므로 안 조이는 쪽으로 접는다.
        if (alive < 1) {
            return CircuitState.CLOSED;
        }
        // **반쯤 열린 노드가 하나라도 있으면 전면 정지가 아니다.** 반쯤 열림은
        // "뒷단을 지금 시험하는 중" 이라는 뜻이고, 배분을 0 으로 만들면 그
        // 시험에 쓸 호출이 하나도 안 나간다 — 서킷이 표본을 못 채워 다시 닫힐
        // 길이 없어진다. 진입만 있고 해제가 없는 상태를 우리가 만드는 셈이다.
        //
        // 뒤집어 말하면 OPEN 은 **클러스터 어디에도 프로브가 없을 때만** 나온다.
        // 열린 노드는 어차피 호출을 안 내보내므로, 그때의 크레딧 0 은 아무것도
        // 굶기지 않는다. 대신 노드들의 대기 시간이 어긋나 있으면 장애 내내
        // 누군가는 시험 중이라 OPEN 에 잘 도달하지 않는다 — 의도한 방향이다.
        if (halfOpen > 0) {
            return CircuitState.HALF_OPEN;
        }
        // **과반이라야 전면 정지다.** 소수만 열린 것은 그 노드들의 뒷단 경로만
        // 나쁜 경우일 수 있다. 그때 전 클러스터를 세우면 멀쩡한 노드가 낸
        // 관측 하나로 배분이 멎는다.
        if (open * 2 > alive) {
            return CircuitState.OPEN;
        }
        // 소수는 조인다. 평소 속도로 두면 그 노드들 뒤의 약한 뒷단이 계속 맞는다.
        return open > 0 ? CircuitState.HALF_OPEN : CircuitState.CLOSED;
    }

    /**
     * 한 계단만 푼다. <b>건너뛰지 않는다</b> — OPEN 에서 바로 CLOSED 로 가면
     * 그 사이를 한 번도 확인하지 않은 채 전면 개방이 일어난다.
     */
    static CircuitState eased(CircuitState from) {
        return from == CircuitState.OPEN ? CircuitState.HALF_OPEN : CircuitState.CLOSED;
    }

    /** 조이는 정도. 큰 쪽이 더 조인다 — 방향을 비교하는 자리마다 다시 적지 않는다. */
    static int severity(CircuitState state) {
        return switch (state) {
            case CLOSED -> 0;
            case HALF_OPEN -> 1;
            case OPEN -> 2;
        };
    }
}

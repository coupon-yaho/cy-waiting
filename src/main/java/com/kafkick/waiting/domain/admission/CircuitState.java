package com.kafkick.waiting.domain.admission;

/**
 * 뒷단 서킷의 상태. <b>판정의 입력이다.</b> 판정이 모르면 half-open 구간에
 * 도착한 트래픽이 통째로 약한 뒷단에 꽂혀 회복이 지연된다. 상태 셋만 받는 것은
 * 도메인이 resilience4j 를 알면 그날부터 그 라이브러리에 묶이기 때문이다.
 */
public enum CircuitState {

    /** 정상. 판정이 서킷을 안 본 것과 같다. */
    CLOSED,

    /** 열렸다. 뒷단이 못 받는다는 가장 직접적인 증거다 — 신규 유입은 줄로 간다. */
    OPEN,

    /** 반쯤 열렸다. 회복 판정이 공정하려면 <b>소량만</b> 닿아야 한다. */
    HALF_OPEN
}

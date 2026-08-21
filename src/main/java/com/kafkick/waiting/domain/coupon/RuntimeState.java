package com.kafkick.waiting.domain.coupon;

/**
 * 쿠폰이 <b>지금 실제로</b> 어떤 상태인가. 기계가 관측한 값이다.
 *
 * <p>정책({@link QueueMode})과 섞지 않는다 — 섞으면 "붐빈다"와 "줄을 세우기로
 * 했다"를 구분할 수 없다.
 */
public enum RuntimeState {

    /** 줄이 없다. 상한 안이면 대기열 없이 통과시킨다 — 이 제품의 존재 이유(R1). */
    IDLE,

    /** 줄이 서 있다. 신규 유입은 뒤에 선다. */
    QUEUEING,

    /**
     * 이번 틱 백로그가 몫 이하다 — {@code credit >= waiting}.
     *
     * <p>줄이 곧 없어진다는 뜻은 <b>아니다.</b> 신규 유입이 계속 들어오면
     * (ALWAYS 모드, 지속 과부하) 다음 틱에 다시 {@code QUEUEING} 이 된다.
     */
    DRAINING,

    /** 재고가 소진됐는데 대기자가 남아 있다. 배분 대상에서 빠진다. */
    CLOSED
}

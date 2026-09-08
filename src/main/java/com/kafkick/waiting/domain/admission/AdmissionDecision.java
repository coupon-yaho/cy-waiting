package com.kafkick.waiting.domain.admission;

/**
 * 입장 판정 결과. 사용자가 받는 응답은 통과·큐·종결 셋이지만 값을 잘게 나눈 것은
 * <b>대응이 다르기 때문</b>이다. 큐로 보낸 이유가 쿠폰 상한인지 노드 상한인지에
 * 따라 조일 대상이 달라진다.
 */
public enum AdmissionDecision {

    /** 차례가 와서 토큰을 받은 사람. 다시 세우지 않는다. */
    PASS_TOKEN,

    /** 운영자가 이 쿠폰의 대기열을 꺼뒀다. */
    PASS_BYPASS,

    /** 판정 재료가 낡았지만 줄이 비어 있다. 상한 안에서 통과시킨다. */
    PASS_FAIL_OPEN,

    /** 상한 안에서 줄도 토큰도 없이 뒷단으로. <b>한산한 쿠폰이 나가는 자리다.</b> */
    PASS_UNDER_CAP,

    /** 낡았는데 줄에 사람이 있다. 모른다는 것이 추월의 사유가 되지 않는다. */
    ENQUEUE_STALE,

    /** 운영자가 무조건 줄을 세우기로 한 쿠폰. */
    ENQUEUE_ALWAYS,

    /** 이미 붐빈다. 앞사람이 있으면 뒤에 선다. */
    ENQUEUE_BACKLOG,

    /** 그 쿠폰이 유휴 몫을 다 썼다. <b>그 쿠폰만</b> 조이면 된다. */
    ENQUEUE_RATE_COUPON,

    /** 이 노드가 초당 감당량을 다 썼다. <b>노드를 늘려야</b> 한다. */
    ENQUEUE_RATE_GLOBAL,

    /**
     * 예산은 남았는데 리미터가 키를 더 못 들고 있다. 상한 고갈과 묶으면 운영자가
     * 쿠폰이나 노드를 조인다 — 여기서 조일 것은 {@code maxKeys} 다.
     */
    ENQUEUE_KEY_SATURATED,

    /**
     * 서킷이 열렸다. <b>fallback 이 아니라 줄로 보낸다</b> — 사용자는 503 대신
     * 순번을 받고 뒷단은 완전히 쉰다. 회복 뒤 크레딧이 그 줄을 배수한다.
     */
    ENQUEUE_CIRCUIT_OPEN,

    /** 재고가 없다. Redis 도 뒷단도 치지 않고 여기서 끝낸다. */
    REJECT_SOLD_OUT,

    /** 줄 자체가 꽉 찼다. */
    REJECT_QUEUE_FULL,

    /** fail-open 상한마저 넘었다. */
    REJECT_OVERLOAD,

    /**
     * 토큰을 들고 왔지만 노드 상한을 넘었다. 큐 뒤로 안 보낸다 — 차례가 온 사람을
     * 되돌리면 허가가 "아마도" 가 된다. 짧은 재시도를 안내한다.
     */
    RETRY_TOKEN;

    /** 뒷단으로 흘려보낸다. */
    public boolean isPass() {
        return this == PASS_TOKEN
                || this == PASS_BYPASS
                || this == PASS_FAIL_OPEN
                || this == PASS_UNDER_CAP;
    }

    /** 줄을 세운다. */
    public boolean isEnqueue() {
        return this == ENQUEUE_STALE
                || this == ENQUEUE_CIRCUIT_OPEN
                || this == ENQUEUE_ALWAYS
                || this == ENQUEUE_BACKLOG
                || this == ENQUEUE_RATE_COUPON
                || this == ENQUEUE_RATE_GLOBAL
                || this == ENQUEUE_KEY_SATURATED;
    }

    /**
     * <b>낡음이 답을 바꾼 판정인가.</b> 이 셋은 낡음 자체가 조건이라, 재료가 신선했다면
     * 다른 답이 나왔다. 줄을 세우는 갈래는 낡아도 답이 같으므로 안 든다 — 표시하면
     * 스냅샷이 멎은 구간이 통째로 열화가 되고, 그 지표로는 알람을 못 건다.
     *
     * <p>값을 늘리면 <b>여기서 컴파일이 깨진다.</b> 집합을 밖에 손으로 베껴 두면
     * 사다리를 재정렬할 때 조용히 갈린다.
     */
    public boolean judgedOnStaleMaterial() {
        return switch (this) {
            case PASS_FAIL_OPEN, ENQUEUE_STALE, REJECT_OVERLOAD -> true;
            case PASS_TOKEN, PASS_BYPASS, PASS_UNDER_CAP, ENQUEUE_CIRCUIT_OPEN,
                 ENQUEUE_ALWAYS, ENQUEUE_BACKLOG, ENQUEUE_RATE_COUPON,
                 ENQUEUE_RATE_GLOBAL, ENQUEUE_KEY_SATURATED, REJECT_SOLD_OUT,
                 REJECT_QUEUE_FULL, RETRY_TOKEN -> false;
        };
    }

    /** 여기서 끝낸다. 줄도 뒷단도 없다. */
    public boolean isReject() {
        return this == REJECT_SOLD_OUT
                || this == REJECT_QUEUE_FULL
                || this == REJECT_OVERLOAD
                || this == RETRY_TOKEN;
    }
}

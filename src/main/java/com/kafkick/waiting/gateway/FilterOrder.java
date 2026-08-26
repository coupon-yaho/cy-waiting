package com.kafkick.waiting.gateway;

import org.springframework.core.Ordered;

/**
 * 필터끼리의 앞뒤를 <b>한 곳에서</b> 정한다. 각자 정하면 안 정한 것과 같아서,
 * 선언 위치를 옮기는 것만으로 순서가 바뀐다.
 */
public final class FilterOrder {

    /** 브라우저는 사전 요청에 회원 헤더를 안 붙인다. 형식 검증이 앞서면 다 막힌다. */
    public static final int CORS = Ordered.HIGHEST_PRECEDENCE + 100;

    /** 형식이 깨진 요청은 판정에 들어가기 전에 끊는다. */
    public static final int IDENTITY = CORS + 1;

    /**
     * 남용 방지. <b>판정 앞이다</b> — 판정에 들어가면 남용 요청이 노드 예산을
     * 만지고, 큐에 넣으면 공격자가 자리를 차지한다.
     */
    public static final int ABUSE = IDENTITY + 1;

    /**
     * 순번 조회. <b>라우트를 안 탄다</b> — 뒷단으로 갈 요청이 아니다.
     *
     * <p>남용 방지 뒤다. 막힐 요청이 레디스를 치게 두지 않는다.
     */
    public static final int QUEUE_STATUS = ABUSE + 1;

    private FilterOrder() {
    }
}

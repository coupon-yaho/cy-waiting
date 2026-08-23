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

    private FilterOrder() {
    }
}

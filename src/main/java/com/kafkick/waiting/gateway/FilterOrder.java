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

    /**
     * 라우트 안의 판정. <b>서킷 앞이다</b> — 뒤로 가면 서킷이 열린 동안 판정이
     * 아예 안 돌아, 이 노드가 줄을 세운 적 없는 것으로 보이고 래치가 표식을 못
     * 받는다. 그러면 다음 창의 신규 유입이 방금 줄 선 사람을 추월한다 (불변식 4).
     *
     * <p>라우트 필터는 전역 필터와 다른 사슬이라 위 값들과 겹쳐도 된다. 여기서
     * 정하는 것은 <b>라우트 안에서의 앞뒤</b>다.
     */
    public static final int ROUTE_ADMISSION = 0;

    /**
     * 라우트 안의 서킷.
     *
     * <p><b>응답을 쓰는 필터보다 안쪽이어야 한다.</b> 프레임워크의 쓰기 필터가
     * {@code -1} 이라, 그보다 앞으로 가면 폴백이 이미 커밋된 응답에 쓰려 들고
     * 읽기 전용 헤더에서 터진다 — 그 예외가 원래 실패를 덮는다.
     */
    public static final int ROUTE_CIRCUIT = ROUTE_ADMISSION + 1;

    private FilterOrder() {
    }
}

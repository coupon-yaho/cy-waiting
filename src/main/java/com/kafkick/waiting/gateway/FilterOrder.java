package com.kafkick.waiting.gateway;

import org.springframework.cloud.gateway.filter.NettyWriteResponseFilter;

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
     * <b>서킷 앞이다</b> — 뒤로 가면 서킷이 열린 동안 판정이 안 돌아 래치가 표식을
     * 못 받고, 다음 창의 신규 유입이 방금 줄 선 사람을 추월한다. 라우트 필터는
     * 전역 필터와 다른 사슬이라 위 값들과 겹쳐도 된다.
     */
    public static final int ROUTE_ADMISSION = 0;

    /**
     * <b>응답을 쓰는 필터보다 안쪽이어야 한다.</b> 프레임워크의 쓰기 필터가
     * {@code -1} 이라, 그보다 앞이면 폴백이 이미 커밋된 응답에 쓰려 들고 읽기 전용
     * 헤더에서 터진다 — 그 예외가 원래 실패를 덮는다.
     */
    public static final int ROUTE_CIRCUIT = ROUTE_ADMISSION + 1;

    /**
     * 연결이 안 된 인스턴스를 다음 대로 넘긴다. <b>서킷 안쪽이다</b> — 바깥에 두면
     * 서킷의 폴백이 하류 오류를 다 삼켜 재시도가 한 번도 안 돈다.
     */
    public static final int ROUTE_RETRY = ROUTE_CIRCUIT + 1;

    /**
     * <b>응답 본문을 보려면 여기여야 한다.</b> 쓰기 필터는 <b>자기가 받은</b>
     * exchange 에 쓰므로, 뒤에 서면 우리가 감싼 것을 아무도 안 쓰고 담는 것이 늘 빈
     * 본문이다 — 상태만 보는 검사로는 안 드러난다.
     */
    private static final int BEFORE_WRITE_RESPONSE =
            NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER - 1;

    /**
     * 뒷단의 매진 응답을 본다. 이 자리는 판정·서킷보다도 바깥이라, 관찰자가
     * {@code CLIENT_RESPONSE_ATTR} 로 정말 뒷단에 닿은 응답인지 가른다 — 안 가르면
     * 게이트웨이 자신이 낸 매진을 되먹여 뒷단이 살아나도 안 풀린다.
     */
    public static final int ROUTE_SOLD_OUT = BEFORE_WRITE_RESPONSE;

    /** 조회 라우트의 코얼레싱. 같은 이유로 쓰기 필터보다 앞이다. */
    public static final int ROUTE_COALESCING = BEFORE_WRITE_RESPONSE;

    /**
     * 본문 쓰기 상한. <b>서킷 안쪽에 못 둔다</b> — 그 자리가 쓰기 필터보다 앞이라
     * 서킷보다 바깥이고, 여기서 끊은 것은 서킷 창에 안 쌓인다. 매진 관찰보다 한 칸
     * 바깥인 것은, 반대로 두면 시한이 끊은 뒤에 관찰이 도는 순서가 되어서다.
     */
    public static final int ROUTE_BODY = BEFORE_WRITE_RESPONSE - 1;

    private FilterOrder() {
    }
}

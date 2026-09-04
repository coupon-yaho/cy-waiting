package com.kafkick.waiting.routing;

/**
 * 요청 하나가 라우팅을 지나며 남기는 속성의 이름.
 *
 * <p>재시도는 같은 요청을 다시 고르므로, 시도 사이에 남아야 하는 것은 응답이
 * 아니라 <b>요청 속성</b>에 둔다. 재시도가 되돌리는 것은 응답 쪽뿐이다.
 */
public final class RoutingAttributes {

    /** 이 요청이 이미 실패해 본 인스턴스 식별자들. 값은 {@code Set<String>} 이다. */
    public static final String TRIED = RoutingAttributes.class.getName() + ".tried";

    private RoutingAttributes() {
    }
}

package com.kafkick.waiting.gateway;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import org.springframework.cloud.gateway.filter.factory.RetryGatewayFilterFactory;

/**
 * 연결이 안 된 인스턴스를 다음 대로 넘기는 설정.
 *
 * <p><b>라우팅 설정 밖으로 꺼내 둔다.</b> 거기 정적 헬퍼로 두면 시험이 클래스
 * 이름으로만 부를 수 있고, 재시도 횟수를 설정에서 받게 될 때 호출부가 다 바뀐다.
 */
public final class ConnectRetry {

    private ConnectRetry() {
    }

    public static ConnectRetry of() {
        return new ConnectRetry();
    }

    /**
     * 연결 단계 실패에만 무는 설정. <b>따로 꺼내 둔다</b> — 필터로 감싸고 나면
     * 무엇에 무는지가 밖에서 안 보여, 상태 기반 재시도가 켜져도 시험이 못 잡는다.
     */
    public RetryGatewayFilterFactory.RetryConfig config() {
        RetryGatewayFilterFactory.RetryConfig config =
                new RetryGatewayFilterFactory.RetryConfig();
        // 인스턴스가 열 대여도 한 번이면 충분하다. 여러 번 돌면 죽은 뒷단에
        // 요청 하나가 그만큼 오래 매달려 격벽만 채운다.
        config.setRetries(1);
        config.allMethods();
        // **상태 기반 재시도를 끈다.** 기본값이 5xx 계열이라 안 비우면
        // 발급이 답을 받은 뒤에도 다시 가고, 그 한 건이 곧 초과 발급이다.
        config.setSeries();
        config.setStatuses();
        // **연결이 못 서는 갈래가 하나가 아니다.** 포트가 닫히면 거절, 라우팅이 안
        // 되면 도달 불가이고 뒤엣것은 하위 타입이 아니다. 더 넓히지는 않는다 —
        // 응답을 받기 시작한 뒤의 끊김을 다시 보내는 것이 곧 초과 발급이다.
        //
        // **이름 풀이 실패는 안 넣는다.** 연결 상한이 채널 옵션이라 리졸버에는
        // 안 걸리고, 재시도가 그 상한을 두 배로 늘려 격벽 지연을 넘긴다. 다음
        // 대도 같은 리졸버를 타므로 다시 보내도 결과가 같다.
        //
        // **열거가 완전한 것은 epoll 에서다.** NIO 로 떨어지면 경로 없음이 소켓
        // 오류로 와, 타입만으로는 응답 도중 끊김과 못 갈라 여기 못 넣는다.
        config.setExceptions(ConnectException.class, NoRouteToHostException.class);
        return config;
    }
}

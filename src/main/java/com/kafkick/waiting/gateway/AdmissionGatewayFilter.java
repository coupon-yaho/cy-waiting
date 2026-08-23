package com.kafkick.waiting.gateway;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 발급 요청을 통과·대기·거절로 가른다. <b>판정 재료는 전부 로컬 스냅샷에서 읽는다</b>
 * — 요청마다 레디스를 치면 제어 평면을 만든 이유가 사라진다.
 *
 * <p>판정 내용은 아직 없다. 라우트에 붙는 것 자체가 조용히 사라질 수 있어
 * 자리부터 세운다.
 */
public class AdmissionGatewayFilter implements GatewayFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        return chain.filter(exchange);
    }

    @Override
    public String toString() {
        return "Admission";
    }
}

package com.kafkick.waiting.gateway;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 전달 헤더를 믿어도 되는 홉의 설정.
 *
 * <p><b>비면 아무 헤더도 안 믿는다.</b> 앞단이 그 헤더를 덮어쓴다는 보장이
 * 있을 때만 채운다 — 아니면 매 요청 다른 값이 들어와 상한이 무의미해진다.
 *
 * @param cidrs 신뢰하는 대역. 못 읽는 표기는 버린다
 */
@ConfigurationProperties("waiting.proxy")
public record ProxyProperties(List<String> cidrs) {

    public TrustedProxies trusted() {
        return TrustedProxies.of(cidrs);
    }
}

package com.kafkick.waiting.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 뒷단에 실어 보내는 멱등 키의 모드.
 *
 * <p>나머지 설정과 같은 방식으로 받는다 — 바인딩이 실패하면 어떤 프로퍼티에 어떤 값이
 * 들어왔는지를 기동 로그가 말해 준다.
 */
@ConfigurationProperties(prefix = "waiting.idempotency")
public record IdempotencyProperties(IdempotencyKey.Mode mode) {

    public IdempotencyProperties {
        mode = mode == null ? IdempotencyKey.Mode.UUID : mode;
    }
}

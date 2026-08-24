package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.queue.QueueToken;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 토큰 비밀키.
 *
 * <p><b>기본값을 두지 않는다.</b> 두면 아무도 안 넣은 채로 운영에 나가고,
 * 그때 서명은 공개된 값으로 찍힌다 — 서명이 있다는 사실만 남고 뜻은 사라진다.
 *
 * @param secret 16자 이상. 짧으면 {@link QueueToken} 이 기동을 막는다
 */
@ConfigurationProperties("waiting.token")
public record QueueTokenProperties(String secret) {

    public QueueToken queueToken() {
        return QueueToken.of(secret);
    }

    /**
     * <b>기본 구현을 그대로 두지 않는다.</b> 레코드는 필드를 다 찍는다 — 바인딩
     * 실패 하나로 공개 저장소 CI 로그에 키가 남는다.
     */
    @Override
    public String toString() {
        return "QueueTokenProperties[secret=***]";
    }
}

package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.QueueToken;
import java.time.Instant;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 토큰 비밀키. <b>기본값을 두지 않는다</b> — 두면 아무도 안 넣은 채로 운영에 나가
 * 공개된 값으로 서명하고, 서명이 있다는 사실만 남고 뜻은 사라진다.
 *
 * @param secret        16자 이상. 짧으면 {@link QueueToken} 이 기동을 막는다
 * @param previous      검증에서만 받아 주는 옛 키. 롤링 배포의 창이다 (CY-902)
 * @param rolloutEndsAt <b>회전 전체가 끝나는 때</b>. 판 하나의 끝이 아니다 (AIJ-0273)
 */
@ConfigurationProperties("waiting.token")
public record QueueTokenProperties(String secret, List<String> previous,
        Instant rolloutEndsAt) {

    public QueueTokenProperties {
        // 안 넣으면 창을 안 연 것이다. null 을 그대로 두면 배선이 터진다.
        previous = previous == null ? List.of() : List.copyOf(previous);
    }

    public QueueToken queueToken() {
        return QueueToken.of(secret, previous, rolloutEndsAt);
    }

    public EntryToken entryToken() {
        return EntryToken.of(secret, previous, rolloutEndsAt);
    }

    /**
     * <b>기본 구현을 그대로 두지 않는다.</b> 레코드는 필드를 다 찍는다 — 바인딩
     * 실패 하나로 공개 저장소 CI 로그에 키가 남는다.
     */
    @Override
    public String toString() {
        return "QueueTokenProperties[secret=***, previous=%d개, rolloutEndsAt=%s]"
                .formatted(previous.size(), rolloutEndsAt);
    }
}

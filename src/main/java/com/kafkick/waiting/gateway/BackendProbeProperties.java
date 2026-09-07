package com.kafkick.waiting.gateway;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 합성 프로브의 노브.
 *
 * @param enabled  <b>기본은 꺼짐이다</b> — 뒷단이 부하를 받는 헬스 경로를 내기
 *                 전에 켜면 정적 200 을 보고 서킷이 거짓으로 닫힌다 (CY-890)
 * @param path     칠 경로. 발급 경로가 아니라 줄 밖이다
 * @param interval 회차 간격. 반쯤 열린 표본을 이 속도로 채운다
 */
@ConfigurationProperties("waiting.backend.probe")
public record BackendProbeProperties(boolean enabled, String path, Duration interval) {

    public BackendProbeProperties {
        path = path == null || path.isBlank() ? "/actuator/health" : path.trim();
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("probe path 는 / 로 시작해야 한다: " + path);
        }
        interval = interval == null ? Duration.ofSeconds(1) : interval;
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("probe interval 은 양수여야 한다: " + interval);
        }
        // **대기 시간보다 성기면 안 된다.** 열린 뒤 허가가 나는 순간을 놓치면
        // 그다음 회차까지 회복이 통째로 밀린다. 상한은 판정 쪽 폴링과 같은 자리다.
        if (interval.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("probe interval 은 1분 이하여야 한다: " + interval);
        }
    }
}

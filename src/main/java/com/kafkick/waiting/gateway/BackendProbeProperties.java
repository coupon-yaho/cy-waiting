package com.kafkick.waiting.gateway;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 합성 프로브의 노브.
 *
 * @param enabled  <b>기본은 꺼짐이다</b> — 뒷단이 부하를 받는 헬스 경로를 내기
 *                 전에 켜면 정적 200 을 보고 서킷이 거짓으로 닫힌다 (CY-890)
 * @param path     칠 경로. <b>기본값이 없다</b> — 정적 200 을 주는 경로가 기본으로
 *                 실리면 켜는 데 한 줄이면 되고 그것이 거짓 회복을 만든다
 * @param interval 회차 간격. 반쯤 열린 표본을 이 속도로 채운다
 */
@ConfigurationProperties("waiting.backend.probe")
public record BackendProbeProperties(boolean enabled, String path, Duration interval) {

    /** 서킷이 열린 뒤 반쯤 열리기까지의 대기(5초) 아래여야 그 순간을 안 놓친다. */
    private static final Duration MAX_INTERVAL = Duration.ofSeconds(5);

    public BackendProbeProperties {
        // **기본 경로를 안 준다.** 정적 200 을 주는 경로를 기본으로 실으면 켜는 데
        // 한 줄이면 되고, 그 한 줄이 계획서가 적어 둔 거짓 회복을 그대로 만든다.
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException(
                    "프로브를 켜려면 probe path 를 적어야 한다 — 부하를 받는 경로여야 한다");
        }
        path = path.trim();
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("probe path 는 / 로 시작해야 한다: " + path);
        }
        interval = interval == null ? Duration.ofSeconds(1) : interval;
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("probe interval 은 양수여야 한다: " + interval);
        }
        // **열린 구간의 대기 시간보다 성기면 안 된다.** 반쯤 열린 순간을 놓치면
        // 그다음 회차까지 회복이 통째로 밀린다. 그 값이 5초라 여기를 그 아래로 묶는다.
        if (interval.compareTo(MAX_INTERVAL) > 0) {
            throw new IllegalArgumentException(
                    "probe interval 은 " + MAX_INTERVAL + " 이하여야 한다: " + interval);
        }
    }
}

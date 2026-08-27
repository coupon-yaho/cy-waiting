package com.kafkick.waiting.gateway;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 조회를 모을 경로와 수명.
 *
 * <p><b>화이트리스트로만 켭니다.</b> 기본이 켜짐이면 개인화된 응답이 붙는 순간
 * 남의 응답을 받습니다 — 그 사고는 되돌릴 수 없습니다.
 *
 * @param enabled 통째로 끄는 스위치. 장애 중에 되돌릴 수단이다
 * @param maxBodyBytes 이보다 큰 응답은 안 모은다. 보호 장치가 메모리 사고의 원인이
 *                     되면 안 된다
 * @param maxKeys 담을 수 있는 키 수. 경로와 쿼리로 만드는 값이라 상한이 필요하다
 * @param routes 모을 경로. 여기 없는 경로는 그대로 프록시한다
 */
@ConfigurationProperties("waiting.coalescing")
public record CoalescingProperties(boolean enabled, int maxBodyBytes, int maxKeys,
        List<Route> routes) {

    /**
     * @param path 정확히 이 경로. 패턴이 아니라 문자열이다 — 패턴을 넓게 적으면
     *             모을 생각이 없던 경로가 딸려 들어온다
     * @param ttl 이만큼은 다시 안 묻는다. 판정이 최대 1.5초 낡은 재료를 쓰므로
     *            수백 ms 는 그보다 훨씬 신선하다
     */
    public record Route(String path, Duration ttl) {

        public Route {
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException("path 는 필수다");
            }
            if (ttl == null || ttl.isNegative() || ttl.toMillis() < 1) {
                throw new IllegalArgumentException("ttl 은 1ms 이상이어야 한다: " + ttl);
            }
        }
    }

    public CoalescingProperties {
        routes = routes == null ? List.of() : List.copyOf(routes);
        if (enabled) {
            if (maxBodyBytes < 1) {
                throw new IllegalArgumentException(
                        "maxBodyBytes 는 1 이상이어야 한다: " + maxBodyBytes);
            }
            if (maxKeys < 1) {
                throw new IllegalArgumentException("maxKeys 는 1 이상이어야 한다: " + maxKeys);
            }
            if (routes.isEmpty()) {
                // 켰는데 경로가 없으면 아무것도 안 모은다. 켰다고 믿는 상태가 가장 나쁘다.
                throw new IllegalArgumentException("모을 경로가 없는데 켜져 있다");
            }
        }
    }

    /** 경로별 수명. 없는 경로는 안 모은다. */
    public Map<String, Duration> ttlByPath() {
        return routes.stream().collect(Collectors.toUnmodifiableMap(
                Route::path, Route::ttl, (a, b) -> {
                    throw new IllegalArgumentException("같은 경로를 두 번 적었다");
                }));
    }

    /** 켜져 있고 이 경로가 목록에 있는가. */
    public boolean covers(String path) {
        return enabled && ttlByPath().containsKey(Objects.requireNonNull(path, "path"));
    }
}

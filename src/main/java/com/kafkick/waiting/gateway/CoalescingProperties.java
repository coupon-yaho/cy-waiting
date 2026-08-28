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
 * 남의 응답을 받습니다.
 *
 * @param enabled 통째로 끄는 스위치. 장애 중에 되돌릴 수단이다
 * @param maxBodyBytes 이보다 큰 응답은 안 모은다
 * @param maxCacheBytes 담아 둘 수 있는 전체 바이트. <b>키 수로만 막으면 유계가
 *                      아니다</b> — 상한이 그대로 OOM 의 근거가 된다
 * @param maxKeys 동시에 모을 수 있는 키 수
 * @param routes 모을 경로. 여기 없는 경로는 그대로 프록시한다
 */
@ConfigurationProperties("waiting.coalescing")
public record CoalescingProperties(boolean enabled, int maxBodyBytes,
        long maxCacheBytes, int maxKeys, List<Route> routes) {

    /**
     * @param path 정확히 이 경로. 패턴을 넓게 적으면 모을 생각이 없던 경로가
     *             딸려 들어온다
     * @param ttl 이만큼은 다시 안 묻는다. 판정이 최대 1.5초 낡은 재료를 쓴다
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
            if (maxCacheBytes < maxBodyBytes) {
                // 한 건도 못 담는 예산은 켜 둔 것이 아니다.
                throw new IllegalArgumentException(
                        "maxCacheBytes 는 maxBodyBytes 이상이어야 한다: " + maxCacheBytes);
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

    /**
     * 경로별 수명. 없는 경로는 안 모읍니다.
     *
     * <p><b>부르는 쪽이 한 번만 부릅니다.</b> 요청마다 만들면 조회 한 건마다 맵을
     * 새로 짓는 셈이고, 모으기로 던 뒷단 부하가 할당과 GC 로 돌아옵니다.
     */
    public Map<String, Duration> ttlByPath() {
        return routes.stream().collect(Collectors.toUnmodifiableMap(
                Route::path, Route::ttl, (a, b) -> {
                    throw new IllegalArgumentException("같은 경로를 두 번 적었다");
                }));
    }
}

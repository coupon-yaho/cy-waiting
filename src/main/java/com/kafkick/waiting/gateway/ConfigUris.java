package com.kafkick.waiting.gateway;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * <b>비었는지만 보면 검증이 아니다.</b> 스킴이 빠진 값은 그대로 뜨고 모든 프록시가
 * 실패하거나, 아무 오리진과도 안 맞아 프론트가 막힌다 — 둘 다 기동은 성공한다.
 *
 * <p>메시지에 값을 안 싣는다. 기동 실패 로그에 자격 증명이나 아직 안 알려진
 * 내부 호스트명이 그대로 흘러간다.
 */
final class ConfigUris {

    private ConfigUris() {
    }

    /** 상태가 없지만 인스턴스다 — 검증이 늘면 여기 필드가 생긴다 (JS-13). */
    static ConfigUris create() {
        return new ConfigUris();
    }

    /** 프록시가 스킴·호스트·포트만 가져가므로 나머지가 붙으면 조용히 버려진다. */
    void backend(String value) {
        URI uri = parsed(value, "waiting.backend.uri");
        httpScheme(uri, "waiting.backend.uri");
        if (hasExtra(uri)) {
            throw new IllegalArgumentException(
                    "waiting.backend.uri 에 경로·질의·조각·사용자 정보를 붙일 수 없다");
        }
    }

    private boolean hasExtra(URI uri) {
        return (uri.getRawPath() != null && !uri.getRawPath().isEmpty())
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null
                || uri.getRawUserInfo() != null;
    }

    /**
     * 브라우저가 보내는 {@code Origin} 은 스킴·호스트·기본이 아닌 포트뿐이다.
     * 나머지가 붙은 값은 기동에 성공하고 아무와도 안 맞는다.
     */
    void origin(String value) {
        // "null" 은 와일드카드 검사를 지나면서 샌드박스 프레임에 그대로 맞는다.
        if (value != null && "null".equalsIgnoreCase(value.trim())) {
            throw new IllegalArgumentException("오리진에 쓸 수 없는 값이 있다");
        }
        if (value != null && value.contains("*")) {
            throw new IllegalArgumentException("오리진에 와일드카드를 쓸 수 없다");
        }
        URI uri = parsed(value, "오리진");
        httpScheme(uri, "오리진");
        if (hasExtra(uri)) {
            throw new IllegalArgumentException("오리진에 경로·질의·조각·사용자 정보를 붙일 수 없다");
        }
        // 기본 포트를 적으면 브라우저가 보내는 값과 안 맞는다.
        int defaultPort = "https".equals(uri.getScheme()) ? 443 : 80;
        if (uri.getPort() == defaultPort) {
            throw new IllegalArgumentException("오리진에 기본 포트를 적으면 브라우저 값과 안 맞는다");
        }
    }

    private URI parsed(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(what + " 가 비어 있다");
        }
        try {
            return new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(what + " 를 못 읽었다", e);
        }
    }

    private void httpScheme(URI uri, String what) {
        if (!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme())
                || uri.getHost() == null) {
            throw new IllegalArgumentException(what + " 는 http 나 https 로 시작하는 주소여야 한다");
        }
    }
}

package com.kafkick.waiting.gateway;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 입장 토큰을 어디로 내리고, 뒷단이 어느 이름으로 받는가.
 *
 * <p>내리는 자리와 이름이 코드에 박혀 있었다. 계약이 다른 곳에 붙이려면 둘 다 골라야
 * 한다. 다만 이름은 그 값이 헤더 줄이 되므로 아무 문자나 받지 않는다.
 */
@ConfigurationProperties(prefix = "waiting.entry-token")
public record EntryTokenDelivery(Where where, String header, String backendHeader) {

    /** 응답의 어디에 싣는가. */
    public enum Where {
        /** 응답 바디에만. 지금 동작이다. */
        BODY,
        /** 응답 헤더에만. */
        HEADER,
        /** 둘 다. 옮겨 가는 동안 쓴다. */
        BOTH
    }

    static final String DEFAULT_HEADER = "Entry-Token";

    /**
     * HTTP 토큰 문자만 받는다 (RFC 9110). <b>공백이나 줄바꿈이 들어가면 헤더가
     * 갈린다</b> — 그 값이 그대로 헤더 줄이 된다.
     */
    private static final Pattern TOKEN = Pattern.compile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$");

    /**
     * 게이트웨이가 스스로 쓰거나 믿는 이름. <b>여기로 고르면 클라이언트 값이 그것을 덮는다</b>
     * — 신원·멱등 키·IP·전송 틀·CORS 헤더가 설정 한 줄로 위조된다.
     */
    private static final Set<String> RESERVED = Set.of("x-member-id", "x-member-grade",
            "idempotency-key", "queue-token", "x-request-id", "authorization", "host",
            "content-length", "transfer-encoding", "connection", "cookie", "set-cookie",
            "forwarded", "x-real-ip", "true-client-ip", "cf-connecting-ip", "origin",
            "content-type", "cache-control", "retry-after", "vary");

    /** 이름 묶음. 끝까지 적으면 새 이름 하나가 빠진다. */
    private static final List<String> RESERVED_PREFIXES =
            List.of("x-forwarded-", "access-control-");

    public EntryTokenDelivery {
        where = where == null ? Where.BODY : where;
        header = name(header, DEFAULT_HEADER);
        // 안 적으면 받은 이름 그대로 나간다. 게이트웨이는 이 헤더를 안 벗긴다.
        backendHeader = name(backendHeader, header);
    }

    /** 받는 이름과 뒷단 이름이 다르면 넘길 때 바꿔 실어야 한다. */
    public boolean renames() {
        return !header.equals(backendHeader);
    }

    /** 응답 헤더로 내리면 노출 목록에 올려야 한다. 안 올리면 브라우저가 못 읽는다. */
    public List<String> exposed() {
        return where == Where.BODY ? List.of() : List.of(header);
    }

    /** 압축 생성자가 부르므로 정적이어야 한다. 패키지 밖에서는 안 쓴다. */
    static String name(String value, String fallback) {
        // **빈 값은 안 적은 것이다.** 이 저장소의 설정은 환경변수 기본값을 비워
        // 두는 모양이라(`${VAR:}`), 비었다고 막으면 아무도 안 넣은 배포가 안 뜬다.
        if (value == null || value.isBlank()) {
            return fallback;
        }
        if (!TOKEN.matcher(value).matches()) {
            throw new IllegalArgumentException("헤더 이름에 쓸 수 없는 문자가 있다: " + value);
        }
        String lower = value.toLowerCase(Locale.ROOT);
        if (RESERVED.contains(lower) || RESERVED_PREFIXES.stream().anyMatch(lower::startsWith)) {
            throw new IllegalArgumentException(
                    "게이트웨이가 쓰는 헤더라 입장 토큰 이름으로 못 쓴다: " + value);
        }
        return value;
    }
}

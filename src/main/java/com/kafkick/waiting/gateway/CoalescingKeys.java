package com.kafkick.waiting.gateway;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.http.HttpHeaders;
import org.springframework.web.server.ServerWebExchange;

/**
 * 같은 응답을 받아도 되는 요청인지를 정합니다.
 *
 * <p>담을 때 쓴 키와 찾을 때 쓴 키가 같은 규칙이어야 합니다. 그 불변식이 필터에
 * 흩어져 있을 때 실제로 틈이 생겼습니다.
 */
final class CoalescingKeys {

    /** 전부 갈린다는 뜻. 이건 키로 못 만든다. */
    static final String ALL = "*";

    /** 경로별로 <b>뒷단이 갈린다고 말한 헤더</b>. 응답을 받아 봐야 압니다. */
    private final Map<String, List<String>> varyByPath = new ConcurrentHashMap<>();

    private CoalescingKeys() {
    }

    static CoalescingKeys create() {
        return new CoalescingKeys();
    }

    /**
     * 키와 그 키를 만들 때 쓴 규칙.
     *
     * @param value 캐시와 모으기가 쓰는 키
     * @param vary 만들 때 알고 있던 갈림 헤더. 응답이 다른 것을 말하면 이 키로
     *             모인 사람들은 <b>같은 응답을 받으면 안 된다</b>
     */
    record Key(String value, List<String> vary) {
    }

    /**
     * 경로 · 쿼리 · 갈림 헤더의 값으로 만듭니다.
     *
     * <p>뒷단이 어떤 헤더로 갈린다고 하면 그 값도 키에 들어가야 합니다 — 안 넣으면
     * 그 값이 다른 사람이 같은 응답을 받습니다.
     */
    Key of(ServerWebExchange exchange, String path) {
        StringBuilder key = new StringBuilder();
        // **길이를 앞에 붙인다.** 구분자만으로 이으면 조각 안에 그 구분자를 넣어
        // 경계를 옮길 수 있다 — 쿼리에 `|X-Member-Id=` 를 심으면 남의 키가 된다.
        part(key, path);
        part(key, exchange.getRequest().getURI().getRawQuery());
        HttpHeaders headers = exchange.getRequest().getHeaders();
        List<String> vary = varyByPath.getOrDefault(path, List.of());
        for (String name : vary) {
            part(key, name);
            part(key, String.join(",", headers.getOrEmpty(name)));
        }
        return new Key(key.toString(), vary);
    }

    private void part(StringBuilder key, String value) {
        String text = value == null ? "" : value;
        key.append(text.length()).append(':').append(text);
    }

    /**
     * 뒷단이 말한 것을 적어 둡니다. 다음 요청부터 키에 들어갑니다.
     *
     * @return 이번 응답이 말한 갈림 헤더
     */
    // 배포로 Vary 가 바뀌면 키 스킴이 통째로 갈린다. 값이 바뀔 때만 남긴다.
    List<String> learn(String path, HttpHeaders headers) {
        List<String> vary = headers.getOrEmpty(HttpHeaders.VARY).stream()
                .flatMap(line -> Arrays.stream(line.split(",")))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .toList();
        if (!vary.isEmpty()) {
            varyByPath.put(path, vary);
        }
        return vary;
    }

    /**
     * 이 응답을 <b>이 키로 모인 사람들에게 나눠 줘도 되는가.</b>
     *
     * <p>배우기 전의 첫 무리가 가장 위험합니다 — 갈림 헤더를 몰라 회원이 서로
     * 다른 요청들이 한 키에 붙어 있습니다.
     */
    boolean shareable(Key key, List<String> learned) {
        return !learned.contains(ALL) && learned.equals(key.vary());
    }
}

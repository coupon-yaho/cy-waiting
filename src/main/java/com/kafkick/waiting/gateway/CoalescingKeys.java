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
 * <p><b>담을 때 쓴 키와 찾을 때 쓴 키가 같은 규칙이어야 합니다.</b> 그 불변식이
 * 필터에 흩어져 있을 때 실제로 틈이 생겼습니다 — 아래 {@code learned} 가 그 자리입니다.
 */
final class CoalescingKeys {

    /** 전부 갈린다는 뜻. 이건 키로 못 만든다. */
    static final String ALL = "*";

    /**
     * 경로별로 <b>뒷단이 갈린다고 말한 헤더</b>. 응답을 받아 봐야 압니다.
     */
    private final Map<String, List<String>> varyByPath = new ConcurrentHashMap<>();

    private CoalescingKeys() {
    }

    static CoalescingKeys create() {
        return new CoalescingKeys();
    }

    /**
     * 키와 <b>그 키를 만들 때 쓴 규칙</b>.
     *
     * @param value 캐시와 모으기가 쓰는 키
     * @param vary 이 키를 만들 때 알고 있던 갈림 헤더. 응답이 다른 것을 말하면
     *             이 키로 모인 사람들은 <b>같은 응답을 받으면 안 된다</b>
     */
    record Key(String value, List<String> vary) {
    }

    /**
     * 경로 · 쿼리 · 갈림 헤더의 값으로 만듭니다.
     *
     * <p>쿼리가 다르면 다른 응답입니다. 뒷단이 어떤 헤더로 갈린다고 하면 그 값도
     * 키에 들어가야 합니다 — 안 넣으면 그 값이 다른 사람이 같은 응답을 받습니다.
     */
    Key of(ServerWebExchange exchange, String path) {
        String query = exchange.getRequest().getURI().getRawQuery();
        StringBuilder key = new StringBuilder(path);
        if (query != null) {
            key.append('?').append(query);
        }
        HttpHeaders headers = exchange.getRequest().getHeaders();
        List<String> vary = varyByPath.getOrDefault(path, List.of());
        for (String name : vary) {
            key.append('|').append(name).append('=')
                    .append(String.join(",", headers.getOrEmpty(name)));
        }
        return new Key(key.toString(), vary);
    }

    /**
     * 뒷단이 말한 것을 적어 둡니다. 다음 요청부터 키에 들어갑니다.
     *
     * @return 이번 응답이 말한 갈림 헤더
     */
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
     * <p><b>배우기 전의 첫 무리가 가장 위험합니다.</b> 그때는 갈림 헤더를 몰라
     * 회원이 서로 다른 요청들이 한 키에 붙어 있습니다. 응답이 "이 헤더로 갈린다"
     * 고 말하는 순간, 그 무리는 남의 응답을 받게 됩니다.
     */
    boolean shareable(Key key, List<String> learned) {
        return !learned.contains(ALL) && learned.equals(key.vary());
    }
}

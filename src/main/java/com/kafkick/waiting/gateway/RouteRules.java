package com.kafkick.waiting.gateway;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.HttpMethod;

/**
 * 어느 경로를 어디로 보내는가. 규칙을 여러 개 적을 수 있다.
 *
 * <p>적는 것은 경로와 메서드와 주소와 <b>성질</b>뿐이다. 필터 이름은 안 받는다 — 이름으로
 * 적으면 안 풀려도 기동이 성공하고 판정만 사라진다.
 */
@ConfigurationProperties(prefix = "waiting.routes")
public record RouteRules(List<Rule> rules) {

    /** 설정이 적는 것은 자리뿐이다. 제약은 코드가 준다 — 이 값이 레디스 키가 된다. */
    static final String ID_SLOT = "{couponId}";

    static final String ID_PATTERN = "{couponId:[A-Za-z0-9_-]{1,64}}";

    /** 라우트 경로가 있어야 할 곳. 신원 검사·남용 제한·교차 출처가 이 아래에만 걸린다. */
    static final String API_PREFIX = "/api/";

    /**
     * 줄 조회 경로. <b>아직 설정으로 못 바꾼다</b> — 이 경로는 라우트를 안 타서
     * 라우트 밖 필터 둘이 제 패턴으로 직접 맞춘다.
     */
    public static final String QUEUE_PATH = "/api/v1/coupons/" + ID_SLOT + "/queue";

    /** 규칙의 성질. 무엇이 붙는지는 이것이 정한다. */
    public enum Kind {
        /** 부작용이 있는 진입. 판정·서킷·매진 관찰이 붙고 재시도는 연결 단계뿐이다. */
        ENTRY,
        /** 멱등한 조회. 모으기가 붙고 재시도가 자유롭다. */
        QUERY,
        /** 줄 조회. 게이트웨이가 종결하므로 뒷단으로 안 간다. */
        QUEUE
    }

    /** 부작용이 없는 메서드. 진입 규칙은 이것으로 적을 수 없다. */
    private static final Set<HttpMethod> SAFE_METHODS =
            Set.of(HttpMethod.GET, HttpMethod.HEAD);

    private static final List<Rule> DEFAULTS = List.of(
            new Rule("issue", Kind.ENTRY, "POST",
                    List.of("/api/v1/coupons/" + ID_SLOT + "/issue"), null),
            new Rule("coupons", Kind.QUERY, "GET",
                    List.of("/api/v1/coupons", "/api/v1/coupons/" + ID_SLOT), null),
            new Rule("queue", Kind.QUEUE, "GET", List.of(QUEUE_PATH), null));

    public RouteRules {
        if (rules == null) {
            rules = DEFAULTS;
        }
        if (rules.isEmpty()) {
            throw new IllegalArgumentException("라우팅 규칙이 하나도 없다 — 아무 경로도 안 잡힌다");
        }
        Set<String> seen = new HashSet<>();
        for (Rule rule : rules) {
            // **이름이 곧 서킷 이름이고 라우트 id 다.** 겹치면 한쪽이 조용히 덮인다.
            if (!seen.add(rule.id())) {
                throw new IllegalArgumentException("라우팅 규칙 이름이 겹친다: " + rule.id());
            }
        }
        Set<String> paths = new HashSet<>();
        for (Rule rule : rules) {
            // **바꿀 수 있는 척하지 않는다.** 이 경로는 라우트를 안 타므로 규칙을
            // 고쳐도 필터가 안 따라온다 — 기동은 성공하고 그 경로만 뒷단으로 샌다.
            if (rule.kind() == Kind.QUEUE && !rule.paths().equals(List.of(QUEUE_PATH))) {
                throw new IllegalArgumentException(
                        "줄 조회 경로는 아직 설정으로 못 바꾼다: " + QUEUE_PATH + " 여야 한다");
            }
            // **라우트 순서는 선언 순이고 전부 같은 우선순위다.** 경로가 겹치면 앞이
            // 이기고 뒤는 한 건도 안 받는데, 기동에도 지표에도 안 나온다.
            for (String path : rule.paths()) {
                if (!paths.add(rule.method() + " " + path)) {
                    throw new IllegalArgumentException(
                            "라우팅 경로가 겹친다: " + rule.method() + " " + path);
                }
            }
        }
        rules = List.copyOf(rules);
    }

    /** 뒷단으로 가는 규칙만. 줄 조회는 게이트웨이가 끝낸다. */
    public List<Rule> forwarded() {
        return rules.stream().filter(r -> r.kind() != Kind.QUEUE).toList();
    }

    /**
     * 경로와 성질을 한 쌍으로 적는다. 주소를 비우면 공통 뒷단으로 간다.
     *
     * @param method {@link HttpMethod} 이름
     */
    public record Rule(String id, Kind kind, String method, List<String> paths, String uri) {

        public Rule {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("라우팅 규칙에 이름이 없다");
            }
            if (kind == null) {
                throw new IllegalArgumentException("라우팅 규칙 '" + id + "' 에 성질이 없다");
            }
            if (paths == null || paths.isEmpty()) {
                throw new IllegalArgumentException("라우팅 규칙 '" + id + "' 에 경로가 없다");
            }
            resolveMethod(id, method);
            // **뒷단으로 가는 길이 두 갈래면 안 된다.** 줄 조회는 게이트웨이가 끝내므로
            // 주소를 받으면 그 값이 아무 데도 안 쓰이고, 적은 사람은 쓰인다고 믿는다.
            if (kind == Kind.QUEUE && uri != null) {
                throw new IllegalArgumentException(
                        "줄 조회 규칙 '" + id + "' 은 뒷단으로 안 간다 — 주소를 받지 않는다");
            }
            if (uri != null) {
                try {
                    ConfigUris.create().backend(uri);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            "라우팅 규칙 '" + id + "' 의 주소가 뒷단 형식이 아니다: " + uri, e);
                }
            }
            // **성질과 메서드가 어긋나면 안 된다.** 발급 경로를 조회로 적으면 판정도
            // 매진 관찰도 안 붙어 뒷단으로 그대로 가고, 기동은 초록이다. 반대로
            // 조회를 진입으로 적으면 한산한 쿠폰의 조회가 줄로 간다.
            boolean safe = SAFE_METHODS.contains(resolveMethod(id, method));
            if (kind == Kind.ENTRY && safe) {
                throw new IllegalArgumentException(
                        "진입 규칙 '" + id + "' 이 안전한 메서드다: " + method);
            }
            if (kind != Kind.ENTRY && !safe) {
                throw new IllegalArgumentException(
                        "조회 규칙 '" + id + "' 이 부작용 있는 메서드다: " + method);
            }
            // **자리를 경로마다 본다.** 목록 중 하나만 맞으면 통과하던 때는, 자리를
            // 맞춘 미끼 경로 하나를 두고 다른 경로로 제약을 넓힐 수 있었다.
            for (String path : paths) {
                if (!path.startsWith(API_PREFIX)) {
                    throw new IllegalArgumentException(
                            "라우팅 규칙 '" + id + "' 의 경로가 " + API_PREFIX + " 밖이다: " + path);
                }
                int brace = path.indexOf('{');
                if (brace >= 0 && !path.startsWith(ID_SLOT, brace)) {
                    throw new IllegalArgumentException(
                            "라우팅 규칙 '" + id + "' 의 경로가 식별자 자리를 고쳐 적었다: "
                                    + path + " — " + ID_SLOT + " 만 쓴다");
                }
                // 판정과 줄 조회는 식별자를 경로에서 꺼낸다. 자리가 없으면 기동은
                // 성공하고 그 경로만 400 을 낸다.
                if (kind != Kind.QUERY && brace < 0) {
                    throw new IllegalArgumentException(
                            "라우팅 규칙 '" + id + "' 의 경로에 " + ID_SLOT + " 자리가 없다: "
                                    + path);
                }
            }
            paths = List.copyOf(paths);
        }

        public HttpMethod resolvedMethod() {
            return resolveMethod(id, method);
        }

        /** 식별자 자리에 제약을 박아 돌려준다. 설정은 그 정규식을 못 넓힌다. */
        public List<String> expandedPaths() {
            List<String> out = new ArrayList<>(paths.size());
            for (String path : paths) {
                out.add(path.replace(ID_SLOT, ID_PATTERN));
            }
            return List.copyOf(out);
        }

    }

    /** `HttpMethod.valueOf` 는 모르는 이름도 새로 만들어 낸다. 목록에 있는 것만 받는다. */
    static HttpMethod resolveMethod(String id, String method) {
        if (method == null || method.isBlank()) {
            throw new IllegalArgumentException("라우팅 규칙 '" + id + "' 에 메서드가 없다");
        }
        HttpMethod resolved = HttpMethod.valueOf(method.trim().toUpperCase(Locale.ROOT));
        if (Arrays.stream(HttpMethod.values()).noneMatch(resolved::equals)) {
            throw new IllegalArgumentException(
                    "라우팅 규칙 '" + id + "' 의 메서드를 모른다: " + method);
        }
        return resolved;
    }
}

package com.kafkick.waiting.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;
import com.kafkick.waiting.routing.RoutingProperties;
import org.springframework.cloud.gateway.filter.factory.RetryGatewayFilterFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import com.kafkick.waiting.control.HealthConfig;
import java.util.function.IntSupplier;
import org.springframework.cloud.gateway.filter.factory.SpringCloudCircuitBreakerFilterFactory;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.GatewayFilterSpec;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.springframework.http.HttpMethod;
import org.springframework.web.server.ServerWebExchange;

/**
 * 프록시할 경로를 <b>명시적으로만</b> 적는다. 설정에 이름으로 적으면 필터가
 * 안 풀려도 기동이 성공하고 판정만 사라진다 — 인스턴스를 직접 붙여 그 실패를 없앤다.
 */
@Configuration
@EnableConfigurationProperties({GatewayRoutes.Backend.class, RouteRules.class})
public class GatewayRoutes {

    /** 연결 단계에만 무는 재시도 설정. 무엇에 무는지가 밖에서 보여야 한다. */
    private final ConnectRetry connectRetryPolicy = ConnectRetry.singleAttempt();

    /** 프레임워크가 라우트별 응답 상한을 읽는 키. 이름을 틀리면 조용히 안 걸린다. */
    private static final String RESPONSE_TIMEOUT_ATTR = "response-timeout";

    /**
     * 라우트별 연결 상한을 읽는 키. <b>안 걸면 30초가 선다</b> — 죽은 인스턴스의
     * 주소는 거절도 답도 없어 그동안 요청이 매달리고, 연결이 실패하지 않으니
     * 연결 실패 재시도가 걸릴 자리도 없다.
     */
    private static final String CONNECT_TIMEOUT_ATTR = "connect-timeout";

    /**
     * 서킷이 열렸을 때 넘길 주소. <b>받는 주소와 같은 상수에서 나온다</b> — 갈리면
     * 기동은 되고 장애 때만 404 가 드러나며, 404 는 매진으로 읽혀 다시 오지 않는다.
     */
    public static final String FALLBACK_URI = "forward:" + BackendFallbackRoutes.FALLBACK_ISSUE;

    /** 서킷이 실패로 셀 뒷단 응답. 계열로는 못 적어 하나씩 적는다. */
    private static final Set<String> SERVER_FAILURES = Set.of("500", "502", "503", "504");

    /**
     * 서킷의 이름. 지금은 뒷단 주소가 하나라 하나뿐이다. 가용량 기반 분배가 붙으면
     * <b>인스턴스마다 따로 잡는다</b> — 하나로 묶으면 한 대가 죽어도 전부 막힌다.
     */
    public static final String CIRCUIT = "backend";

    /**
     * 관례로 쓰이는 클라이언트 IP 헤더. 프레임워크는 {@code X-Forwarded-*} 만
     * 지우므로 이것들은 그대로 넘어가고, 뒷단이 하나라도 믿으면 IP 단위 제한이
     * 헤더 한 줄로 우회된다.
     */
    private static final String[] SPOOFABLE_CLIENT_IP =
            {"X-Real-IP", "X-Client-IP", "True-Client-IP", "CF-Connecting-IP"};

    private GatewayFilterSpec stripSpoofableClientIp(GatewayFilterSpec spec) {
        GatewayFilterSpec stripped = spec;
        for (String header : SPOOFABLE_CLIENT_IP) {
            stripped = stripped.removeRequestHeader(header);
        }
        return stripped;
    }

    /**
     * 원본 경로도 같이 본다. 술어는 세그먼트를 디코딩하고 매트릭스 파라미터를
     * 떼어 낸 값으로 맞추는데 전달은 원본을 보낸다 — 갈리면 판정한 쿠폰과 뒷단이
     * 받는 쿠폰이 달라진다.
     */
    private Predicate<ServerWebExchange> rawPathIsPlain() {
        return exchange -> {
            String raw = exchange.getRequest().getURI().getRawPath();
            return raw.indexOf('%') < 0 && raw.indexOf(';') < 0;
        };
    }

    /**
     * 뒷단 쿠폰 서비스.
     *
     * @param connectTimeout 연결을 이만큼만 기다린다. <b>안 정하면 30초다</b>
     */
    @ConfigurationProperties("waiting.backend")
    public record Backend(String uri, Duration responseTimeout, Duration connectTimeout) {

        // 검증은 밖에 둔다. 압축 생성자에서 부를 수 있는 것은 정적뿐이라,
        // 안에 두면 그 자리에서만 쓰이는 정적 메서드가 생긴다.
        public Backend {
            ConfigUris.create().backend(uri);
            if (responseTimeout == null || responseTimeout.toMillis() < 1) {
                throw new IllegalArgumentException(
                        "responseTimeout 은 1ms 이상이어야 한다: " + responseTimeout);
            }
            // **격벽 시한보다 앞이어야 한다.** 뒤에 있으면 격벽이 먼저 끊고, 그때
            // 서킷에 가는 것은 취소라 창에 안 쌓인다. 시험이 아니라 여기서 막는 것은
            // 배포 설정 한 줄이 이 순서를 뒤집기 때문이다.
            if (responseTimeout.compareTo(AdmissionGatewayFilter.MAX_IN_FLIGHT) >= 0) {
                throw new IllegalArgumentException(
                        "responseTimeout 은 격벽 시한(" + AdmissionGatewayFilter.MAX_IN_FLIGHT
                                + ") 보다 짧아야 한다: " + responseTimeout);
            }
            if (connectTimeout == null || connectTimeout.toMillis() < 1) {
                throw new IllegalArgumentException(
                        "connectTimeout 은 1ms 이상이어야 한다: " + connectTimeout);
            }
            // **응답 상한보다 짧아야 한다.** 길면 응답 상한이 먼저 끊어 연결
            // 실패가 영영 안 드러나고, 그 요청은 재시도 없이 그대로 실패한다 —
            // 연결 상한을 둔 이유가 사라진다.
            if (connectTimeout.compareTo(responseTimeout) >= 0) {
                throw new IllegalArgumentException(
                        "connectTimeout 은 responseTimeout(" + responseTimeout
                                + ") 보다 짧아야 한다: " + connectTimeout);
            }
        }
    }

    /**
     * 서킷 필터를 손으로 만든다. {@code circuitBreaker(...)} 는 order 를 줄 자리가
     * 없어 0 으로 붙고, 판정도 0 이면 둘의 앞뒤가 안정 정렬에만 기대게 된다.
     */
    private GatewayFilter circuit(SpringCloudCircuitBreakerFilterFactory breakers,
            String routeId) {
        SpringCloudCircuitBreakerFilterFactory.Config config =
                new SpringCloudCircuitBreakerFilterFactory.Config();
        config.setName(CIRCUIT);
        config.setFallbackUri(FALLBACK_URI);
        // **라우트 id 를 받는다.** 규칙이 여럿이면 이 값이 규칙마다 달라야 한다 —
        // 박아 두면 다른 규칙의 폴백이 엉뚱한 라우트 이름으로 돈다.
        config.setRouteId(routeId);
        // **응답이 오면 서킷은 성공으로 센다.** 상태를 실패로 바꾸지 않으면 뒷단이
        // 전부 500 을 내도 안 열린다. 501·505 는 뒷단이 아프다는 뜻이 아니라 뺀다.
        config.setStatusCodes(SERVER_FAILURES);
        return breakers.apply(config);
    }

    /**
     * 연결이 안 된 인스턴스를 다음 대로 넘긴다. <b>연결 단계 실패에만 건다</b> —
     * 발급은 멱등이 아니라 뒷단에 닿은 뒤의 재시도는 그 한 건이 곧 초과 발급이고,
     * 연결이 안 됐다는 것만이 그 요청이 아무 일도 안 했음을 보장한다.
     */
    private GatewayFilter connectRetry(RetryGatewayFilterFactory retries) {
        return retries.apply(connectRetryPolicy.config());
    }

    /**
     * 걸려 있는 건수를 값으로 낸다. 제어 평면이 종료할 때 이 값을 본다. <b>타입이
     * 아니라 값이다</b> — 게이트웨이 타입을 참조하면 제어 평면이 요청 경로를 안다.
     */
    @Bean(HealthConfig.IN_FLIGHT)
    public IntSupplier inFlightRequests(AdmissionGatewayFilter admission) {
        return admission::inFlight;
    }

    /**
     * 본문 쓰기 상한. <b>응답 상한의 배수</b>라 배포 없이 같이 움직인다. 따로 적으면
     * 응답 상한만 고쳤을 때 두 값이 갈리고, 어느 쪽이 먼저 끊는지가 바뀐다.
     */
    private BodyDeadline bodyDeadline(Backend backend, MeterRegistry meters) {
        return BodyDeadline.of(backend.responseTimeout().multipliedBy(2),
                backend.responseTimeout(), meters);
    }

    /**
     * 뒷단으로 가는 주소. 라우팅이 켜지면 {@code lb://} 로 보내 균형기가 고른다.
     * <b>끄면 단일 주소로 돌아간다</b> — 설정 한 줄이 롤백 수단이다.
     */
    private String backendUri(Backend backend, ObjectProvider<RoutingProperties> routing) {
        RoutingProperties properties = routing.getIfAvailable();
        return properties == null || !properties.enabled()
                ? backend.uri() : "lb://" + properties.serviceId();
    }

    /**
     * 진입 규칙의 뒷단은 하나다. 서킷과 배분이 뒷단을 안 가르므로, 둘이면 한쪽 장애가 다른 쪽 발급을
     * 폴백으로 보낸다. 가르려면 쿠폰이 뒷단에 묶여야 한다. 주소를 비운 규칙은 공통 뒷단으로 센다.
     */
    private void requireOneEntryBackend(RouteRules rules, String shared) {
        Map<String, List<String>> byBackend = new TreeMap<>();
        for (RouteRules.Rule rule : rules.rules()) {
            if (rule.kind() == RouteRules.Kind.ENTRY) {
                String backend = backendKey(rule.uri() == null ? shared : rule.uri());
                byBackend.computeIfAbsent(backend, key -> new ArrayList<>()).add(rule.id());
            }
        }
        // 주소는 안 싣는다. 내부 호스트명이 기동 로그로 샌다. 규칙 이름이면 어디를 고칠지 안다.
        if (byBackend.size() > 1) {
            throw new IllegalStateException("진입 규칙의 뒷단이 둘 이상이다 — 서킷과 배분이 하나라 "
                    + "장애가 섞인다. 뒷단별 규칙: " + byBackend.values());
        }
    }

    /** 같은 뒷단이면 같은 값. 스킴·호스트는 대소문자를, 포트는 스킴의 기본값을 맞춘다. */
    private String backendKey(String uri) {
        URI parsed = URI.create(uri);
        String scheme = parsed.getScheme().toLowerCase(Locale.ROOT);
        int port = parsed.getPort() != -1 ? parsed.getPort()
                : "https".equals(scheme) ? 443 : "http".equals(scheme) ? 80 : -1;
        return scheme + "://" + parsed.getHost().toLowerCase(Locale.ROOT) + ":" + port;
    }

    @Bean
    public RouteLocator routes(RouteLocatorBuilder builder, Backend backend, RouteRules rules,
            AdmissionGatewayFilter admission, QueryCoalescingFilter coalescing,
            SoldOutObserver soldOut, SpringCloudCircuitBreakerFilterFactory breakers,
            MeterRegistry meters, ObjectProvider<RoutingProperties> routing,
            RetryGatewayFilterFactory retries) {
        BodyDeadline bodyDeadline = bodyDeadline(backend, meters);
        String shared = backendUri(backend, routing);
        RouteLocatorBuilder.Builder built = builder.routes();
        for (RouteRules.Rule rule : rules.forwarded()) {
            // **규칙이 주소를 적으면 그쪽이 이긴다.** 안 적으면 공통 뒷단으로 간다 —
            // 규칙 하나만 쓰던 배포가 아무것도 안 고치고 그대로 돈다.
            // **규칙이 주소를 적으면 그 규칙만 균형기 밖으로 나간다.** 규칙의 주소는
            // http/https 로만 받으므로 `lb://` 가 될 수 없다 — 노드 선택도 재시도도
            // 그 경로에서 꺼진다. 라우팅을 켠 채로는 그 조합을 안 받는다.
            if (rule.uri() != null && shared.startsWith("lb://")) {
                throw new IllegalStateException(
                        "라우팅이 켜져 있는데 규칙 '" + rule.id() + "' 이 제 주소를 적었다"
                                + " — 그 경로만 균형기를 우회한다");
            }
            String uri = rule.uri() != null ? rule.uri() : shared;
            // 죽은 주소로 간 요청이 5xx 로 새면 안 된다. **균형기가 있을 때만 건다** —
            // 단일 주소로 되돌리면 고를 다음 대가 없어 같은 죽은 주소로 두 번 간다.
            boolean balanced = uri.startsWith("lb://");
            built = built.route(rule.id(), r -> r
                    .method(rule.resolvedMethod())
                    .and().path(rule.expandedPaths().toArray(String[]::new))
                    .and().predicate(rawPathIsPlain())
                    // **앞뒤를 값으로 정한다.** 안 정하면 둘 다 0 이라 선언 위치를
                    // 옮기는 것만으로 순서가 바뀌고, 서킷이 판정 앞으로 가면 래치가
                    // 죽는다 (FilterOrder).
                    .filters(f -> {
                        GatewayFilterSpec spec = stripSpoofableClientIp(f);
                        // **성질이 늘면 여기가 컴파일 에러가 되어야 한다.** `else` 로
                        // 두면 새 성질에 모으기가 조용히 붙는다.
                        switch (rule.kind()) {
                            case ENTRY -> spec = spec
                                    .filter(admission, FilterOrder.ROUTE_ADMISSION)
                                    .filter(circuit(breakers, rule.id()),
                                            FilterOrder.ROUTE_CIRCUIT);
                            // **모으기는 조회에만 붙인다.** 발급에 붙이면 같은 응답을
                            // 여럿이 받고, 그건 곧 초과 발급이다.
                            case QUERY -> spec = spec
                                    .filter(coalescing, FilterOrder.ROUTE_COALESCING);
                            case QUEUE -> throw new IllegalStateException(
                                    "줄 조회는 라우트가 아니다: " + rule.id());
                            default -> throw new IllegalStateException(
                                    "모르는 성질: " + rule.kind());
                        }
                        if (balanced) {
                            spec = spec.filter(connectRetry(retries), FilterOrder.ROUTE_RETRY);
                        }
                        if (rule.kind() == RouteRules.Kind.ENTRY) {
                            // **진입에만 붙인다.** 조회 응답에는 매진 코드가 재고
                            // 정보로 실릴 수 있고, 그건 관찰이 아니다.
                            spec = spec.filter(soldOut, FilterOrder.ROUTE_SOLD_OUT);
                        }
                        // **본문이 안 끝나는 뒷단을 끊는다.** 응답 상한은 헤더까지만
                        // 재고, 헤더가 나간 뒤라 판정 쪽 시한도 커넥션을 못 끊는다.
                        return spec.filter(bodyDeadline, FilterOrder.ROUTE_BODY);
                    })
                    // **끊는 자리가 서킷 안쪽이어야 한다.** 밖에서 끊으면 서킷에 가는
                    // 것은 오류가 아니라 취소이고, 취소는 창에 안 쌓인다 — 멎은 뒷단의
                    // 서킷이 영영 안 열린다. 조회 쪽도 마찬가지다: 멎은 요청 하나가
                    // 그 키를 영구히 잠그고 뒤이은 조회가 모두 거기 붙는다.
                    .metadata(RESPONSE_TIMEOUT_ATTR, backend.responseTimeout().toMillis())
                    .metadata(CONNECT_TIMEOUT_ATTR, (int) backend.connectTimeout().toMillis())
                    .uri(uri));
        }
        // 규칙마다 도는 검사 뒤에 둔다. 균형기 우회가 더 정확한 원인이라 그쪽이 먼저 알린다.
        requireOneEntryBackend(rules, shared);
        return built.build();
    }

}

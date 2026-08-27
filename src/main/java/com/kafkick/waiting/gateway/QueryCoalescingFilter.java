package com.kafkick.waiting.gateway;

import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 같은 조회를 <b>뒷단 한 번</b>으로 모으고, 아주 짧게 들고 있습니다.
 *
 * <p>발급은 판정이 막아 주는데 조회는 그대로 통과합니다. 그리고 이건 R2 까지
 * 갉아먹습니다 — 인스턴스가 조회로 포화되면 낮은 가용량을 보고하고, 발급 유입까지
 * 같이 조여집니다.
 */
public final class QueryCoalescingFilter implements GatewayFilter {

    private static final String METRIC = "waiting.coalescing";

    /**
     * 응답을 가르는 헤더. <b>이게 있으면 안 모읍니다.</b>
     *
     * <p>지금 조회 응답에는 개인화가 없습니다. 하지만 "내가 발급받았는지" 필드가
     * 하나 붙는 순간 남의 응답을 받게 되고, 사람 리뷰로는 그 한 줄을 못 막습니다.
     */
    private static final List<String> PERSONAL = List.of(
            HttpHeaders.AUTHORIZATION, "X-Member-Id", "X-Member-Grade", "Cookie");

    private final CoalescingProperties props;

    private final ResponseCache cache;

    private final SingleFlight<Captured> flight;

    private final MeterRegistry meters;

    private QueryCoalescingFilter(CoalescingProperties props, Clock clock,
            MeterRegistry meters) {
        this.props = Objects.requireNonNull(props, "props 는 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.cache = ResponseCache.of(clock, props.enabled() ? props.maxKeys() : 1);
        this.flight = SingleFlight.withMaxKeys(props.enabled() ? props.maxKeys() : 1);
    }

    public static QueryCoalescingFilter of(CoalescingProperties props, Clock clock,
            MeterRegistry meters) {
        return new QueryCoalescingFilter(props, clock, meters);
    }

    /** 뒷단이 돌려준 것. {@code oversize} 면 담지도 나눠 주지도 않는다. */
    private record Captured(int status, Map<String, List<String>> headers, byte[] body,
            boolean oversize) {
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().pathWithinApplication().value();
        // **화이트리스트 밖은 그대로 흘린다.** 기본이 켜짐이면 개인화된 응답이
        // 붙는 순간 남의 응답을 받는다.
        if (!HttpMethod.GET.equals(exchange.getRequest().getMethod())
                || !props.covers(path)) {
            return chain.filter(exchange);
        }
        // **응답을 가르는 헤더가 있으면 안 모은다.** 하나로 모으면 그 값이 다른
        // 사람이 같은 응답을 받는다.
        if (hasPersonalHeader(exchange)) {
            count("skipped", "personal");
            return chain.filter(exchange);
        }

        String key = keyOf(exchange, path);
        Duration ttl = props.ttlByPath().get(path);

        Optional<ResponseCache.Entry> hit = cache.get(key);
        if (hit.isPresent()) {
            count("hit", "cache");
            return write(exchange, hit.get());
        }

        // 내가 뒷단을 부른 쪽인지 기억한다. 부른 쪽은 이미 자기 응답을 썼으므로
        // 두 번 쓰면 안 된다.
        AtomicBoolean called = new AtomicBoolean();
        return flight.join(key, () -> {
                    called.set(true);
                    return proxy(exchange, chain, ttl, key);
                })
                .flatMap(captured -> {
                    if (called.get()) {
                        return Mono.empty();
                    }
                    if (captured.oversize()) {
                        // 모으기를 포기한 응답이다. 나눠 줄 것이 없으니 각자 부른다.
                        count("skipped", "oversize");
                        return chain.filter(exchange);
                    }
                    count("hit", "flight");
                    return write(exchange, new ResponseCache.Entry(
                            captured.status(), captured.headers(), captured.body()));
                });
    }

    /**
     * 뒷단으로 보내면서 응답을 함께 담습니다.
     *
     * <p><b>흘려보내면서 담습니다.</b> 삼켰다가 다시 쓰면 상한을 넘긴 응답을
     * 되돌릴 방법이 없어, 보호 장치가 메모리 사고의 원인이 됩니다.
     */
    private Mono<Captured> proxy(ServerWebExchange exchange, GatewayFilterChain chain,
            Duration ttl, String key) {
        List<byte[]> chunks = new ArrayList<>();
        AtomicBoolean tooBig = new AtomicBoolean();
        ServerHttpResponse original = exchange.getResponse();
        ServerHttpResponseDecorator tee = new ServerHttpResponseDecorator(original) {
            @Override
            public Mono<Void> writeWith(org.reactivestreams.Publisher<? extends DataBuffer> body) {
                return super.writeWith(Flux.from(body).doOnNext(buffer -> capture(
                        buffer, chunks, tooBig)));
            }
        };
        count("miss", FailureCause.NONE);
        return chain.filter(exchange.mutate().response(tee).build())
                .then(Mono.fromSupplier(() -> finish(original, chunks, tooBig, ttl, key)));
    }

    /**
     * 담되 <b>상한에서 멈춥니다.</b> 넘긴 응답은 담지도 나눠 주지도 않습니다 —
     * 뒤엣사람은 각자 부릅니다.
     */
    private void capture(DataBuffer buffer, List<byte[]> chunks, AtomicBoolean tooBig) {
        if (tooBig.get()) {
            return;
        }
        int held = chunks.stream().mapToInt(c -> c.length).sum();
        if (held + buffer.readableByteCount() > props.maxBodyBytes()) {
            tooBig.set(true);
            chunks.clear();
            return;
        }
        byte[] copy = new byte[buffer.readableByteCount()];
        // **읽는 자리를 안 옮긴다.** 옮기면 뒤이어 나가는 본문이 잘린다.
        buffer.toByteBuffer().get(copy);
        chunks.add(copy);
    }

    private Captured finish(ServerHttpResponse response, List<byte[]> chunks,
            AtomicBoolean tooBig, Duration ttl, String key) {
        HttpStatusCode status = response.getStatusCode();
        int code = status == null ? 200 : status.value();
        // 헤더를 값으로 복사한다. 뷰를 들고 있으면 다음 요청이 그 응답의
        // 헤더를 고칠 때 담아 둔 것까지 같이 바뀐다.
        Map<String, List<String>> headers = new LinkedHashMap<>();
        response.getHeaders().forEach((name, values) -> headers.put(name, List.copyOf(values)));
        if (tooBig.get()) {
            return new Captured(code, headers, new byte[0], true);
        }
        byte[] body = join(chunks);
        Captured captured = new Captured(code, headers, body, false);
        // **장애 응답은 안 담는다.** 담으면 그 수명 동안 장애가 고정되고,
        // 뒷단이 멀쩡해져도 계속 실패를 돌려준다.
        if (code < 400 && !noStore(response.getHeaders())) {
            cache.put(key, new ResponseCache.Entry(code, headers, body), ttl);
        }
        return captured;
    }

    /** 뒷단이 담지 말라면 안 담는다. 우리 판단으로 덮어쓰지 않는다. */
    private boolean noStore(HttpHeaders headers) {
        String control = headers.getCacheControl();
        return control != null && control.contains("no-store");
    }

    private byte[] join(List<byte[]> chunks) {
        int total = chunks.stream().mapToInt(c -> c.length).sum();
        byte[] all = new byte[total];
        int at = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, all, at, chunk.length);
            at += chunk.length;
        }
        return all;
    }

    private Mono<Void> write(ServerWebExchange exchange, ResponseCache.Entry entry) {
        ServerHttpResponse response = exchange.getResponse();
        if (response.isCommitted()) {
            return response.setComplete();
        }
        response.setRawStatusCode(entry.status());
        entry.headers().forEach((name, values) -> response.getHeaders().put(name, values));
        return response.writeWith(Mono.just(
                response.bufferFactory().wrap(entry.body())));
    }

    private boolean hasPersonalHeader(ServerWebExchange exchange) {
        HttpHeaders headers = exchange.getRequest().getHeaders();
        return headers.headerNames().stream()
                .anyMatch(name -> PERSONAL.stream().anyMatch(name::equalsIgnoreCase));
    }

    /** 경로와 쿼리로 만든다. 쿼리가 다르면 다른 응답이다. */
    private String keyOf(ServerWebExchange exchange, String path) {
        String query = exchange.getRequest().getURI().getRawQuery();
        return query == null ? path : path + "?" + query;
    }

    private void count(String outcome, String cause) {
        meters.counter(METRIC, "outcome", outcome, "cause", cause).increment();
    }

    /** 담고 있는 키 수. 지표가 이 값을 읽는다. */
    int cachedKeys() {
        return cache.size();
    }

    /** 지금 모으고 있는 키 수. */
    int inFlight() {
        return flight.inFlight();
    }

    static String utf8(byte[] body) {
        return new String(body, StandardCharsets.UTF_8);
    }
}

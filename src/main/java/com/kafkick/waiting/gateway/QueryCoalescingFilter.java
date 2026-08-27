package com.kafkick.waiting.gateway;

import com.kafkick.waiting.control.FailureWindow;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(QueryCoalescingFilter.class);

    private static final String METRIC = "waiting.coalescing";

    /**
     * 자격 증명. <b>이게 실려 오면 안 모읍니다.</b>
     *
     * <p>토큰을 든 요청은 정의상 그 사람 것입니다. 우리 API 는 이 헤더를 안 쓰므로
     * 실려 왔다면 뒷단이 다르게 답할 수 있다고 봅니다.
     */
    private static final List<String> CREDENTIALS = List.of(HttpHeaders.AUTHORIZATION);

    /**
     * 연결에 매인 헤더. <b>다시 쓸 때 옮기면 안 됩니다.</b>
     *
     * <p>담아 둔 값은 그때의 연결에 대한 것이라, 다른 연결에 그대로 실으면 길이와
     * 인코딩이 어긋나 응답이 안 끝납니다 — 클라이언트는 영원히 기다립니다.
     */
    private static final List<String> HOP_BY_HOP = List.of(
            "connection", "keep-alive", "transfer-encoding", "content-length",
            "te", "trailer", "upgrade", "proxy-authenticate", "proxy-authorization");

    private final CoalescingProperties props;

    private final Map<String, Duration> ttlByPath;

    private final ResponseCache cache;

    private final SingleFlight<Captured> flight;

    private final MeterRegistry meters;

    private final CoalescingKeys keys = CoalescingKeys.create();

    /** 키 상한에 닿아 모으기가 멎은 구간. 카운터만 두면 사후에 못 답한다 (LG-2). */
    private final FailureWindow saturation;

    private QueryCoalescingFilter(CoalescingProperties props, Clock clock,
            MeterRegistry meters) {
        this.props = Objects.requireNonNull(props, "props 는 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.cache = ResponseCache.of(clock, props.enabled() ? props.maxKeys() : 1);
        this.flight = SingleFlight.withMaxKeys(props.enabled() ? props.maxKeys() : 1);
        // **경로별 수명을 미리 뽑는다.** 요청마다 다시 만들면 100K RPS 에서
        // 조회 한 건마다 맵을 새로 짓는 셈이다.
        this.ttlByPath = props.ttlByPath();
        this.saturation = FailureWindow.create();
    }

    public static QueryCoalescingFilter of(CoalescingProperties props, Clock clock,
            MeterRegistry meters) {
        return new QueryCoalescingFilter(props, clock, meters);
    }

    /**
     * 뒷단이 돌려준 것.
     *
     * @param shareable 이 키로 모인 사람들에게 나눠 줘도 되는가. 거짓이면 각자
     *                  부른다 — 이름이 "크다" 였을 때 이 자리의 세 번째 사례를
     *                  아무도 못 떠올렸다
     * @param cause 못 나눠 주는 이유. 지표가 이 값으로 갈린다
     */
    private record Captured(int status, Map<String, List<String>> headers, byte[] body,
            boolean shareable, String cause) {
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
        // **자격 증명이 실려 오면 안 모은다.** 하나로 모으면 그 값이 다른 사람이
        // 같은 응답을 받는다.
        if (hasCredential(exchange)) {
            count("skipped", "credential");
            return chain.filter(exchange);
        }

        CoalescingKeys.Key key = keys.of(exchange, path);
        Duration ttl = ttlByPath.get(path);

        Optional<ResponseCache.Entry> hit = cache.get(key.value());
        if (hit.isPresent()) {
            count("hit", "cache");
            return write(exchange, hit.get());
        }

        // 내가 뒷단을 부른 쪽인지 기억한다. 부른 쪽은 이미 자기 응답을 썼으므로
        // 두 번 쓰면 안 된다.
        AtomicBoolean called = new AtomicBoolean();
        // **모으기가 멎었으면 그 사실을 남긴다.** 카운터만 두면 뒷단 도달 수가
        // 조용히 원상복귀하고, 아무도 이유를 모른다.
        if (flight.isFull() && saturation.entered()) {
            log.warn("모으기 포화 진입 — 키 상한 {} 를 채웠다. max-keys 를 올리거나 ttl 을 줄인다",
                    props.maxKeys());
        }
        return flight.join(key.value(), () -> {
                    called.set(true);
                    return proxy(exchange, chain, ttl, key, path);
                })
                .flatMap(captured -> {
                    if (called.get()) {
                        return Mono.empty();
                    }
                    if (!captured.shareable()) {
                        // 나눠 줄 수 없는 응답이다. 각자 부른다.
                        count("skipped", captured.cause());
                        return chain.filter(exchange);
                    }
                    count("hit", "flight");
                    return write(exchange, new ResponseCache.Entry(
                            captured.status(), captured.headers(), captured.body()));
                })
                // **뒷단이 터지면 뒤엣사람도 같이 실패한다.** 각자 재시도시키면
                // 죽은 뒷단에 모아 둔 수만큼이 한꺼번에 다시 간다 — 모으기가
                // 증폭기가 된다. 그 판단을 지표에 남긴다.
                .doOnError(e -> {
                    if (!called.get()) {
                        count("shared-failure", "backend");
                    }
                });
    }

    /**
     * 뒷단으로 보내면서 응답을 함께 담습니다.
     *
     * <p><b>흘려보내면서 담습니다.</b> 삼켰다가 다시 쓰면 상한을 넘긴 응답을
     * 되돌릴 방법이 없어, 보호 장치가 메모리 사고의 원인이 됩니다.
     */
    private Mono<Captured> proxy(ServerWebExchange exchange, GatewayFilterChain chain,
            Duration ttl, CoalescingKeys.Key key, String path) {
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
                .then(Mono.fromSupplier(() ->
                        finish(original, chunks, tooBig, ttl, key, path, exchange)));
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
            AtomicBoolean tooBig, Duration ttl, CoalescingKeys.Key key, String path,
            ServerWebExchange exchange) {
        HttpStatusCode status = response.getStatusCode();
        int code = status == null ? 200 : status.value();
        // 헤더를 값으로 복사한다. 뷰를 들고 있으면 다음 요청이 그 응답의
        // 헤더를 고칠 때 담아 둔 것까지 같이 바뀐다.
        Map<String, List<String>> headers = new LinkedHashMap<>();
        response.getHeaders().forEach((name, values) -> headers.put(name, List.copyOf(values)));
        if (tooBig.get()) {
            return new Captured(code, headers, new byte[0], false, "oversize");
        }
        byte[] body = join(chunks);
        List<String> learned = keys.learn(path, response.getHeaders());

        // **담는 것과 나눠 주는 것은 다른 판단이다.** 담을 때는 방금 배운 것으로
        // 다시 만든 키를 쓰므로 늘 안전하다. 안 담으면 경로마다 뒷단을 한 번씩
        // 더 부르고, 그 한 번이 오픈 순간의 버스트다.
        String storeKey = keys.of(exchange, path).value();
        // **장애 응답은 안 담는다.** 담으면 그 수명 동안 장애가 고정되고,
        // 뒷단이 멀쩡해져도 계속 실패를 돌려준다.
        if (code < 400 && !noStore(response.getHeaders())
                && !learned.contains(CoalescingKeys.ALL)) {
            cache.put(storeKey, new ResponseCache.Entry(code, headers, body), ttl);
            if (cache.isFull()) {
                count("skipped", "cache-full");
            }
        }

        // **배우기 전의 첫 무리가 가장 위험하다.** 그때는 갈림 헤더를 몰라 회원이
        // 서로 다른 요청들이 한 키에 붙어 있다. 응답이 "이 헤더로 갈린다" 고
        // 말하는 순간, 그 무리는 남의 응답을 받게 된다.
        if (!keys.shareable(key, learned)) {
            return new Captured(code, headers, new byte[0], false,
                    learned.contains(CoalescingKeys.ALL) ? "vary-all" : "vary-learned");
        }
        return new Captured(code, headers, body, true, FailureCause.NONE);
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
        // **연결에 매인 헤더는 안 옮긴다.** 담아 둔 값은 그때의 연결에 대한
        // 것이라, 다른 연결에 그대로 실으면 길이와 인코딩이 어긋나 응답이 안
        // 끝난다 — 클라이언트는 영원히 기다린다.
        entry.headers().forEach((name, values) -> {
            if (!HOP_BY_HOP.contains(name.toLowerCase(Locale.ROOT))) {
                response.getHeaders().put(name, values);
            }
        });
        return response.writeWith(Mono.just(
                response.bufferFactory().wrap(entry.body())));
    }

    // **쿠키는 여기 없다.** 브라우저는 분석 쿠키 하나만 있어도 매 요청에 싣는다 —
    // 있다고 거르면 이 기능이 브라우저에서 한 번도 안 돈다. 회원 헤더로 거르다
    // 통째로 죽였던 것과 같은 실수다. 쿠키로 갈리는 응답은 뒷단이 `Vary: Cookie`
    // 로 말해야 한다. 안 말하면 못 막고, 그때는 화이트리스트에서 빼야 한다.
    private boolean hasCredential(ServerWebExchange exchange) {
        HttpHeaders headers = exchange.getRequest().getHeaders();
        return headers.headerNames().stream()
                .anyMatch(name -> CREDENTIALS.stream().anyMatch(name::equalsIgnoreCase));
    }

    private void count(String outcome, String cause) {
        meters.counter(METRIC, "outcome", outcome, "cause", cause).increment();
    }

    /**
     * 상한에 얼마나 가까운지를 게이지로 냅니다.
     *
     * <p>상한에 닿으면 모으기가 조용히 멎습니다. 뒷단 도달 수만 원상복귀하고
     * 그림에는 아무것도 안 남습니다 (6.10.9 · 6.10.10).
     */
    public void bindMetrics(MeterRegistry registry) {
        Gauge.builder("waiting.coalescing.cached", cache, ResponseCache::size)
                .description("담고 있는 키 수. 상한에 닿으면 모으기가 멎는다")
                .strongReference(true)
                .register(registry);
        Gauge.builder("waiting.coalescing.in.flight", flight, SingleFlight::inFlight)
                .description("지금 모으고 있는 키 수")
                .strongReference(true)
                .register(registry);
    }
}

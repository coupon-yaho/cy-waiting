package com.kafkick.waiting.gateway;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.kafkick.waiting.control.FailureWindow;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
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
     * 뒷단이 <b>공유해도 된다고 말하는</b> 지시어.
     *
     * <p>표준 의미 그대로다 — 공유 캐시가 나눠 줘도 되는 응답. 자체 헤더를 만들면
     * 그 뜻을 두 팀이 각자 해석하게 되고, 발급 계층이 안 붙이는 날 조용히 나뉜다.
     */
    private static final String SHARED = "public";

    /** 계약이 아직 안 섰다는 신호. <b>거절과는 다른 사건이다.</b> */
    private static final String NO_SHARED_MARKER = "no-shared-marker";

    /**
     * 되돌리기 전에 연속으로 봐야 하는 선언 수.
     *
     * <p><b>뒷단이 여럿이면 롤링 배포 중 절반만 헤더를 붙인다.</b> 응답 하나로
     * 되돌리면 그 구간 내내 켜졌다 꺼졌다 하고, 진입·복귀 로그가 요청마다 나간다.
     */
    private static final int RECOVERY_STREAK = 20;

    /**
     * 자격 증명. <b>이게 실려 오면 안 모읍니다.</b>
     *
     * <p>토큰을 든 요청은 정의상 그 사람 것입니다. 우리 API 는 이 헤더를 안 쓰므로
     * 실려 왔다면 뒷단이 다르게 답할 수 있다고 봅니다.
     */
    private static final List<String> CREDENTIALS = List.of(HttpHeaders.AUTHORIZATION);

    /**
     * 응답의 뜻을 바꾸는 요청 헤더.
     *
     * <p>키에 안 넣을 것이면 모으지도 않아야 합니다 — 범위 요청이 전체를 받거나
     * 조건부 요청이 조건 없는 200 을 받습니다.
     */
    private static final List<String> SPECIAL = List.of(
            HttpHeaders.RANGE, HttpHeaders.IF_NONE_MATCH, HttpHeaders.IF_MODIFIED_SINCE,
            HttpHeaders.IF_MATCH, HttpHeaders.IF_UNMODIFIED_SINCE, HttpHeaders.IF_RANGE,
            HttpHeaders.CACHE_CONTROL, HttpHeaders.PRAGMA);

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

    /**
     * 공유 선언을 안 하는 경로.
     *
     * <p><b>가짓수가 유계다</b> — {@code filter} 의 {@code ttlByPath.containsKey}
     * 가드를 지난 경로만 들어온다. 밖에서 오는 값이 아니라 상한이 필요 없다.
     */
    private final Set<String> declined = ConcurrentHashMap.newKeySet();

    /** 연속으로 본 선언 수. 한 번이라도 안 오면 0 으로 돌아간다. */
    private final Map<String, AtomicInteger> declaring = new ConcurrentHashMap<>();

    /**
     * 계약이 안 선 구간을 <b>경로마다</b> 쌍으로 남긴다.
     *
     * <p>하나로 두면 두 경로가 잇달아 멎을 때 두 번째는 진입 로그가 안 나가고,
     * 첫 번째만 회복해도 창이 닫혀 "복귀" 가 찍힌다 — 아직 안 모으는 경로를 두고.
     */
    private final Map<String, FailureWindow> contracts = new ConcurrentHashMap<>();

    /** 키 상한에 닿아 모으기가 멎은 구간. 카운터만 두면 사후에 못 답한다 (LG-2). */
    private final FailureWindow saturation;

    private QueryCoalescingFilter(CoalescingProperties props, Clock clock,
            MeterRegistry meters) {
        this.props = Objects.requireNonNull(props, "props 는 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.cache = ResponseCache.ofBytes(clock, props.enabled() ? props.maxCacheBytes() : 1);
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
            boolean shareable, String cause, String key) {
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().pathWithinApplication().value();
        // **화이트리스트 밖은 그대로 흘린다.** 기본이 켜짐이면 개인화된 응답이
        // 붙는 순간 남의 응답을 받는다.
        if (!HttpMethod.GET.equals(exchange.getRequest().getMethod())
                || !props.enabled() || !ttlByPath.containsKey(path)) {
            return chain.filter(exchange);
        }
        // **자격 증명이 실려 오면 안 모은다.** 하나로 모으면 그 값이 다른 사람이
        // 같은 응답을 받는다.
        if (hasCredential(exchange)) {
            count("skipped", "credential");
            return chain.filter(exchange);
        }
        // **뜻이 다른 GET 은 같은 응답을 받으면 안 된다.** 범위 요청이 전체를
        // 받거나, 조건부 요청이 조건 없는 200 을 받는다.
        if (isSpecialRequest(exchange)) {
            count("skipped", "request-directive");
            return chain.filter(exchange);
        }

        // **선언을 안 하는 뒷단에는 붙지 않는다.** 붙으면 리더의 왕복이 끝난 뒤
        // 각자 다시 부르므로, 뒷단 부하는 필터가 없을 때와 같고 지연만 두 배가
        // 된다 — 없느니만 못한 상태다. 응답을 보면 배우므로 붙지 않아도 회복된다.
        if (declined.contains(path)) {
            count("skipped", NO_SHARED_MARKER);
            // **요청을 센다.** 창을 여는 자리에서 세면 경로 수가 되어, 세 시간짜리
            // 구간에도 "1건을 못 모았다" 로 찍힌다.
            contracts.computeIfAbsent(path, p -> FailureWindow.create()).entered();
            return passThrough(exchange, chain, path);
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
        if (flight.isFull() || cache.isFull()) {
            if (saturation.entered()) {
                log.warn("모으기 포화 진입 — 상한을 채웠다. max-keys 나 max-cache-bytes 를 "
                        + "올리거나 ttl 을 줄인다");
            }
        } else {
            // 쌍으로 안 남기면 진입만 있고 언제 풀렸는지가 없다 (LG-2). 그리고
            // 한 번 켠 뒤 안 끄면 두 번째 포화부터는 조용하다.
            saturation.exited().ifPresent(r -> log.warn(
                    "모으기 포화 해제 — {}초 동안 {}건을 못 모았다",
                    NANOSECONDS.toSeconds(r.elapsedNanos()), r.swallowed()));
        }
        return flight.join(key.value(), () -> {
                    called.set(true);
                    return proxy(exchange, chain, ttl, key, path);
                })
                .flatMap(captured -> {
                    if (called.get()) {
                        return Mono.empty();
                    }
                    // **값이 같은 사람에게는 줄 수 있다.** 배우기 전에 모인 무리를
                    // 통째로 돌려보내면, 정작 모여야 할 오픈 첫 버스트가 하나도
                    // 안 모인다 — 캐시가 가장 차가운 순간이 가장 뜨거운 순간이다.
                    if (!captured.shareable()
                            && captured.key() != null
                            && captured.key().equals(keys.of(exchange, path).value())) {
                        count("hit", "flight-revalidated");
                        return write(exchange, new ResponseCache.Entry(
                                captured.status(), captured.headers(), captured.body()));
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
        // **들고 다닌다.** 청크마다 앞엣것을 다시 합산하면 제곱 시간이고, 그
        // 계산이 요청 경로의 이벤트 루프에서 돈다.
        AtomicLong held = new AtomicLong();
        AtomicBoolean tooBig = new AtomicBoolean();
        ServerHttpResponse original = exchange.getResponse();
        ServerHttpResponseDecorator tee = new ServerHttpResponseDecorator(original) {
            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                return super.writeWith(Flux.from(body).doOnNext(buffer -> capture(
                        buffer, chunks, held, tooBig)));
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
    private void capture(DataBuffer buffer, List<byte[]> chunks, AtomicLong held,
            AtomicBoolean tooBig) {
        if (tooBig.get()) {
            return;
        }
        if (held.get() + buffer.readableByteCount() > props.maxBodyBytes()) {
            tooBig.set(true);
            chunks.clear();
            held.set(0);
            return;
        }
        byte[] copy = new byte[buffer.readableByteCount()];
        // **읽는 자리를 안 옮긴다.** 옮기면 뒤이어 나가는 본문이 잘린다.
        buffer.toByteBuffer().get(copy);
        chunks.add(copy);
        held.addAndGet(copy.length);
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
            return new Captured(code, headers, new byte[0], false, "oversize", null);
        }
        byte[] body = join(chunks);
        List<String> learned = keys.learn(path, response.getHeaders());
        // **한 번만 만든다.** 응답마다 소문자 사본과 집합을 두세 번 짓는 것은
        // 5ms 예산(G6.11)을 쓰는 자리다.
        Set<String> directives = directives(response.getHeaders().getCacheControl());
        learnDeclaration(path, directives, code);

        // **담는 것과 나눠 주는 것은 다른 판단이다.** 담을 때는 방금 배운 것으로
        // 다시 만든 키를 쓰므로 늘 안전하다. 안 담으면 경로마다 뒷단을 한 번씩
        // 더 부르고, 그 한 번이 오픈 순간의 버스트다.
        String storeKey = keys.of(exchange, path).value();
        // **장애 응답은 안 담는다.** 담으면 그 수명 동안 장애가 고정되고,
        // 뒷단이 멀쩡해져도 계속 실패를 돌려준다.
        String refusal = code < 400 ? refusal(response.getHeaders(), directives) : null;
        boolean shareable = code < 400 && refusal == null
                && !learned.contains(CoalescingKeys.ALL);
        if (shareable) {
            cache.put(storeKey, new ResponseCache.Entry(code, headers, body), ttl);
            if (cache.isFull()) {
                count("skipped", "cache-full");
            }
        }

        // **배우기 전의 첫 무리가 가장 위험하다.** 그때는 갈림 헤더를 몰라 회원이
        // 서로 다른 요청들이 한 키에 붙어 있다. 응답이 "이 헤더로 갈린다" 고
        // 말하는 순간, 그 무리는 남의 응답을 받게 된다.
        if (!shareable) {
            // **담기만 막으면 안 된다.** 지금 모여 있는 사람들이 그대로 그 응답을
            // 받는다 — no-store 를 낸 뒷단이 막으려던 것이 정확히 그것이다.
            // 키를 안 실어 보내 아무도 되받지 못하게 한다.
            String cause = learned.contains(CoalescingKeys.ALL) ? "vary-all"
                    : code >= 400 ? "error-status" : refusal;
            // **부른 쪽에서도 센다.** 뒤에 모인 사람 경로에서만 세면 순차 트래픽
            // — 프로덕션의 보통 상태 — 에서 이 카운터가 아예 안 오른다. 그러면
            // "계약이 안 서서 모으기가 꺼졌다" 는 사실의 신호가 하나도 없다.
            count("refused", cause);
            return new Captured(code, headers, new byte[0], false, cause, null);
        }
        if (!keys.shareable(key, learned)) {
            // 배우기 전에 모인 무리다. 갈리는 값이 같은 사람은 이 키로 되찾아 간다.
            return new Captured(code, headers, body, false, "vary-learned", storeKey);
        }
        return new Captured(code, headers, body, true, FailureCause.NONE, storeKey);
    }

    /**
     * 붙지 않고 그대로 흘리되 <b>응답은 본다.</b>
     *
     * <p>안 보면 한 번의 누락이 영구가 됩니다 — 안 붙으니 응답을 못 보고, 못 보니
     * 선언이 돌아온 것을 모릅니다. 본문은 안 건드리고 헤더만 읽습니다.
     */
    // **커밋될 때만 돕니다.** 클라이언트가 중간에 끊으면 콜백이 아예 안 돌고,
    // 오류 응답은 커밋되더라도 학습에서 걸러진다. 그래서 실제 회복 조건은
    // "커밋된 정상 응답이 연속으로 와야 한다" 이고, 그동안 이 경로는 계속 안 붙는다.
    // 안 붙는 동안에는 Vary 학습도 같이 멎는다.
    //
    // 체인보다 먼저 등록하므로 뒤 필터가 Cache-Control 을 덧붙이면 그것은 못 본다.
    // 지금 그런 필터는 없고, 스모크 시나리오가 "우리만 달면 게이트웨이가 드러난다"
    // 로 그 전제를 지킨다.
    private Mono<Void> passThrough(ServerWebExchange exchange, GatewayFilterChain chain,
            String path) {
        exchange.getResponse().beforeCommit(() -> {
            ServerHttpResponse response = exchange.getResponse();
            HttpStatusCode status = response.getStatusCode();
            learnDeclaration(path, directives(response.getHeaders().getCacheControl()),
                    status == null ? HttpStatus.OK.value() : status.value());
            return Mono.empty();
        });
        return chain.filter(exchange);
    }

    /**
     * 이 경로의 뒷단이 <b>공유를 선언하는가</b>를 응답에서 배웁니다.
     *
     * <p>선언을 안 하면 다음부터는 안 붙습니다. 붙으면 지연만 두 배가 되고 뒷단
     * 부하는 그대로라 없느니만 못합니다. 선언이 오면 곧바로 되돌립니다.
     *
     * <p>실패 응답으로는 안 배웁니다 — 장애 구간의 5xx 에 헤더가 없다고 계약이
     * 깨진 것으로 읽으면, 뒷단이 살아난 뒤에도 안 모읍니다.
     */
    private void learnDeclaration(String path, Set<String> directives, int code) {
        if (code >= 400) {
            return;
        }
        FailureWindow window = contracts.computeIfAbsent(path, p -> FailureWindow.create());
        AtomicInteger streak = declaring.computeIfAbsent(path, p -> new AtomicInteger());
        if (directives.contains(SHARED)) {
            // **한 건으로 안 되돌린다.** 뒷단 절반만 헤더를 붙인 롤링 구간에서
            // 켜졌다 꺼졌다 하며 로그가 요청마다 나간다 (LG-3).
            if (streak.incrementAndGet() >= RECOVERY_STREAK && declined.remove(path)) {
                window.exited().ifPresent(r -> log.warn(
                        "공유 선언 복귀 — {} 에서 {}초 동안 {}건을 못 모았다",
                        path, NANOSECONDS.toSeconds(r.elapsedNanos()), r.swallowed()));
            }
            return;
        }
        streak.set(0);
        if (declined.add(path) && window.entered()) {
            log.warn("공유 선언 없음 — {} 의 응답에 Cache-Control: public 이 없어 "
                    + "모으기를 멈춘다. 발급 계층이 붙이기 전까지 뒷단 도달이 안 줄어든다", path);
        }
    }

    /**
     * 왜 못 나누는가. {@code null} 이면 나눠도 된다.
     *
     * <p><b>"거절했다" 와 "말한 적이 없다" 를 가릅니다.</b> 뒤엣것은 발급 계층과의
     * 계약이 아직 안 섰다는 신호라, 한 라벨에 묶으면 언제 닫을 수 있는지를 못 봅니다.
     */
    private String refusal(HttpHeaders headers) {
        return refusal(headers, directives(headers.getCacheControl()));
    }

    private String refusal(HttpHeaders headers, Set<String> directives) {
        if (!headers.getOrEmpty(HttpHeaders.SET_COOKIE).isEmpty()) {
            return "set-cookie";
        }
        if (directives.contains("no-store") || directives.contains("private")
                || directives.contains("no-cache")) {
            return "not-shareable";
        }
        return directives.contains(SHARED) ? null : NO_SHARED_MARKER;
    }

    // **쿠키를 심는 응답은 못 나눈다.** `public` 은 "공유 캐시가 저장해도 된다"
    // 이지 "개인 자격 증명이 없다" 가 아니다. 뒷단 프레임워크가 세션을 부트스트랩
    // 하며 붙이면 그 사이로 지나가고, 받는 브라우저는 남의 세션을 자기 것으로
    // 저장한다.
    //
    // 헤더만 벗기지 않는다. 벗기면 뒷단이 심으려던 쿠키가 리더에게만 가고
    // 나머지는 조용히 못 받아, 증상이 인증 실패로 나타난다.
    //
    // **말이 없어도 못 나눈다.** 개인화됐는지 아는 것은 뒷단뿐이고, 기본이
    // 나눔이면 필드 하나가 붙는 날 남의 응답이 나간다.
    /**
     * 지시어를 <b>토큰으로</b> 가릅니다.
     *
     * <p>부분 문자열로 보면 {@code no-public} 이라고 거절한 뒷단이 허락한 것으로
     * 읽히고, {@code community="public-catalog"} 같은 확장 지시어도 허락이 됩니다.
     */
    private Set<String> directives(String control) {
        if (control == null) {
            return Set.of();
        }
        Set<String> names = new HashSet<>();
        for (String part : control.toLowerCase(Locale.ROOT).split(",")) {
            // 값은 안 본다. 따옴표 안에 콤마가 있으면 조각이 갈리지만, 갈린
            // 조각이 지시어 이름과 같아질 일은 없다.
            int eq = part.indexOf('=');
            names.add((eq < 0 ? part : part.substring(0, eq)).strip());
        }
        return names;
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
    /**
     * 뜻이 달라 같은 응답을 받으면 안 되는 요청인가.
     *
     * <p>범위·조건부 요청과 캐시 지시어는 응답의 의미를 바꿉니다. 키에 안 넣을
     * 것이면 모으지도 않아야 합니다.
     */
    private boolean isSpecialRequest(ServerWebExchange exchange) {
        HttpHeaders headers = exchange.getRequest().getHeaders();
        return headers.headerNames().stream()
                .anyMatch(name -> SPECIAL.stream().anyMatch(name::equalsIgnoreCase));
    }

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
                .description("담고 있는 키 수")
                .strongReference(true)
                .register(registry);
        Gauge.builder("waiting.coalescing.bytes", cache, ResponseCache::bytes)
                .description("담고 있는 바이트. 예산에 닿으면 모으기가 멎는다")
                .strongReference(true)
                .register(registry);
        Gauge.builder("waiting.coalescing.in.flight", flight, SingleFlight::inFlight)
                .description("지금 모으고 있는 키 수")
                .strongReference(true)
                .register(registry);
    }
}

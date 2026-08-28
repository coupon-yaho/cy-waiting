package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.SecondWindowLimiter.AcquireResult;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;
import reactor.core.publisher.Mono;

/**
 * 한 사람이 노드 예산을 다 먹는 것을 막는다.
 *
 * <p>판정의 상한은 쿠폰별과 노드 전역뿐이라 <b>사용자 단위 상한이 어디에도 없다.</b>
 * 큐가 결국 막긴 하지만, 정상 사용자를 전부 큐로 미는 것 자체가 공격 성공이다.
 */
@Component
@Order(FilterOrder.ABUSE)
public final class AbuseLimitFilter implements WebFilter {

    private static final PathPattern QUEUE = PathPatternParser.defaultInstance
            .parse("/api/v1/coupons/{couponId}/queue");

    private static final PathPattern API = PathPatternParser.defaultInstance.parse("/api/**");

    private static final String MEMBER_ID = "X-Member-Id";

    /**
     * 프록시가 붙이는 헤더. <b>맨 끝만 믿는다</b> — 앞쪽은 클라이언트가 채워 넣을
     * 수 있어, 그걸 믿으면 남의 IP 로 위장해 남의 몫을 태운다.
     */
    private static final String FORWARDED_FOR = "X-Forwarded-For";

    private static final String METRIC = "waiting.abuse";

    /** 발급 경로의 사람당 초당 상한. 사람이 손으로 누를 수 있는 수의 몇 배다. */
    private static final long MEMBER_ISSUE_CAP = 5;

    /**
     * 주소당 상한. <b>사람당보다 훨씬 크다</b> — NAT 뒤에서는 수백 명이 한 주소를
     * 쓰고, 좁게 잡으면 그들이 통째로 막힌다. 여기서 잡는 것은 한 대의 처리량이다.
     */
    private static final long IP_ISSUE_CAP = 200;

    /**
     * 폴링 경로의 상한.
     *
     * <p><b>발급보다 느슨하다.</b> 1초 간격으로 물으라고 해 놓고 그 폴링을 막으면
     * 정상 대기자가 끊긴다. 탭이 여럿일 수 있으니 그 열 배를 준다.
     */
    private static final long MEMBER_POLL_CAP = 10;

    /** 주소당 폴링 상한. 대기자 전원이 같은 회사에서 물을 수 있다. */
    private static final long IP_POLL_CAP = 2_000;

    /** 키 상한. 식별자를 바꿔가며 메모리를 밀어내는 것을 막는다. */
    private static final int MAX_KEYS = 100_000;

    /** 배수를 안 거는 갈래. {@code 1.0} 을 그대로 쓰면 깜빡한 것과 구분이 안 된다. */
    private static final double NO_SCALE = 1.0;

    private static final PollIntervalPolicy BACKOFF = PollIntervalPolicy.of(0.2);

    private final SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(MAX_KEYS);
    private final TrustedProxies trusted;
    private final Clock clock;
    private final MeterRegistry meters;
    private final DoubleSupplier random;
    private final ApiError error;

    private AbuseLimitFilter(Clock clock, MeterRegistry meters, DoubleSupplier random,
            TrustedProxies trusted) {
        this.trusted = Objects.requireNonNull(trusted, "trusted 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.random = Objects.requireNonNull(random, "random 은 필수다");
        this.error = ApiError.of(clock);
    }

    /** 흔들림의 난수원은 스레드마다 따로 둔다 — 공유하면 그 자체가 경합점이다. */
    @Autowired
    AbuseLimitFilter(Clock clock, MeterRegistry meters, TrustedProxies trusted) {
        this(clock, meters, () -> ThreadLocalRandom.current().nextDouble(), trusted);
    }

    public static AbuseLimitFilter of(Clock clock, MeterRegistry meters, TrustedProxies trusted) {
        return new AbuseLimitFilter(clock, meters, trusted);
    }

    /** 난수원을 받는다. 고정하지 못하면 흔들림이 실제로 붙었는지 못 잰다 (TS-4). */
    public static AbuseLimitFilter of(Clock clock, MeterRegistry meters, DoubleSupplier random,
            TrustedProxies trusted) {
        return new AbuseLimitFilter(clock, meters, random, trusted);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!API.matches(exchange.getRequest().getPath().pathWithinApplication())) {
            return chain.filter(exchange);
        }
        long nowSec = clock.instant().getEpochSecond();
        boolean polling = polling(exchange);

        // **둘 다 보되 상한이 다르다.** 회원 식별자는 바꾸는 비용이 0 이라 그것만
        // 으로는 우회되고, 주소는 여럿이 나눠 쓰므로 좁게 잡으면 남을 막는다.
        String member = exchange.getRequest().getHeaders().getFirst(MEMBER_ID);
        String ip = clientIp(exchange);
        if (ip == null) {
            // **주소를 못 읽으면 막는다.** 열어 주면 그 상태를 만드는 것이 곧
            // 우회 통로가 된다 — 상한 없는 경로를 남기는 셈이다.
            return reject(exchange, "no-address");
        }
        if (member == null) {
            // 형식 검증이 앞에서 걸렀어야 한다. 주소 상한만으로 간다.
            return limiter.tryAcquire("abuse:i:" + ip,
                    polling ? IP_POLL_CAP : IP_ISSUE_CAP, nowSec)
                    ? chain.filter(exchange)
                    : reject(exchange, "ip");
        }
        // **한 걸음에 둘 다 잡는다.** 따로 차감하면 뒤에서 거부됐을 때 앞의 몫이
        // 이미 깎여, 통과한 요청이 하나도 없는데 예산이 빈다.
        AcquireResult acquired = limiter.tryAcquireAll(
                "abuse:m:" + member, polling ? MEMBER_POLL_CAP : MEMBER_ISSUE_CAP,
                "abuse:i:" + ip, polling ? IP_POLL_CAP : IP_ISSUE_CAP, nowSec);
        return switch (acquired) {
            case ACQUIRED -> chain.filter(exchange);
            case COUPON_EXHAUSTED -> reject(exchange, "member");
            case GLOBAL_EXHAUSTED -> reject(exchange, "ip");
            // 키가 상한에 닿았다. 남용과 구별이 안 되지만 열어 주면 그것이 곧 통로다.
            case KEY_SATURATED -> reject(exchange, "saturated");
        };
    }

    /**
     * 지시한 간격을 지킨 사람은 안 걸려야 한다. 게이트웨이가 물으라고 해 놓고
     * 그 폴링을 막으면 정상 대기자가 끊긴다.
     */
    private boolean polling(ServerWebExchange exchange) {
        return QUEUE.matches(exchange.getRequest().getPath().pathWithinApplication());
    }

    /**
     * <b>신뢰하는 홉을 지나온 요청만 헤더를 믿는다.</b> 아무나 채워 넣을 수 있게
     * 두면 매 요청 다른 값을 넣어 상한을 넘고, 더 나쁘게는 키를 무한히 만들어
     * 상한에 닿게 한다 — 그때부터 정상 사용자가 막힌다.
     */
    private String clientIp(ServerWebExchange exchange) {
        String socket = socketAddress(exchange);
        if (socket == null || !trusted.isTrusted(socket)) {
            return socket;
        }
        // 프록시가 자기 앞의 주소를 뒤에 붙이므로 우리가 아는 홉이 넣은 값은 마지막이다.
        List<String> forwarded = exchange.getRequest().getHeaders().get(FORWARDED_FOR);
        if (forwarded == null || forwarded.isEmpty()) {
            return socket;
        }
        String last = forwarded.get(forwarded.size() - 1);
        String candidate = last.substring(last.lastIndexOf(',') + 1).trim();
        // **주소로 안 읽히면 버린다.** 프록시 주소로 바꾸면 그 뒤의 모두가 한 몫을
        // 나눠 쓰고, 그대로 키로 쓰면 값을 바꿔가며 키를 무한히 만들 수 있다.
        return TrustedProxies.literal(candidate) == null ? null : candidate;
    }

    /** 미해결 주소는 {@code getAddress()} 가 비어 있다. 그대로 부르면 터진다. */
    private String socketAddress(ServerWebExchange exchange) {
        InetSocketAddress remote = exchange.getRequest().getRemoteAddress();
        if (remote == null || remote.getAddress() == null) {
            return null;
        }
        return remote.getAddress().getHostAddress();
    }

    /**
     * <b>큐에 안 넣는다.</b> 넣으면 공격자가 자리를 차지하고, 그 자리는 정상
     * 사용자의 것이다.
     */
    // **배수를 명시적으로 안 건다.** 이 갈래는 판정보다 앞이라 재료를 아직 안
    // 봤고, 여기서 홀더를 읽으면 요청 경로에 판정과 무관한 의존이 하나 는다.
    // 남용 요청을 예산에 맞춰 배려할 이유도 없다.
    private Mono<Void> reject(ServerWebExchange exchange, String kind) {
        meters.counter(METRIC, "key", kind).increment();
        return error.write(exchange, ApiError.Code.RATE_LIMITED,
                (int) BACKOFF.intervalSec(EtaPolicy.UNKNOWN, random, NO_SCALE));
    }
}

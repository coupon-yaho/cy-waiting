package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
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
     * 주소당 상한. <b>사람당 상한보다 훨씬 크다</b> — NAT 뒤에서는 수백 명이
     * 한 주소를 쓰고, 좁게 잡으면 그들이 통째로 막힌다.
     *
     * <p>여기서 잡는 것은 한 대가 두드리는 것이다. 식별자를 바꿔가며 우회해도
     * 이 상한이 그 기계의 처리량을 묶는다.
     */
    private static final long IP_ISSUE_CAP = 200;

    /**
     * 폴링 경로의 상한.
     *
     * <p><b>발급보다 느슨하다.</b> 게이트웨이가 1초 간격으로 물으라고 해 놓고 그
     * 폴링을 남용으로 막으면 정상 대기자가 끊긴다. 가장 짧은 밴드가 1초이고
     * 탭이 여럿일 수 있으니 그 열 배를 준다.
     */
    private static final long MEMBER_POLL_CAP = 10;

    /** 주소당 폴링 상한. 대기자 전원이 같은 회사에서 물을 수 있다. */
    private static final long IP_POLL_CAP = 2_000;

    /** 키 상한. 식별자를 바꿔가며 메모리를 밀어내는 것을 막는다. */
    private static final int MAX_KEYS = 100_000;

    private static final PollIntervalPolicy BACKOFF = PollIntervalPolicy.of(0.2);

    private final SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(MAX_KEYS);
    private final Clock clock;
    private final MeterRegistry meters;
    private final DoubleSupplier random;
    private final ApiError error;

    private AbuseLimitFilter(Clock clock, MeterRegistry meters, DoubleSupplier random) {
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.random = Objects.requireNonNull(random, "random 은 필수다");
        this.error = ApiError.of(clock);
    }

    /** 흔들림의 난수원은 스레드마다 따로 둔다 — 공유하면 그 자체가 경합점이다. */
    @Autowired
    AbuseLimitFilter(Clock clock, MeterRegistry meters) {
        this(clock, meters, () -> ThreadLocalRandom.current().nextDouble());
    }

    public static AbuseLimitFilter of(Clock clock, MeterRegistry meters) {
        return new AbuseLimitFilter(clock, meters);
    }

    /** 난수원을 받는다. 고정하지 못하면 흔들림이 실제로 붙었는지 못 잰다 (TS-4). */
    public static AbuseLimitFilter of(Clock clock, MeterRegistry meters, DoubleSupplier random) {
        return new AbuseLimitFilter(clock, meters, random);
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
        long memberCap = polling ? MEMBER_POLL_CAP : MEMBER_ISSUE_CAP;
        if (member != null && !limiter.tryAcquire("abuse:m:" + member, memberCap, nowSec)) {
            return reject(exchange, "member");
        }
        String ip = clientIp(exchange);
        long ipCap = polling ? IP_POLL_CAP : IP_ISSUE_CAP;
        if (ip != null && !limiter.tryAcquire("abuse:i:" + ip, ipCap, nowSec)) {
            return reject(exchange, "ip");
        }
        return chain.filter(exchange);
    }

    /**
     * 지시한 간격을 지킨 사람은 안 걸려야 한다. 게이트웨이가 물으라고 해 놓고
     * 그 폴링을 막으면 정상 대기자가 끊긴다.
     */
    private boolean polling(ServerWebExchange exchange) {
        return QUEUE.matches(exchange.getRequest().getPath().pathWithinApplication());
    }

    /**
     * <b>맨 끝만 믿는다.</b> 프록시가 자기 앞의 주소를 뒤에 붙이므로, 우리가
     * 아는 홉이 넣은 값은 마지막이다. 앞쪽은 클라이언트가 채워 넣을 수 있다.
     */
    private String clientIp(ServerWebExchange exchange) {
        List<String> forwarded = exchange.getRequest().getHeaders().get(FORWARDED_FOR);
        if (forwarded == null || forwarded.isEmpty()) {
            var remote = exchange.getRequest().getRemoteAddress();
            return remote == null ? null : remote.getAddress().getHostAddress();
        }
        String last = forwarded.get(forwarded.size() - 1);
        int mark = last.lastIndexOf(',');
        return last.substring(mark + 1).trim();
    }

    /**
     * <b>큐에 안 넣는다.</b> 넣으면 공격자가 자리를 차지하고, 그 자리는 정상
     * 사용자의 것이다.
     */
    private Mono<Void> reject(ServerWebExchange exchange, String kind) {
        meters.counter(METRIC, "key", kind).increment();
        return error.write(exchange, ApiError.Code.RATE_LIMITED,
                (int) BACKOFF.intervalSec(EtaPolicy.UNKNOWN, random));
    }
}

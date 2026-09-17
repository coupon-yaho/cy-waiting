package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter.AcquireResult;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter.Axis;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import com.kafkick.waiting.domain.net.IpLiteral;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Arrays;
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
 * 한 사람이 노드 예산을 다 먹는 것을 막는다. 판정의 상한은 쿠폰별과 노드 전역뿐이라
 * <b>사용자 단위 상한이 어디에도 없다.</b> 큐가 결국 막긴 하지만, 정상 사용자를 전부
 * 큐로 미는 것 자체가 공격 성공이다.
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

    /** 접은 채로 통과한 수. 거절 지표와 섞으면 통제가 꺼진 구간이 남용 급증으로 읽힌다. */
    private static final String FOLDED = "waiting.abuse.folded";

    /** 접힘을 이만큼 더 들고 있는다. 매 초 비는 창을 해제로 읽지 않으려는 늦춤이다. */
    private static final long FOLD_LINGER_SECONDS = 3;

    /** 지금 접혀 있는가. 계수만으로는 "아직 꺼져 있는가" 를 못 묻는다. */
    private static final String FOLDING = "waiting.abuse.folding";

    private static final Logger log = LoggerFactory.getLogger(AbuseLimitFilter.class);

    /** 발급 경로의 사람당 초당 상한. 사람이 손으로 누를 수 있는 수의 몇 배다. */
    private static final long MEMBER_ISSUE_CAP = 5;

    /**
     * 주소당 상한. <b>사람당보다 훨씬 크다</b> — NAT 뒤에서는 수백 명이 한 주소를
     * 쓰고, 좁게 잡으면 그들이 통째로 막힌다. 여기서 잡는 것은 한 대의 처리량이다.
     */
    private static final long IP_ISSUE_CAP = 200;

    /**
     * 폴링 경로의 상한. <b>발급보다 느슨하다</b> — 1초 간격으로 물으라고 해 놓고 그
     * 폴링을 막으면 정상 대기자가 끊긴다. 탭이 여럿일 수 있으니 그 열 배를 준다.
     */
    private static final long MEMBER_POLL_CAP = 10;

    /** 주소당 폴링 상한. 대기자 전원이 같은 회사에서 물을 수 있다. */
    private static final long IP_POLL_CAP = 2_000;

    /**
     * 축당 키 상한. <b>피크(100K)의 두 배다</b> — 한 초에 서로 다른 사람이 피크만큼 오면 축이 차는데,
     * 식별자 축이 차면 통제가 접히고 주소 축이 차면 접을 곳조차 없다. v6 에서는 주소도 사람마다 달라
     * 두 축의 값 공간이 사실상 같으므로 대칭으로 둔다. 창은 매 초 통째로 비운다.
     */
    private static final int MAX_KEYS = 200_000;

    private static final PollIntervalPolicy POLL = PollIntervalPolicy.standard();

    private final SecondWindowLimiter limiter;

    /** 접힘이 시작된 초. 0 이면 안 접힌 상태다. */
    private final AtomicLong foldingSince = new AtomicLong();

    /** 접은 채로 지나간 수. 풀릴 때 한 번에 낸다. */
    private final AtomicLong foldedPassed = new AtomicLong();

    /** 마지막으로 축이 차 있던 초. 이 시각에서 늦춤만큼 지나야 해제로 친다. */
    private final AtomicLong foldSaturatedAt = new AtomicLong();

    /** 접힘의 진입과 해제를 한 걸음으로 묶는다. 갈라 두면 해제가 새 포화를 덮어 진입 줄이 사라진다. */
    private final Object foldLock = new Object();
    private final TrustedProxies trusted;
    private final Clock clock;
    private final MeterRegistry meters;
    private final DoubleSupplier random;
    private final ApiError error;

    private AbuseLimitFilter(Clock clock, MeterRegistry meters, DoubleSupplier random,
            TrustedProxies trusted) {
        this(clock, meters, random, trusted, SecondWindowLimiter.withMaxKeys(MAX_KEYS));
    }

    private AbuseLimitFilter(Clock clock, MeterRegistry meters, DoubleSupplier random,
            TrustedProxies trusted, SecondWindowLimiter limiter) {
        this.limiter = Objects.requireNonNull(limiter, "limiter 는 필수다");
        this.trusted = Objects.requireNonNull(trusted, "trusted 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.random = Objects.requireNonNull(random, "random 은 필수다");
        this.error = ApiError.of(clock);
        Gauge.builder(FOLDING, foldingSince, since -> since.get() == 0 ? 0 : 1)
                .register(this.meters);
    }

    /** 흔들림의 난수원은 스레드마다 따로 둔다 — 공유하면 그 자체가 경합점이다. */
    @Autowired
    AbuseLimitFilter(Clock clock, MeterRegistry meters, TrustedProxies trusted) {
        this(clock, meters, () -> ThreadLocalRandom.current().nextDouble(), trusted);
    }

    public static AbuseLimitFilter of(Clock clock, MeterRegistry meters, TrustedProxies trusted) {
        return new AbuseLimitFilter(clock, meters, trusted);
    }

    /** 리미터를 좁게 줘 포화를 만든다. <b>시험 자리다</b> — 운영은 키 상한이 십만이라 못 채운다. */
    static AbuseLimitFilter withLimiter(Clock clock, MeterRegistry meters, DoubleSupplier random,
            TrustedProxies trusted, SecondWindowLimiter limiter) {
        return new AbuseLimitFilter(clock, meters, random, trusted, limiter);
    }

    /** 난수원을 받는다. 고정하지 못하면 흔들림이 실제로 붙었는지 못 잰다. */
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
        long ipCap = polling ? IP_POLL_CAP : IP_ISSUE_CAP;
        if (member == null) {
            // 형식 검증이 앞에서 걸렀어야 한다. 주소 상한만으로 간다.
            return byAddressOnly(exchange, chain, ip, ipCap, nowSec, false);
        }
        // **한 걸음에 둘 다 잡는다.** 따로 차감하면 뒤에서 거부됐을 때 앞의 몫이
        // 이미 깎여, 통과한 요청이 하나도 없는데 예산이 빈다.
        AcquireResult acquired = limiter.tryAcquireAll(
                Axis.SECONDARY, "abuse:m:" + member,
                polling ? MEMBER_POLL_CAP : MEMBER_ISSUE_CAP,
                Axis.PRIMARY, addressKey(ip), ipCap, nowSec);
        return switch (acquired) {
            case ACQUIRED -> {
                foldMaybeEnded(nowSec);
                yield chain.filter(exchange);
            }
            case COUPON_EXHAUSTED -> reject(exchange, "member");
            case GLOBAL_EXHAUSTED -> reject(exchange, "ip");
            // **식별자 축이 찼으면 그 축을 접는다** (CY-925). 식별자에 서명이 없어 값을 바꿔가며
            // 채울 수 있는데, 거절로 두면 채운 쪽이 아니라 새로 온 대기자가 막힌다. 주소 축은
            // 신뢰 홉 검사로 닫혀 있어 그쪽만으로도 한 대의 처리량은 잡힌다.
            // 주소 축이 찬 것이라면 접을 곳이 없다. 같은 태그로 묶으면 운영자가 둘을 못 가른다.
            case KEY_SATURATED -> byAddressOnly(exchange, chain, ip, ipCap, nowSec,
                    limiter.saturated(Axis.SECONDARY, nowSec));
        };
    }

    /**
     * 식별자 축을 접고 주소 축만으로 본다. 여는 것이 아니라 축을 하나 줄이는 것이다.
     *
     * <p><b>접고 통과한 것도 센다</b> — 거절만 세면 포화가 활발할수록 지표가 조용해져, 통제가 꺼진 구간이
     * 운영자 눈에 "포화 없음" 으로 보인다.
     */
    private Mono<Void> byAddressOnly(ServerWebExchange exchange, WebFilterChain chain,
            String ip, long ipCap, long nowSec, boolean folded) {
        if (!limiter.tryAcquire(Axis.PRIMARY, addressKey(ip), ipCap, nowSec)) {
            // **접을 곳이 없는 쪽을 따로 센다.** 주소 축이 차서 막힌 것을 주소 상한 초과와 같은
            // 이름으로 묶으면, 접힘보다 심각한 상태가 평소 신호에 섞여 안 보인다.
            return reject(exchange,
                    limiter.saturated(Axis.PRIMARY, nowSec) ? "keyspace" : "ip");
        }
        if (folded) {
            // **거절 지표와 섞지 않는다.** 같은 메터에 얹으면 접힌 구간의 통과가 "막은 수" 로 합산된다.
            Counter.builder(FOLDED).register(meters).increment();
            foldStarted(nowSec);
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
     * <b>신뢰하는 홉을 지나온 요청만 헤더를 믿는다.</b> 아무나 채워 넣을 수 있게
     * 두면 매 요청 다른 값을 넣어 상한을 넘고, 더 나쁘게는 키를 무한히 만들어
     * 상한에 닿게 한다 — 그때부터 정상 사용자가 막힌다.
     */
    private String clientIp(ServerWebExchange exchange) {
        String socket = socketAddress(exchange);
        // **신뢰 판정은 원문으로 한다.** 접은 주소로 물으면 신뢰 대역의 경계가 달라진다.
        if (socket == null || !trusted.isTrusted(socket)) {
            return foldedSocket(socket);
        }
        // 프록시가 자기 앞의 주소를 뒤에 붙이므로 우리가 아는 홉이 넣은 값은 마지막이다.
        List<String> forwarded = exchange.getRequest().getHeaders().get(FORWARDED_FOR);
        if (forwarded == null || forwarded.isEmpty()) {
            return foldedSocket(socket);
        }
        String last = forwarded.get(forwarded.size() - 1);
        String candidate = last.substring(last.lastIndexOf(',') + 1).trim();
        // **주소로 안 읽히면 버린다.** 프록시 주소로 바꾸면 그 뒤의 모두가 한 몫을
        // 나눠 쓰고, 그대로 키로 쓰면 값을 바꿔가며 키를 무한히 만들 수 있다.
        //
        // **정규형으로 되만든다.** v6 는 같은 주소를 여러 모양으로 적을 수 있어(::1 · 0:0:0:0:0:0:0:1),
        // 원문을 키로 쓰면 표기만 바꿔 상한을 통째로 우회한다. 접힌 구간에서는 이 축이 유일한 문이다.
        return canonical(IpLiteral.parse(candidate));
    }

    /**
     * 바이트에서 되만든 주소 문자열. 같은 주소는 반드시 같은 키가 된다.
     *
     * <p><b>v6 는 /64 로 묶는다</b> (CY-940). 하나의 /64 가 주소를 1.8e19 개 주므로, 주소마다 키를 만들면
     * 주소당 상한이 매 요청 새 예산으로 리셋되고 키 공간도 유계가 아니다. v4 와 v4-mapped 는 그대로 둔다 —
     * 묶으면 한 주소가 아니라 대역 하나가 한 몫을 나눠 쓴다.
     */
    private String canonical(byte[] address) {
        if (address == null) {
            return null;
        }
        byte[] key = address;
        boolean prefix = address.length == V6_BYTES && !mappedV4(address);
        if (prefix) {
            key = address.clone();
            Arrays.fill(key, V6_PREFIX_BYTES, V6_BYTES, (byte) 0);
        }
        try {
            String text = InetAddress.getByAddress(key).getHostAddress();
            return prefix ? text + "/64" : text;
        } catch (UnknownHostException e) {
            return null;
        }
    }

    /**
     * 소켓 주소의 키. 앞단이 없는 배포도 전달 헤더 경로와 같은 함수를 쓴다 — 안 그러면 직결 v6 가 주소를 돌려
     * 상한을 우회한다.
     *
     * <p><b>못 읽으면 원문을 쓴다.</b> 소켓이 준 값은 사용자가 채운 것이 아니라 커널이 안 주소다. 스코프가
     * 붙은 링크 로컬(`fe80::1%eth0`)처럼 리터럴로 안 읽히는 모양이 있는데, 그것을 거절로 바꾸면 되던 연결이
     * 끊긴다. 존은 떼고 접는다.
     */
    private String foldedSocket(String socket) {
        if (socket == null) {
            return null;
        }
        int zone = socket.indexOf('%');
        String folded = canonical(IpLiteral.parse(zone < 0 ? socket : socket.substring(0, zone)));
        return folded != null ? folded : socket;
    }

    /** v4-mapped v6 (::ffff:a.b.c.d). 묶으면 v4 대역 하나가 한 몫이 되므로 주소 그대로 쓴다. */
    private boolean mappedV4(byte[] address) {
        for (int i = 0; i < 10; i++) {
            if (address[i] != 0) {
                return false;
            }
        }
        return address[10] == (byte) 0xff && address[11] == (byte) 0xff;
    }

    /** v6 주소의 바이트 수. */
    private static final int V6_BYTES = 16;

    /** /64 가 차지하는 바이트 수. 뒤는 인터페이스 식별자라 주인이 마음대로 바꾼다. */
    private static final int V6_PREFIX_BYTES = 8;

    /** 주소 축의 키. 두 곳에서 만들면 한쪽만 고쳐도 조용히 갈린다. */
    private String addressKey(String ip) {
        return "abuse:i:" + ip;
    }

    /**
     * 접힘이 시작됐다. <b>통제 하나가 꺼지는 전이라 한 번은 남긴다.</b> 두 번째 줄부터는 같은 사건이고,
     * 공격이 이어지는 내내 찍으면 그 구간이 로그를 덮는다.
     */
    private void foldStarted(long nowSec) {
        synchronized (foldLock) {
            foldSaturatedAt.set(nowSec);
            if (foldingSince.compareAndSet(0, nowSec)) {
                log.warn("식별자 축이 찼다 — 사람당 상한을 접고 주소 상한만으로 판정한다. "
                        + "키 상한과 유입을 함께 본다");
            }
        }
        foldedPassed.incrementAndGet();
    }

    /**
     * 접힘이 풀렸는가. <b>창이 비는 것은 해제가 아니다</b> — 리미터가 매 초 두 축을 통째로 비우므로 새 초의
     * 첫 요청은 늘 통과한다. 그것을 해제로 읽으면 공격 내내 진입과 해제가 초당 한 쌍씩 나가고, 남는 지속
     * 시간은 언제나 1초라 얼마나 오래 접혀 있었는지를 아무 줄도 말해 주지 않는다.
     */
    private void foldMaybeEnded(long nowSec) {
        // **정상 경로는 여기서 끝난다.** 접혀 있지 않으면 잠금까지 안 간다 — 요청마다 부르는 자리라
        // 여기서 걸어 잠그면 통제가 멀쩡한 구간이 필터 하나로 직렬화된다.
        if (foldingSince.get() == 0) {
            return;
        }
        synchronized (foldLock) {
            long since = foldingSince.get();
            if (since == 0 || nowSec - foldSaturatedAt.get() < FOLD_LINGER_SECONDS) {
                return;
            }
            foldingSince.set(0);
            log.info("식별자 축이 풀렸다 — {}초 동안 접은 채 {}건이 지나갔다",
                    Math.max(0, nowSec - since), foldedPassed.getAndSet(0));
        }
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
     * <b>큐에 안 넣는다.</b> 넣으면 공격자가 정상 사용자의 자리를 차지한다. 배수도 안 건다 —
     * 이 갈래는 판정보다 앞이라 홀더를 읽으면 요청 경로에 무관한 의존이 하나 늘고, 남용
     * 요청을 예산에 맞춰 배려할 이유도 없다.
     */
    private Mono<Void> reject(ServerWebExchange exchange, String kind) {
        meters.counter(METRIC, "key", kind).increment();
        return error.write(exchange, ApiError.Code.RATE_LIMITED,
                (int) POLL.intervalSec(EtaPolicy.UNKNOWN, random, PollIntervalPolicy.NO_SCALE));
    }
}

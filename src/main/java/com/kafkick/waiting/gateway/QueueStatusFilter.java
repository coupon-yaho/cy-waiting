package com.kafkick.waiting.gateway;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import com.kafkick.waiting.domain.queue.QueueEntry;
import com.kafkick.waiting.domain.queue.QueueState;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;
import reactor.core.publisher.Mono;

/**
 * <b>대상을 토큰으로 특정한다</b> — 회원 헤더로 고르면 헤더 하나 바꿔서 남의
 * 순번을 본다. 로그인이 없어 그 헤더는 위조 가능하다.
 */
@Component
@Order(FilterOrder.QUEUE_STATUS)
public final class QueueStatusFilter implements WebFilter {

    private static final PathPattern PATH = PathPatternParser.defaultInstance
            .parse("/api/v1/coupons/{couponId}/queue");

    private static final String TOKEN_PARAM = "queueToken";

    private static final String METRIC = "waiting.queue.status";

    /** 폴링 간격의 흔들림. 같은 밴드가 한꺼번에 두드리지 않게 한다. */
    private static final PollIntervalPolicy POLL = PollIntervalPolicy.of(0.2);

    /** 조회 예산의 키. <b>판정과 나눈다</b> — 폴링이 발급 예산을 갉아먹으면 안 된다. */
    private static final String POLL_KEY = "poll:";

    /**
     * 이 노드가 초당 받아 주는 조회 수.
     *
     * <p>동시 대기 20,000 이 폴링 간격 1초로 물으면 그만큼 온다. 노드 수로
     * 나눠야 맞지만 조회는 어느 노드로든 가므로, 한 노드가 전부 받는 최악을 둔다.
     */
    private long pollCap() {
        return MAX_POLL_PER_SEC;
    }

    private static final long MAX_POLL_PER_SEC = 20_000;

    private final SnapshotHolder holder;
    private final QueuePort queue;
    private final QueueToken tokens;
    private final EntryToken entryTokens;
    private final Clock clock;
    private final MeterRegistry meters;
    private final DoubleSupplier random;
    private final SecondWindowLimiter limiter;
    private final ApiError error;
    private final QueueResponse response = QueueResponse.create();

    private QueueStatusFilter(SnapshotHolder holder, QueuePort queue, QueueToken tokens,
            Clock clock, MeterRegistry meters, DoubleSupplier random,
            SecondWindowLimiter limiter, EntryToken entryTokens) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
        this.queue = Objects.requireNonNull(queue, "queue 는 필수다");
        this.tokens = Objects.requireNonNull(tokens, "tokens 는 필수다");
        this.entryTokens = Objects.requireNonNull(entryTokens, "entryTokens 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.random = Objects.requireNonNull(random, "random 은 필수다");
        this.limiter = Objects.requireNonNull(limiter, "limiter 는 필수다");
        this.error = ApiError.of(clock);
    }

    /** 흔들림의 난수원은 스레드마다 따로 둔다 — 공유하면 그 자체가 경합점이다. */
    @Autowired
    QueueStatusFilter(SnapshotHolder holder, QueuePort queue, QueueToken tokens,
            Clock clock, MeterRegistry meters, SecondWindowLimiter limiter,
            EntryToken entryTokens) {
        this(holder, queue, tokens, clock, meters,
                () -> ThreadLocalRandom.current().nextDouble(), limiter, entryTokens);
    }

    public static QueueStatusFilter of(SnapshotHolder holder, QueuePort queue,
            QueueToken tokens, Clock clock, MeterRegistry meters, SecondWindowLimiter limiter,
            EntryToken entryTokens) {
        return new QueueStatusFilter(holder, queue, tokens, clock, meters, limiter, entryTokens);
    }

    /** 난수원을 받는다. 고정하지 못하면 흔들림이 실제로 붙었는지 못 잰다 (TS-4). */
    public static QueueStatusFilter of(SnapshotHolder holder, QueuePort queue,
            QueueToken tokens, Clock clock, MeterRegistry meters, DoubleSupplier random,
            SecondWindowLimiter limiter, EntryToken entryTokens) {
        return new QueueStatusFilter(holder, queue, tokens, clock, meters, random, limiter,
                entryTokens);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        var vars = PATH.matchAndExtract(exchange.getRequest().getPath().pathWithinApplication());
        if (vars == null) {
            return chain.filter(exchange);
        }
        String couponId = vars.getUriVariables().get("couponId");
        Optional<String> member = tokens.verify(
                exchange.getRequest().getQueryParams().getFirst(TOKEN_PARAM),
                couponId, clock.instant());
        if (member.isEmpty()) {
            // **사유를 나누지 않는다.** 없는 토큰과 남의 토큰을 갈라 주면
            // 어느 쪽을 고쳐야 하는지 알려 주는 셈이다.
            count("no-token");
            return error.write(exchange, ApiError.Code.INVALID_REQUEST);
        }
        // **폴링은 읽기가 아니라 쓰기다.** 생존 신호를 갱신하고 차례가 오면 큐에서
        // 뺀다. 토큰은 줄을 서면 누구나 받고 한 시간 사니, 상한이 없으면 토큰 몇
        // 개로 공유 레디스에 무제한 쓰기를 넣을 수 있다.
        long nowSec = clock.instant().getEpochSecond();
        if (!limiter.tryAcquire(POLL_KEY, pollCap(), nowSec)) {
            count("rate-limited");
            return error.write(exchange, ApiError.Code.TEMPORARILY_UNAVAILABLE,
                    (int) POLL.intervalSec(EtaPolicy.UNKNOWN, random));
        }
        return queue.status(couponId, member.get(), clock.instant())
                .flatMap(entry -> answer(exchange, couponId, member.get(), entry))
                // 조회가 실패해도 순번은 레디스에 남는다. 다시 물으면 된다.
                .onErrorResume(e -> {
                    count("unavailable");
                    return error.write(exchange, ApiError.Code.TEMPORARILY_UNAVAILABLE,
                            (int) POLL.intervalSec(EtaPolicy.UNKNOWN, random));
                });
    }

    private Mono<Void> answer(ServerWebExchange exchange, String couponId, String memberId,
            QueueEntry entry) {
        count(entry.state().name());
        if (entry.state() == QueueState.NOT_QUEUED) {
            // 다시 오라고 하지 않는다. 끝난 사람을 부르는 것이 된다.
            return response.status(exchange, entry.state(), 0, 0, 0);
        }
        if (entry.state() == QueueState.ADMITTED) {
            // **여기서 발급한다** (지연 발급). 배분 때 미리 만들면 안 돌아온
            // 사람 몫이 그대로 버려지고, 그만큼 뒷사람이 늦게 들어간다.
            return response.admitted(exchange,
                    entryTokens.issue(couponId, memberId, clock.instant()), EntryToken.TTL_SEC);
        }
        double etaSec = EtaPolicy.etaSec(entry.rank(), credit(couponId));
        return response.status(exchange, entry.state(), entry.rank(),
                EtaPolicy.reportSec(etaSec), POLL.intervalSec(etaSec, random));
    }

    /** 배분 속도를 모르면 ETA 도 모른다. 모를수록 자주 묻게 하지 않는다. */
    private double credit(String couponId) {
        SnapshotHolder.View view = holder.view();
        CouponState state = view.snapshot().coupons().get(couponId);
        return state == null || holder.isDataStale(view)
                ? EtaPolicy.UNKNOWN
                : state.credit();
    }

    /** 쿠폰 식별자를 라벨에 안 넣는다. 인증이 없어 아무 문자열이나 들어온다. */
    private void count(String outcome) {
        meters.counter(METRIC, "outcome", outcome).increment();
    }
}

package com.kafkick.waiting.gateway;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.PollIntervalPolicy;
import com.kafkick.waiting.domain.queue.QueueEntry;
import com.kafkick.waiting.domain.queue.QueueState;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
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

    private final SnapshotHolder holder;
    private final QueuePort queue;
    private final QueueToken tokens;
    private final Clock clock;
    private final MeterRegistry meters;
    private final DoubleSupplier random;
    private final ApiError error;
    private final QueueResponse response = QueueResponse.create();

    private QueueStatusFilter(SnapshotHolder holder, QueuePort queue, QueueToken tokens,
            Clock clock, MeterRegistry meters, DoubleSupplier random) {
        this.holder = Objects.requireNonNull(holder, "holder 는 필수다");
        this.queue = Objects.requireNonNull(queue, "queue 는 필수다");
        this.tokens = Objects.requireNonNull(tokens, "tokens 는 필수다");
        this.clock = Objects.requireNonNull(clock, "clock 은 필수다");
        this.meters = Objects.requireNonNull(meters, "meters 는 필수다");
        this.random = Objects.requireNonNull(random, "random 은 필수다");
        this.error = ApiError.of(clock);
    }

    /** 흔들림의 난수원은 스레드마다 따로 둔다 — 공유하면 그 자체가 경합점이다. */
    @Autowired
    QueueStatusFilter(SnapshotHolder holder, QueuePort queue, QueueToken tokens,
            Clock clock, MeterRegistry meters) {
        this(holder, queue, tokens, clock, meters,
                () -> ThreadLocalRandom.current().nextDouble());
    }

    public static QueueStatusFilter of(SnapshotHolder holder, QueuePort queue,
            QueueToken tokens, Clock clock, MeterRegistry meters) {
        return new QueueStatusFilter(holder, queue, tokens, clock, meters);
    }

    /** 난수원을 받는다. 고정하지 못하면 흔들림이 실제로 붙었는지 못 잰다 (TS-4). */
    public static QueueStatusFilter of(SnapshotHolder holder, QueuePort queue,
            QueueToken tokens, Clock clock, MeterRegistry meters, DoubleSupplier random) {
        return new QueueStatusFilter(holder, queue, tokens, clock, meters, random);
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
        return queue.status(couponId, member.get(), clock.instant())
                .flatMap(entry -> answer(exchange, couponId, entry))
                // 조회가 실패해도 순번은 레디스에 남는다. 다시 물으면 된다.
                .onErrorResume(e -> {
                    count("unavailable");
                    return error.write(exchange, ApiError.Code.TEMPORARILY_UNAVAILABLE,
                            (int) POLL.intervalSec(EtaPolicy.UNKNOWN, random));
                });
    }

    private Mono<Void> answer(ServerWebExchange exchange, String couponId, QueueEntry entry) {
        count(entry.state().name());
        if (entry.state() == QueueState.NOT_QUEUED) {
            // 다시 오라고 하지 않는다. 끝난 사람을 부르는 것이 된다.
            return response.status(exchange, entry.state(), 0, 0, 0);
        }
        double etaSec = entry.state() == QueueState.ADMITTED
                ? 0
                : EtaPolicy.etaSec(entry.rank(), credit(couponId));
        return response.status(exchange, entry.state(), entry.rank(),
                (long) Math.max(0, etaSec), POLL.intervalSec(etaSec, random));
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

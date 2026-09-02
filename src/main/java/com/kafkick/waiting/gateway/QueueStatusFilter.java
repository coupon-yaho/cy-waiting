package com.kafkick.waiting.gateway;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.CouponState;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.EtaPolicy;
import com.kafkick.waiting.domain.queue.ErrorBackoff;
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
import org.springframework.http.server.reactive.ServerHttpRequest;
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

    /**
     * 순번 토큰을 싣는 헤더.
     *
     * <p><b>쿼리스트링으로 받지 않는다.</b> 앞단 프록시 액세스 로그에 URL 이
     * 그대로 남고, 그 한 줄이면 남의 차례를 통째로 가로챈다 — 페이로드에
     * `memberId` 가 평문이라 발급까지 이어진다.
     */
    private static final String TOKEN_HEADER = "Queue-Token";

    /** 한 릴리스만 받는 옛 자리. 다음 릴리스에서 뗀다. */
    private static final String TOKEN_PARAM = "queueToken";

    private static final String METRIC = "waiting.queue.status";

    /** 폴링 간격의 흔들림. 같은 밴드가 한꺼번에 두드리지 않게 한다. */
    private static final PollIntervalPolicy POLL =
            PollIntervalPolicy.of(PollIntervalPolicy.NORMAL_JITTER_RATIO);

    /**
     * 오류 경로의 안내 (F7). <b>정상 경로와 다른 정책이다.</b>
     *
     * <p>정상 경로는 사람마다 폴링 시점이 이미 흩어져 있다. 오류는 전원이 같은
     * 초에 받으므로 더 넓게 흩고, 장애가 이어지면 더 멀리 보낸다.
     */
    private static final ErrorBackoff BACKOFF = ErrorBackoff.defaults();

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

    /** 실패가 이어진 시간. 요청 수로 세면 피크에서 밀리초 만에 상한에 닿는다. */
    private final FailureAge failing = new FailureAge();

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
                tokenOf(exchange.getRequest()), couponId, clock.instant());
        if (member.isEmpty()) {
            // **사유를 나누지 않는다.** 없는 토큰과 남의 토큰을 갈라 주면
            // 어느 쪽을 고쳐야 하는지 알려 주는 셈이다.
            count("no-token");
            return error.write(exchange, ApiError.Code.INVALID_REQUEST);
        }
        // **매진이면 줄을 안 친다** (R3 · 7.1.4). 재고가 없으면 답이 정해져
        // 있는데, 그런데도 물으러 가면 매진 순간 몰리는 폴링이 그대로 레디스
        // 부하가 된다 — 정작 그때 줄을 정리해야 한다.
        //
        // **조회 상한보다 앞이다.** 상한은 노드 전역 키 하나라 쿠폰별 격리가
        // 없다. 뒤에 두면 죽은 쿠폰의 폴링이 살아 있는 쿠폰의 예산을 먹고, 매진
        // 폴러 자신도 상한에 걸려 `Retry-After` 가 붙은 503 을 받는다 — 이
        // 변경이 없애려던 폴링 재생산이 정확히 그 경로로 돌아온다.
        //
        // 상한을 안 써도 되는 것은 여기서 레디스를 안 치기 때문이다. 남용은
        // 앞단의 주소·회원 상한이 이미 막는다.
        if (soldOut(couponId)) {
            count("sold-out");
            return response.soldOut(exchange);
        }
        // **폴링은 읽기가 아니라 쓰기다.** 생존 신호를 갱신하고 차례가 오면 큐에서
        // 뺀다. 토큰은 줄을 서면 누구나 받고 한 시간 사니, 상한이 없으면 토큰 몇
        // 개로 공유 레디스에 무제한 쓰기를 넣을 수 있다.
        long nowSec = clock.instant().getEpochSecond();
        if (!limiter.tryAcquire(POLL_KEY, pollCap(), nowSec)) {
            count("rate-limited");
            // **여기야말로 배수를 걸어야 한다.** 거절만 배수를 빼면 과부하일수록
            // 거절 비중이 커져, 예산을 건다는 말이 절반만 맞다.
            //
            // 다만 MAX_POLL_PER_SEC 은 PollBudgetPlanner 의 노드당 예산과 다른
            // 값이다. 배수는 이 갈래가 돌기 한참 전에 걸리므로, 여기 오는 것은
            // 예산을 넘긴 정도가 아니라 노드가 통째로 밀린 상황이다 (CY-728).
            return error.write(exchange, ApiError.Code.TEMPORARILY_UNAVAILABLE,
                    (int) POLL.intervalSec(EtaPolicy.UNKNOWN, random,
                            pollScale(holder.view())));
        }
        // **조회의 성패만 뒷단 장애로 센다.** 아래 flatMap 은 응답을 쓰는데,
        // 거기서 나는 오류는 클라이언트가 끊은 것이다. 그것까지 세면 남이 끊은
        // 일이 뒷단 장애 타이머를 올려, 그다음 진짜 장애의 단계가 이미 높다.
        return queue.status(couponId, member.get(), clock.instant())
                // **한 번으로는 안 푼다.** 샤드 하나가 죽으면 일부만 실패하는데,
                // 성공마다 풀면 그 사이에 성공이 끼어 백오프가 영원히 안 걸린다.
                .doOnNext(ignored -> failing.cleared(clock.instant(), ErrorBackoff.quiet()))
                .doOnError(e -> failing.failed(clock.instant()))
                .flatMap(entry -> answer(exchange, couponId, member.get(), entry))
                // 조회가 실패해도 순번은 레디스에 남는다. 다시 물으면 된다.
                .onErrorResume(e -> {
                    // **무엇이 실패했는지는 남긴다.** 라벨이 하나면 레디스가 끊긴
                    // 것과 역직렬화가 깨진 것이 같은 수치로 보여, 대응이 갈린다.
                    //
                    // **밖에서 온 이름을 그대로 안 쓴다.** 여기 올라오는 것은
                    // 레티스·네티·리액터의 클래스명이고 익명 클래스면 빈
                    // 문자열이다 — 라벨 값 집합을 우리가 안 소유하게 된다.
                    count("unavailable", FailureCause.of(e));
                    return error.write(exchange, ApiError.Code.TEMPORARILY_UNAVAILABLE,
                            backoffSec());
                });
    }

    /**
     * 오류에 실어 보낼 초 (F7).
     *
     * <p>정상 경로의 밴드를 그대로 쓰면 안 된다. 오류는 전원이 같은 초에 받으므로
     * 같은 폭으로는 안 흩어지고, 장애가 이어지는 동안 같은 간격으로 계속
     * 두드리면 회복하려는 뒷단의 자리를 그 요청들이 계속 차지한다.
     */
    // **예산이 정한 바닥을 함께 넘긴다.** 장애 구간이 곧 배수가 커져 있는
    // 구간이라, 무시하면 하필 그때 거절받은 사람만 예산 밖으로 돌아온다.
    private int backoffSec() {
        int step = failing.stepAt(clock.instant(), ErrorBackoff.step());
        long floor = POLL.intervalSec(EtaPolicy.UNKNOWN, () -> 0.5, pollScale(holder.view()));
        return (int) BACKOFF.retryAfterSec(step, floor, random);
    }

    /** 잘못 말하면 기다리던 사람이 줄을 잃으므로, 모르는 것을 끝난 것으로 안 읽는다. */
    private boolean soldOut(String couponId) {
        // **매진 관찰 캐시는 안 본다** (계획 7.2 5.2.1). 그 관찰은 발급을
        // 시도했다가 거절당한 사실이고, 줄 선 사람에게 "네 차례에 못 받는다" 를
        // 뜻하지 않는다. 여기서 읽으면 관찰 하나가 5만 명의 줄을 끊는다.
        SnapshotHolder.View view = holder.view();
        // 첫 틱 전은 지금 `isDataStale` 이 **먼저** 참이 된다 — 재료가 없으면
        // 나이가 거대해지기 때문이다. 그래도 남긴 것은 둘의 뜻이 다르고, 낡음
        // 기준이 바뀌면 갈라지기 때문이다.
        if (view.isBeforeFirstTick() || holder.isDataStale(view)) {
            return false;
        }
        CouponState state = view.snapshot().coupons().get(couponId);
        // **발급 판정과 같은 함수를 부른다.** 여기서 재고를 다시 해석하면 판정이
        // 두 곳에 생기고, 같은 쿠폰에 조회는 "다시 서라" 등록은 409 로 답하는
        // 순간이 생긴다.
        return state != null && state.soldOut();
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
        // **한 View 에서 둘 다 뽑는다.** 따로 읽으면 그 사이 갱신이 들어와
        // ETA 는 회차 N, 배수는 회차 N+1 에서 나온다 — SnapshotHolder 가 View 를
        // 두는 이유가 그것이다.
        SnapshotHolder.View view = holder.view();
        double etaSec = EtaPolicy.etaSec(entry.rank(), credit(view, couponId));
        return response.status(exchange, entry.state(), entry.rank(),
                EtaPolicy.reportSec(etaSec),
                POLL.intervalSec(etaSec, random, pollScale(view)));
    }

    /**
     * 제어 평면이 정한 전역 폴링 배수.
     *
     * <p><b>낡았다고 1.0 으로 안 되돌린다.</b> 되돌리면 제어 평면이 멎은 순간
     * 전원의 간격이 한꺼번에 짧아진다 — 이미 흔들리는 노드에 폴링이 몰린다.
     */
    // 전역 값이라 쿠폰이 스냅샷에서 빠져도 남는다. 쿠폰별 필드에 두면 그
    // 쿠폰이 떨어지는 순간 그 줄 전체가 예산 밖으로 나갔다.
    private double pollScale(SnapshotHolder.View view) {
        return view.snapshot().meta().pollScale();
    }

    /** 배분 속도를 모르면 ETA 도 모른다. 모를수록 자주 묻게 하지 않는다. */
    private double credit(SnapshotHolder.View view, String couponId) {
        CouponState state = view.snapshot().coupons().get(couponId);
        return state == null || holder.isDataStale(view)
                ? EtaPolicy.UNKNOWN
                : state.credit();
    }

    /** 헤더가 먼저다. 쿼리는 옛 클라이언트를 위한 한 릴리스짜리 폴백이다. */
    private String tokenOf(ServerHttpRequest request) {
        String header = request.getHeaders().getFirst(TOKEN_HEADER);
        return header != null ? header : request.getQueryParams().getFirst(TOKEN_PARAM);
    }

    /** 쿠폰 식별자를 라벨에 안 넣는다. 인증이 없어 아무 문자열이나 들어온다. */
    private void count(String outcome) {
        count(outcome, FailureCause.NONE);
    }

    /**
     * <b>태그 키 집합을 늘 같게 둔다.</b> 같은 이름에 키 집합이 둘이면
     * 프로메테우스 레지스트리가 등록을 거절한다 — 지금은 단순 레지스트리라 안
     * 터지고, 6.5 에서 붙이는 순간 터진다.
     */
    private void count(String outcome, String cause) {
        meters.counter(METRIC, "outcome", outcome, "cause", cause).increment();
    }
}

package com.kafkick.waiting.gateway;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * fail-open 구간을 <b>진입·해제 쌍으로</b> 남긴다 (LG-2).
 *
 * <p>이 전이가 로그에 없으면 사후에 <b>추월이 언제 열렸는지</b>를 못 짚는다.
 * 지표는 초 단위로 뭉개져 남고 보존 기간도 짧아, 사고 조사에서 정작 필요한
 * "몇 시 몇 분에 열려 얼마나 갔는가" 를 답하지 못한다.
 */
class AdmissionFailOpenLogTest {

    private static final String COUPON = "c1";

    private static final Instant 지금 = Instant.parse("2026-08-24T00:00:00Z");

    private final MeterRegistry meters = new SimpleMeterRegistry();

    private final SnapshotHolder holder = SnapshotHolder.of(
            Duration.ofSeconds(3), Duration.ofSeconds(10), Clock.fixed(지금, ZoneOffset.UTC));

    private final FakeQueuePort 줄 = FakeQueuePort.create();

    private final SecondWindowLimiter limiter = SecondWindowLimiter.withMaxKeys(10_000);

    /** 구간 시계. 고정하지 못하면 지속 시간 계산이 시험에서 늘 0 이다 (TS-4). */
    private final AtomicLong 나노 = new AtomicLong();

    private final AdmissionGatewayFilter filter = AdmissionGatewayFilter.withIsolatedSoldOutCache(
            holder, AdmissionDecider.of(limiter, 0.7),
            Clock.fixed(지금, ZoneOffset.UTC), meters, () -> 0.5, 줄,
            QueueToken.of("not-a-real-secret-0123456789abcdef"), limiter,
            EntryToken.of("not-a-real-secret-0123456789abcdef"),
            IdempotencyKey.passThrough(), 나노::get);

    private ListAppender<ILoggingEvent> 로그;
    private Level 원래_수준;

    private static ch.qos.logback.classic.Logger 로거() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger(AdmissionGatewayFilter.class);
    }

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        원래_수준 = 로거().getLevel();
        로거().setLevel(Level.DEBUG);
        로거().addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        로거().detachAppender(로그);
        로거().setLevel(원래_수준);
    }

    private List<ILoggingEvent> 남은것(String 조각) {
        return 로그.list.stream().filter(e -> e.getMessage().contains(조각)).toList();
    }

    private void 태운다(String memberId) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST,
                                "/api/v1/coupons/" + COUPON + "/issue")
                        .header("X-Member-Id", memberId));
        exchange.getAttributes().put(
                ServerWebExchangeUtils.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("couponId", COUPON));
        filter.filter(exchange, e -> Mono.empty()).block();
    }

    private void 스냅샷을_심는다() {
        holder.replace(new GatewaySnapshot(
                Map.of(COUPON, CouponStates.queueing(10, 1_000, 5_000)),
                new SnapshotMeta(1_000, 1), 지금));
    }

    /**
     * <b>매 요청 찍지 않는다.</b> 그러면 정작 조사가 필요한 순간에 묻히고, 장애
     * 구간의 로그 자체가 2차 부하가 된다 (LG-3).
     */
    @Test
    @DisplayName("fail_open_진입을_한_번만_남긴다")
    void fail_open_진입을_한_번만_남긴다() {
        스냅샷을_심는다();
        줄.터진다(new IllegalStateException("레디스가 죽었다"));

        태운다("사람1");
        태운다("사람2");
        태운다("사람3");

        assertThat(남은것("fail-open 진입")).hasSize(1);
    }

    /**
     * <b>해제가 없으면 진입만 남는다.</b> 그 로그만 보면 구간이 아직 열려 있는지
     * 닫혔는지 알 수 없고, 지속 시간도 못 잰다.
     */
    @Test
    @DisplayName("걷히면_지속_시간과_건수를_남긴다")
    void 걷히면_지속_시간과_건수를_남긴다() {
        스냅샷을_심는다();
        줄.터진다(new IllegalStateException("레디스가 죽었다"));
        태운다("사람1");
        태운다("사람2");

        // **구간을 실제로 흘린다.** 1초 미만이면 어떤 값이든 0 으로 잘려, 단위를
        // 틀리거나 부호를 뒤집어도 통과한다.
        나노.set(SECONDS.toNanos(7));
        줄.나았다();
        태운다("사람3");

        List<ILoggingEvent> 해제 = 남은것("fail-open 해제");
        assertThat(해제).hasSize(1);
        // **인자까지 본다.** 건수를 안 보면 삼킨 수를 0 으로 적어도 통과한다.
        assertThat(해제.getFirst().getArgumentArray()).containsExactly(7L, 2);
    }

    /** 장애가 없으면 아무것도 안 남는다. 늘 찍히면 전이 로그가 아니다. */
    @Test
    @DisplayName("정상_구간에는_전이를_안_남긴다")
    void 정상_구간에는_전이를_안_남긴다() {
        스냅샷을_심는다();

        태운다("사람1");

        assertThat(남은것("fail-open")).isEmpty();
    }

    /**
     * <b>등록 실패의 사유 라벨에 밖의 이름이 안 들어간다.</b> 판정 지표는 상태
     * 폴링보다 트래픽이 훨씬 많아, 값이 새면 여기서 먼저 터진다.
     */
    @Test
    @DisplayName("등록_실패_사유가_유계다")
    void 등록_실패_사유가_유계다() {
        스냅샷을_심는다();
        줄.터진다(new UnsupportedOperationException(COUPON));

        태운다("사람1");

        assertThat(meters.getMeters())
                .filteredOn(m -> "enqueue-error".equals(m.getId().getTag("outcome")))
                .singleElement()
                .satisfies(m -> assertThat(m.getId().getTags())
                        .containsExactly(Tag.of("cause", "io"),
                                Tag.of("outcome", "enqueue-error")));
    }
}

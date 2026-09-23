package com.kafkick.waiting.chaos;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.kafkick.waiting.control.GatewayRegistry;
import com.kafkick.waiting.control.GatewaySnapshot;
import com.kafkick.waiting.control.SnapshotCodec;
import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.control.SnapshotRefresher;
import com.kafkick.waiting.control.SnapshotSource;
import com.kafkick.waiting.domain.allocation.CreditSmoother;
import com.kafkick.waiting.domain.allocation.QueueingHysteresis;
import com.kafkick.waiting.domain.coupon.CouponStates;
import com.kafkick.waiting.domain.coupon.SnapshotMeta;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

/**
 * C6e 실배선 — 돌아온 노드는 받은 분모를 제 하트비트가 방금 센 값 아래로 안 든다.
 *
 * <p>틱 하나짜리 경합은 실시간으로 못 가른다. 여기서 재는 것은 사슬이 스프링 안에서 이어졌는가다 — 실제 하트비트가
 * 레디스에서 센 값이 실제 갱신 루프를 거쳐 판정이 읽는 홀더에 닿는다. 발행은 스케줄러를 끄고 한 노드로 고정한다.
 */
@Tag("chaos")
@SpringBootTest(properties = "waiting.scheduler.enabled=false")
@Import(ReturningNodeFloorWireScenarioTest.OneNodePublished.class)
class ReturningNodeFloorWireScenarioTest {

    private static final String COUPON = "c6e-wire";

    /** 발행 표시. 나이는 이 시험이 안 본다 — 받아들여지기만 하면 된다. */
    private static final Instant 발행_시각 = Instant.parse("2026-09-24T00:00:00Z");

    /** 가짜 동료의 시각을 레디스 시계에 맞출 때 쓰는 벽시계. */
    private static final Clock 벽시계 = Clock.systemUTC();

    /** 하트비트 주기의 몇 배. 한 번 놓쳐도 넘길 만큼 넉넉하다. */
    private static final Duration 기다림 = Duration.ofSeconds(15);

    private static final List<String> 가짜_노드 = List.of("c6e-fake-1", "c6e-fake-2");

    private static RedisFaults faults;

    @DynamicPropertySource
    static void 배선(DynamicPropertyRegistry registry) {
        faults = RedisFaults.시작한다();
        registry.add("spring.data.redis.url", faults::주소);
    }

    @AfterAll
    static void 내린다() {
        if (faults != null) {
            faults.close();
        }
    }

    /** 리더가 한 노드만 셌다고 발행한 재료. 스케줄러가 꺼져 있어 이 값이 안 바뀐다. */
    @TestConfiguration
    static class OneNodePublished {

        @Bean
        @Primary
        SnapshotSource 한_노드로_발행된_재료() {
            Map<String, String> 재료 = SnapshotCodec.create().encode(
                    new GatewaySnapshot(Map.of(COUPON, CouponStates.idle(1_000_000)),
                            new SnapshotMeta(9_000, 1), 발행_시각),
                    CreditSmoother.Snapshot.empty(), QueueingHysteresis.Snapshot.empty());
            return () -> Mono.just(재료);
        }
    }

    @Autowired
    private GatewayRegistry 등록부;

    @Autowired
    private SnapshotHolder holder;

    @Test
    @DisplayName("C6e_실배선에서_돌아온_노드가_제_관측으로_분모를_든다")
    void C6e_실배선에서_돌아온_노드가_제_관측으로_분모를_든다() {
        StatefulRedisConnection<String, String> 연결 = faults.연결한다();
        // 스크립트는 레디스 시계로 잰다. 밀리초까지 맞춰야 가짜 노드가 미래로 읽혀 정리되지 않는다.
        List<String> 레디스_지금 = 연결.sync().time();
        long 레디스_밀리 = Long.parseLong(레디스_지금.get(0)) * 1_000 + Long.parseLong(레디스_지금.get(1)) / 1_000;
        Clock 레디스_시계 = Clock.offset(벽시계, Duration.ofMillis(레디스_밀리 - 벽시계.millis()));
        GatewayNodes 노드들 = new GatewayNodes(연결, Duration.ofSeconds(3), 레디스_시계);
        ScheduledExecutorService 치기 = Executors.newSingleThreadScheduledExecutor();
        Logger 로거 = (Logger) LoggerFactory.getLogger(SnapshotRefresher.class);
        ListAppender<ILoggingEvent> 로그 = new ListAppender<>();
        로그.start();
        로거.addAppender(로그);
        int[] 평시 = new int[2];
        int[] 돌아온 = new int[2];
        int[] 걷은 = new int[2];
        int[] 해제_직후 = new int[1];
        try {
            ChaosScenario.named("C6e 실배선 분모 바닥")
                    .baseline(() -> {
                        잡는다("혼자 센다", 평시, 1);
                    })
                    // 동료 둘이 돌아온다. 발행은 여전히 한 노드다 — 리더가 아직 안 센 틱이 계속되는 셈이다.
                    .inject(() -> 치기.scheduleAtFixedRate(
                            () -> 가짜_노드.forEach(노드들::등록한다), 0, 500, TimeUnit.MILLISECONDS))
                    .duringFault(() -> {
                        잡는다("하트비트가 셋을 세고 홀더가 셋을 든다", 돌아온, 3);
                    })
                    .recover(() -> {
                        // 진행 중인 등록이 해제 뒤에 닿지 않게 끝나기를 기다린다.
                        치기.shutdownNow();
                        try {
                            치기.awaitTermination(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        가짜_노드.forEach(노드들::해제한다);
                        해제_직후[0] = 노드들.살아있는_수();
                    })
                    .afterRecovery(() -> {
                        잡는다("다시 혼자 센다", 걷은, 1);
                    })
                    // 값은 대기 안에서 잡는다. 대기가 끝난 뒤 다시 읽으면 그 사이 흔들림에 걸린다.
                    .assertEntry(() -> RecoveryCriteria.violations(
                            같다("평시 관측·든 분모", 평시, 1, 1)))
                    .assertDuring(() -> RecoveryCriteria.violations(
                            같다("돌아온 뒤 관측·든 분모", 돌아온, 3, 3),
                            남았다(로그, "받은 분모 1 를 제 관측 3 으로 올려 든다", "올림 구간 진입")))
                    .assertRecovery(() -> RecoveryCriteria.violations(
                            // 해제가 실제로 지웠는가. 안 지워도 정리가 곧 걷어 가 아래 대기는 통과한다.
                            해제_직후[0] == 1 ? Optional.empty()
                                    : Optional.of("해제 직후 살아 있는 수가 %d — 1 이어야 한다".formatted(해제_직후[0])),
                            같다("걷은 뒤 관측·든 분모", 걷은, 1, 1),
                            남았다(로그, "받은 분모를 그대로 든다", "올림 구간 해제")))
                    .run();
        } finally {
            치기.shutdownNow();
            로거.detachAppender(로그);
            연결.close();
        }
    }

    /** 관측과 든 분모가 함께 {@code 기대} 인 순간을 잡아 둔다. */
    private void 잡는다(String 무엇, int[] 담을_곳, int 기대) {
        Awaitility.await().alias(무엇).atMost(기다림).until(() -> {
            담을_곳[0] = 등록부.seenNow();
            담을_곳[1] = 든_분모();
            return 담을_곳[0] == 기대 && 담을_곳[1] == 기대;
        });
    }

    /** 갱신 스레드가 쓰는 중에 읽지 않게 붙이는 쪽의 잠금 아래서 떠 온다. */
    private static Optional<String> 남았다(ListAppender<ILoggingEvent> 로그, String 앞, String 이름) {
        List<ILoggingEvent> 뜬_것;
        synchronized (로그) {
            뜬_것 = List.copyOf(로그.list);
        }
        return 뜬_것.stream().map(ILoggingEvent::getFormattedMessage).anyMatch(줄 -> 줄.startsWith(앞))
                ? Optional.empty() : Optional.of(이름 + " 로그가 없다");
    }

    private int 든_분모() {
        return holder.current().meta().gatewayCount();
    }

    private static Optional<String> 같다(String 이름, int[] 실제, int 관측, int 분모) {
        return 실제[0] == 관측 && 실제[1] == 분모 ? Optional.empty()
                : Optional.of("%s 가 %d·%d — %d·%d 여야 한다".formatted(이름, 실제[0], 실제[1], 관측, 분모));
    }
}

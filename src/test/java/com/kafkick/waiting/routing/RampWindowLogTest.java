package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import reactor.core.publisher.Flux;

/**
 * 되돌리는 구간의 진입과 해제를 쌍으로 남긴다 (LG-2).
 *
 * <p><b>배제 해제 줄은 "뺀 대가 없어졌다" 까지만 말한다.</b> 그 뒤 60초 동안 그 대가
 * 제 몫을 덜 받는 구간이 이어지는데, 로그로는 그 구간이 시작도 끝도 안 보였다.
 */
@Tag("unit")
class RampWindowLogTest {

    private static final long 시작 = 1_800_000_000_000L;

    private static final Duration 배제_시간 = Duration.ofSeconds(10);

    private static final Duration 램프 = Duration.ofSeconds(60);

    private final AtomicLong 시계 = new AtomicLong(시작);

    private final InstanceOutliers 배제기 = InstanceOutliers.of(3, 배제_시간, 램프);

    private ListAppender<ILoggingEvent> 로그;

    private Level 원래_수준;

    private Logger 로거() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger(CapacityAwareLoadBalancer.class);
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

    /** 살아 있는 대들. 시험이 중간에 빼면 다음 회차부터 안 보인다. */
    private final List<ServiceInstance> 산_대들 = new ArrayList<>();

    private CapacityAwareLoadBalancer 균형기() {
        List<ServiceInstance> instances = 산_대들;
        for (int i = 0; i < 4; i++) {
            instances.add(new DefaultServiceInstance("be-" + i, "coupon-service",
                    "10.0.1." + i, 8080, false));
        }
        instances.forEach(i -> i.getMetadata()
                .put(SnapshotInstanceListSupplier.CREDITS, "100"));
        ServiceInstanceListSupplier 목록 = new ServiceInstanceListSupplier() {
            @Override
            public String getServiceId() {
                return "coupon-service";
            }

            @Override
            public Flux<List<ServiceInstance>> get() {
                return Flux.just(List.copyOf(instances));
            }
        };
        return CapacityAwareLoadBalancer.of(목록,
                candidates -> candidates.stream().findFirst(),
                InFlightRegistry.of(Duration.ofSeconds(30)), 배제기,
                시계::get, 1_000);
    }

    private List<String> 줄들() {
        return 로그.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    private List<String> 줄들(Level 수준) {
        return 로그.list.stream()
                .filter(e -> e.getLevel() == 수준)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    @Test
    @DisplayName("되돌리는_구간의_진입과_해제를_남긴다")
    void 되돌리는_구간의_진입과_해제를_남긴다() {
        CapacityAwareLoadBalancer 균형기 = 균형기();
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-0", 시작);
        }
        균형기.choose((Request<?>) null).block();

        시계.set(시작 + 배제_시간.toMillis() + 램프.toMillis() / 2);
        균형기.choose((Request<?>) null).block();
        // **구간의 첫 회차만 찍는다.** 요청마다 찍으면 초당 수천 줄이 쌓인다 (LG-2).
        균형기.choose((Request<?>) null).block();

        assertThat(줄들(Level.INFO)).filteredOn(m -> m.contains("되돌아오는 중"))
                .singleElement().asString()
                .as("몇 대가 되돌아오는지가 든다").contains("1 대");

        시계.set(시작 + 배제_시간.toMillis() + 램프.toMillis());
        균형기.choose((Request<?>) null).block();

        assertThat(줄들(Level.INFO)).filteredOn(m -> m.contains("되돌리기가 끝났다"))
                .as("해제가 없으면 그 구간이 아직 도는지 알 수 없다").hasSize(1);
    }

    /**
     * <b>구간이 닫혔다고 회복한 것이 아니다.</b> 되돌리다 다시 빠지면 되돌리는 대가
     * 0 이 되어 같은 신호가 나는데, 그것을 완주라 부르면 맴도는 상태가 로그에서
     * 정상으로 읽힌다 — 이 구간을 남기는 이유가 정확히 그것을 가르는 것이다.
     */
    @Test
    @DisplayName("되돌리다_다시_빠지면_완주라_안_한다")
    void 되돌리다_다시_빠지면_완주라_안_한다() {
        CapacityAwareLoadBalancer 균형기 = 균형기();
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-0", 시작);
        }
        시계.set(시작 + 배제_시간.toMillis() + 램프.toMillis() / 2);
        균형기.choose((Request<?>) null).block();

        for (int i = 0; i < 3; i++) {
            배제기.failed("be-0", 시계.get());
        }
        균형기.choose((Request<?>) null).block();

        assertThat(줄들()).noneMatch(m -> m.contains("되돌리기가 끝났다"));
        assertThat(줄들(Level.WARN))
                .as("목록에서 빠진 것과 다시 빠진 것은 다른 줄이다")
                .anyMatch(m -> m.contains("다시 빠졌다"));
    }

    /**
     * <b>한 대가 마쳤다고 구간이 성공은 아니다.</b> 둘 중 하나가 완주하고 하나가 다시
     * 빠져도 되돌리는 대는 0 이 된다 — 완주만 보고 가르면 그 회차가 정상으로 남는다.
     */
    @Test
    @DisplayName("완주와_재배제가_섞이면_성공이라_안_한다")
    void 완주와_재배제가_섞이면_성공이라_안_한다() {
        CapacityAwareLoadBalancer 균형기 = 균형기();
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-0", 시작);
            배제기.failed("be-2", 시작);
        }
        long 늦게 = 시작 + 배제_시간.toMillis();
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 늦게);
        }
        시계.set(시작 + 배제_시간.toMillis() + 램프.toMillis() / 2);
        균형기.choose((Request<?>) null).block();

        // be-0 과 be-2 는 여기서 램프를 마치고, be-1 은 아직 램프 중이라 다시 빠진다.
        시계.set(시작 + 배제_시간.toMillis() + 램프.toMillis());
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 시계.get());
        }
        균형기.choose((Request<?>) null).block();

        assertThat(줄들()).noneMatch(m -> m.contains("되돌리기가 끝났다"));
        assertThat(줄들(Level.WARN)).filteredOn(m -> m.contains("되돌리기가 안 끝났다"))
                .singleElement().asString()
                .as("다시 빠진 수와 완주한 수가 자리를 안 바꾼다")
                .contains("1 대가 다시 빠졌다").contains("완주 2 대");
    }

    /**
     * <b>되돌리던 대가 목록에서 빠져도 구간이 닫힌다.</b> 완주도 재배제도 아닌데
     * 되돌리는 대는 0 이 된다 — 롤링 배포마다 지나는 자리라 완주와 갈라야 한다.
     */
    @Test
    @DisplayName("되돌리던_대가_사라지면_완주라_안_한다")
    void 되돌리던_대가_사라지면_완주라_안_한다() {
        CapacityAwareLoadBalancer 균형기 = 균형기();
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-0", 시작);
        }
        시계.set(시작 + 배제_시간.toMillis() + 램프.toMillis() / 2);
        균형기.choose((Request<?>) null).block();

        산_대들.removeIf(i -> "be-0".equals(i.getInstanceId()));
        균형기.choose((Request<?>) null).block();

        assertThat(줄들()).noneMatch(m -> m.contains("되돌리기가 끝났다"))
                .noneMatch(m -> m.contains("다시 빠졌다"));
        assertThat(줄들(Level.WARN))
                .anyMatch(m -> m.contains("되돌리기가 안 끝났다"));
    }

    /** 앓은 대가 없으면 아무 말도 안 한다. 늘 시끄러우면 사람이 안 본다. */
    @Test
    @DisplayName("되돌릴_것이_없으면_안_남긴다")
    void 되돌릴_것이_없으면_안_남긴다() {
        균형기().choose((Request<?>) null).block();

        assertThat(줄들()).noneMatch(m -> m.contains("되돌아오는 중"))
                .noneMatch(m -> m.contains("되돌리기가 끝났다"));
    }
}

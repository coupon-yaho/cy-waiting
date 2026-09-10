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
import java.util.List;
import java.util.Optional;
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
 * 뺀 대를 도로 넣는 구간의 진입과 해제 (LG-2).
 *
 * <p><b>구간이 닫힌 것과 회복한 것은 다르다.</b> 뺀 대가 없어져도 보낼 곳이 여전히
 * 없으면 나아진 것이 아닌데, 그때 해제 줄이 나가면 기록이 반대로 남는다.
 */
@Tag("unit")
class CrowdedOutLogTest {

    private static final long 지금 = 1_800_000_000_000L;

    private static final int 상한 = 1;

    private final InFlightRegistry 레지스트리 = InFlightRegistry.of(Duration.ofSeconds(30));

    private final InstanceOutliers 배제기 =
            InstanceOutliers.of(3, Duration.ofSeconds(10), Duration.ofSeconds(60));

    private ListAppender<ILoggingEvent> 로그;

    private Logger 로거() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger(CapacityAwareLoadBalancer.class);
    }

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        로거().addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        로거().detachAppender(로그);
    }

    private ServiceInstance 인스턴스(String id, String credits) {
        DefaultServiceInstance instance =
                new DefaultServiceInstance(id, "coupon-service", "10.0.1.9", 8080, false);
        instance.getMetadata().put(SnapshotInstanceListSupplier.CREDITS, credits);
        return instance;
    }

    private CapacityAwareLoadBalancer 균형기(ServiceInstance... 대들) {
        ServiceInstanceListSupplier 목록 = new ServiceInstanceListSupplier() {
            @Override
            public String getServiceId() {
                return "coupon-service";
            }

            @Override
            public Flux<List<ServiceInstance>> get() {
                return Flux.just(List.of(대들));
            }
        };
        return CapacityAwareLoadBalancer.of(목록, candidates -> Optional.empty(),
                레지스트리, 배제기, () -> 지금, 상한);
    }

    private List<String> 줄들() {
        return 로그.list.stream()
                .filter(e -> e.getLevel() != Level.DEBUG)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    /**
     * <b>뺀 대가 없어져도 보낼 곳이 없으면 회복이 아니다.</b> 해제를 뺀 대의 유무로만
     * 가르면, 뒷단이 여전히 전부 포화인 채로 "보낼 곳이 남는다" 가 남는다 — 그 직후
     * 빈 답을 내면서다.
     */
    @Test
    @DisplayName("보낼_곳이_없는_채로_해제를_안_찍는다")
    void 보낼_곳이_없는_채로_해제를_안_찍는다() {
        CapacityAwareLoadBalancer 균형기 =
                균형기(인스턴스("be-1", "100"), 인스턴스("be-2", "0"));
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 지금);
        }
        균형기.choose((Request<?>) null).block();
        assertThat(줄들()).anyMatch(m -> m.contains("보낼 곳이 없다"));

        // 배제 창을 닫고, 그 대를 인스턴스별 상한으로 막는다. 뺀 대는 없어졌지만
        // 보낼 곳은 여전히 없다.
        for (int i = 0; i < 3; i++) {
            배제기.succeeded("be-1", 지금);
        }
        레지스트리.started("be-1", 지금);
        균형기.choose((Request<?>) null).block();

        assertThat(줄들()).noneMatch(m -> m.contains("보낼 곳이 남는다"));
    }

    /**
     * <b>로그는 구간의 첫 건만 남는다.</b> 그 구간이 몇 회차나 배제를 무시했는지는
     * 계수로만 보이고, 그 수가 배제 게이지를 견줄 대상이다.
     */
    @Test
    @DisplayName("배제가_무시된_회차를_센다")
    void 배제가_무시된_회차를_센다() {
        CapacityAwareLoadBalancer 균형기 =
                균형기(인스턴스("be-1", "100"), 인스턴스("be-2", "0"));
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 지금);
        }

        균형기.choose((Request<?>) null).block();
        균형기.choose((Request<?>) null).block();

        assertThat(배제기.ejectionsOverridden()).as("로그는 한 줄이어도 두 회차다")
                .isEqualTo(2);
        assertThat(줄들()).filteredOn(m -> m.contains("보낼 곳이 없다")).hasSize(1);
    }

    /** 진짜 회복은 남긴다. 안 남기면 구간이 언제 끝났는지가 없다. */
    @Test
    @DisplayName("보낼_곳이_생기면_해제를_찍는다")
    void 보낼_곳이_생기면_해제를_찍는다() {
        CapacityAwareLoadBalancer 균형기 =
                균형기(인스턴스("be-1", "100"), 인스턴스("be-2", "0"));
        for (int i = 0; i < 3; i++) {
            배제기.failed("be-1", 지금);
        }
        균형기.choose((Request<?>) null).block();

        for (int i = 0; i < 3; i++) {
            배제기.succeeded("be-1", 지금);
        }
        균형기.choose((Request<?>) null).block();

        assertThat(줄들()).anyMatch(m -> m.contains("보낼 곳이 남는다"));
    }
}

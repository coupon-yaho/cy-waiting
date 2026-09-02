package com.kafkick.waiting.routing;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.kafkick.waiting.domain.routing.InFlightRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
 * 인스턴스 수가 가정(A7) 밖이면 알린다 (9.3.9 · R-9).
 *
 * <p><b>자동으로 전략을 안 바꾼다.</b> 인스턴스 수로 전환하면 임계 근처에서
 * 진동하고 — 롤링 배포가 정확히 그 구간을 지난다 — 어느 구간이 무슨 모드였는지가
 * 대시보드에 안 남는다. 알리고 판단은 사람이 한다.
 */
@Tag("unit")
class InstanceCountWarningTest {

    private static final long 지금 = 1_800_000_000_000L;

    private ListAppender<ILoggingEvent> 로그;

    private Level 원래_수준;

    private static ch.qos.logback.classic.Logger 로거() {
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

    private static ServiceInstanceListSupplier 목록(int 대수) {
        List<ServiceInstance> instances = new ArrayList<>();
        for (int i = 0; i < 대수; i++) {
            instances.add(new DefaultServiceInstance("be-" + i, "coupon-service",
                    "10.0.1." + i, 8080, false));
        }
        instances.forEach(i -> i.getMetadata()
                .put(SnapshotInstanceListSupplier.CREDITS, "100"));
        return new ServiceInstanceListSupplier() {
            @Override
            public String getServiceId() {
                return "coupon-service";
            }

            @Override
            public Flux<List<ServiceInstance>> get() {
                return Flux.just(instances);
            }
        };
    }

    private CapacityAwareLoadBalancer 균형기(int 대수) {
        return 균형기(목록(대수));
    }

    private CapacityAwareLoadBalancer 균형기(ServiceInstanceListSupplier 목록) {
        return CapacityAwareLoadBalancer.of(목록,
                candidates -> candidates.stream().findFirst(),
                InFlightRegistry.of(Duration.ofSeconds(30)), () -> 지금, 1_000);
    }

    private List<String> 경고들() {
        return 로그.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    /** 3 대면 무작위 둘을 뽑는 이득이 없다. 라운드로빈을 권한다. */
    @Test
    @DisplayName("적으면_한_번_알린다")
    void 적으면_한_번_알린다() {
        CapacityAwareLoadBalancer 균형기 = 균형기(3);

        균형기.choose((Request<?>) null).block();
        균형기.choose((Request<?>) null).block();

        // **구간의 시작만 찍는다.** 요청마다 찍으면 초당 수천 줄이 쌓인다 (LG-2).
        assertThat(경고들()).singleElement().asString()
                .contains("3 대").contains("round-robin");
    }

    @Test
    @DisplayName("많으면_한_번_알린다")
    void 많으면_한_번_알린다() {
        균형기(50).choose((Request<?>) null).block();

        assertThat(경고들()).singleElement().asString().contains("50 대");
    }

    /** 가정 안이면 아무 말도 안 한다. 늘 시끄러우면 사람이 안 본다. */
    @Test
    @DisplayName("가정_안이면_안_알린다")
    void 가정_안이면_안_알린다() {
        균형기(15).choose((Request<?>) null).block();

        assertThat(경고들()).isEmpty();
    }

    /** 돌아오면 해제도 남긴다. 쌍으로 안 남기면 언제 걷혔는지가 없다 (LG-2). */
    @Test
    @DisplayName("돌아오면_해제를_남긴다")
    void 돌아오면_해제를_남긴다() {
        CapacityAwareLoadBalancer 균형기 = CapacityAwareLoadBalancer.of(
                번갈아_주는_목록(), candidates -> candidates.stream().findFirst(),
                InFlightRegistry.of(Duration.ofSeconds(30)), () -> 지금, 1_000);

        균형기.choose((Request<?>) null).block();
        균형기.choose((Request<?>) null).block();

        assertThat(로그.list.stream().map(ILoggingEvent::getFormattedMessage))
                .anyMatch(m -> m.contains("가정 안으로 돌아왔다"));
    }

    /**
     * <b>밖에서 밖으로 건너뛰어도 다시 알린다.</b>
     *
     * <p>하나로 묶어 세면 처방이 정반대가 됐는데 알람이 조용하고, 운영자는 처음
     * 받은 안내를 그대로 들고 있는다 — 대를 늘리라는 말과 줄이라는 말이 뒤바뀐다.
     */
    @Test
    @DisplayName("적음에서_많음으로_뒤집히면_다시_알린다")
    void 적음에서_많음으로_뒤집히면_다시_알린다() {
        CapacityAwareLoadBalancer 균형기 = 균형기(번갈아_주는_목록(3, 50));

        균형기.choose((Request<?>) null).block();
        균형기.choose((Request<?>) null).block();

        assertThat(경고들()).hasSize(2);
        assertThat(경고들().getLast()).asString().contains("50 대");
    }

    /** 첫 회차는 3 대, 다음 회차는 15 대. 대수가 바뀌는 것이 실제 배포의 모양이다. */
    private static ServiceInstanceListSupplier 번갈아_주는_목록() {
        return 번갈아_주는_목록(3, 15);
    }

    private static ServiceInstanceListSupplier 번갈아_주는_목록(int 첫째, int 둘째) {
        List<Integer> 대수 = new ArrayList<>(List.of(첫째, 둘째));
        return new ServiceInstanceListSupplier() {
            @Override
            public String getServiceId() {
                return "coupon-service";
            }

            @Override
            public Flux<List<ServiceInstance>> get() {
                return Flux.defer(() ->
                        목록(대수.isEmpty() ? 둘째 : 대수.remove(0)).get());
            }
        };
    }
}

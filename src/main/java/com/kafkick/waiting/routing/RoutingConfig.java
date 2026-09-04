package com.kafkick.waiting.routing;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceChooser;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import com.kafkick.waiting.domain.routing.WeightedP2c;
import com.kafkick.waiting.domain.routing.WeightedRoundRobin;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Random;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClientSpecification;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 가용량 비율 라우팅 배선 (Phase 9).
 *
 * <p><b>끄면 단일 주소로 돌아간다.</b> 라우팅이 의심스러우면 설정 한 줄로
 * 되돌린다 — 코드는 남아도 무해하다.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RoutingProperties.class)
@ConditionalOnProperty(prefix = "waiting.routing", name = "enabled", havingValue = "true")
public class RoutingConfig {

    /** 물린 표의 수명은 요청이 살아 있을 수 있는 최대 시간이다 (R-8). */
    @Bean
    InFlightRegistry inFlightRegistry(RoutingProperties properties) {
        return InFlightRegistry.of(properties.inFlightTtl());
    }

    /**
     * 연속으로 실패하는 인스턴스를 잠시 후보에서 뺀다.
     *
     * <p>서킷은 뒷단 전체에 하나라 열리면 다 같이 막힌다. 여기는 인스턴스별로
     * 보되 <b>전부를 빼지는 않는다</b> — 배제가 전면 차단이 되면 안 된다.
     */
    @Bean
    InstanceOutliers instanceOutliers(RoutingProperties properties) {
        return InstanceOutliers.of(properties.outlierFailures(), properties.outlierEjectFor());
    }

    /**
     * <b>두 전략을 다 만든다</b> (R-9). 3~5 대로 줄면 라운드로빈이 더 정확하고
     * 단순한데, 어느 쪽이 나은지는 실측으로 정할 문제다.
     */
    @Bean
    InstanceChooser instanceChooser(RoutingProperties properties) {
        if (RoutingProperties.ROUND_ROBIN.equals(properties.strategy())) {
            return WeightedRoundRobin.create();
        }
        // 난수 시드를 안 고정한다. 고정하면 노드들이 같은 순서로 뽑아,
        // P2C 를 고른 이유인 쏠림 회피가 사라진다.
        return WeightedP2c.of(new Random()::nextInt);
    }

    /**
     * 물려 있는 수를 지표로 낸다 (9.2.6).
     *
     * <p><b>누수는 값이 안 내려가는 것으로만 보인다.</b> 부하가 끝났는데 0 이
     * 아니면 감소를 어디선가 놓친 것이다 (G9.3).
     */
    @Bean
    InFlightMetrics.Binding inFlightMetrics(InFlightRegistry registry, MeterRegistry meters) {
        InFlightMetrics.bind(registry, System::currentTimeMillis, meters);
        return new InFlightMetrics.Binding();
    }

    /** 나간 요청을 세고 어느 경로로 끝나든 되돌린다 (G9.3). */
    @Bean
    InFlightTrackingFilter inFlightTrackingFilter(InFlightRegistry registry,
            InstanceOutliers outliers) {
        return InFlightTrackingFilter.of(registry, outliers, System::currentTimeMillis);
    }

    /** 이 서비스에만 우리 균형기를 건다. 다른 이름으로 가는 것은 기본 배선 그대로다. */
    @Bean
    LoadBalancerClientSpecification couponServiceLoadBalancer(RoutingProperties properties) {
        return new LoadBalancerClientSpecification(properties.serviceId(),
                new Class<?>[] {CouponServiceLoadBalancerConfig.class});
    }
}

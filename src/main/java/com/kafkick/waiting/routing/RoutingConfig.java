package com.kafkick.waiting.routing;

import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceChooser;
import com.kafkick.waiting.domain.routing.WeightedP2c;
import com.kafkick.waiting.domain.routing.WeightedRoundRobin;
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
     * <b>두 전략을 다 만든다</b> (R-9). 3~5 대로 줄면 라운드로빈이 더 정확하고
     * 단순한데, 어느 쪽이 나은지는 실측으로 정할 문제다.
     */
    @Bean
    InstanceChooser instanceChooser(RoutingProperties properties) {
        if (RoutingProperties.ROUND_ROBIN.equals(properties.strategy())) {
            return WeightedRoundRobin.create();
        }
        // 무작위 씨앗을 안 고정한다. 고정하면 노드들이 같은 순서로 뽑아,
        // P2C 를 고른 이유인 쏠림 회피가 사라진다.
        return WeightedP2c.of(new Random()::nextInt);
    }

    /** 나간 요청을 세고 어느 경로로 끝나든 되돌린다 (G9.3). */
    @Bean
    InFlightTrackingFilter inFlightTrackingFilter(InFlightRegistry registry) {
        return InFlightTrackingFilter.of(registry, System::currentTimeMillis);
    }

    /** 이 서비스에만 우리 균형기를 건다. 다른 이름으로 가는 것은 기본 배선 그대로다. */
    @Bean
    LoadBalancerClientSpecification couponServiceLoadBalancer(RoutingProperties properties) {
        return new LoadBalancerClientSpecification(properties.serviceId(),
                new Class<?>[] {CouponServiceLoadBalancerConfig.class});
    }
}

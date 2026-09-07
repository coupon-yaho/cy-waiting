package com.kafkick.waiting.routing;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.routing.AllowedDestinations;
import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceChooser;
import com.kafkick.waiting.domain.routing.InstanceOutliers;
import org.springframework.cloud.loadbalancer.core.ReactorServiceInstanceLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ServiceInstanceListSupplier;
import org.springframework.context.annotation.Bean;

/**
 * 뒷단 하나짜리 자식 컨텍스트.
 *
 * <p><b>{@code @Configuration} 을 안 붙인다.</b> 붙이면 컴포넌트 스캔이 이걸
 * 부모 컨텍스트에도 올려, 서비스마다 하나여야 할 빈이 전역으로 하나가 된다.
 */
public class CouponServiceLoadBalancerConfig {

    /**
     * 목록은 판정 재료에서 온다. <b>여기서 레디스를 읽으면 요청 경로가 레디스를 친다.</b>
     * 목적지 검사를 다시 거는 이유는 그 공급자의 주석에 있다.
     */
    @Bean
    ServiceInstanceListSupplier snapshotInstances(RoutingProperties properties,
            SnapshotHolder holder) {
        return SnapshotInstanceListSupplier.of(properties.serviceId(), holder,
                AllowedDestinations.of(properties.allowedDestinations(), properties.allowedPorts()));
    }

    @Bean
    ReactorServiceInstanceLoadBalancer capacityAwareLoadBalancer(
            ServiceInstanceListSupplier instances, InstanceChooser chooser,
            InFlightRegistry inFlight, InstanceOutliers outliers,
            RoutingProperties properties) {
        return CapacityAwareLoadBalancer.of(instances, chooser, inFlight, outliers,
                System::currentTimeMillis, properties.perInstanceCap());
    }
}

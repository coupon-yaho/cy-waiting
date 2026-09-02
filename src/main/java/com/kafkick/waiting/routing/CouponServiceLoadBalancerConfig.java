package com.kafkick.waiting.routing;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.routing.InFlightRegistry;
import com.kafkick.waiting.domain.routing.InstanceChooser;
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

    /** 목록은 판정 재료에서 온다. <b>여기서 레디스를 읽으면 불변식 1 이 깨진다.</b> */
    @Bean
    ServiceInstanceListSupplier snapshotInstances(RoutingProperties properties,
            SnapshotHolder holder) {
        return SnapshotInstanceListSupplier.of(properties.serviceId(), holder);
    }

    @Bean
    ReactorServiceInstanceLoadBalancer capacityAwareLoadBalancer(
            ServiceInstanceListSupplier instances, InstanceChooser chooser,
            InFlightRegistry inFlight) {
        return CapacityAwareLoadBalancer.of(instances, chooser, inFlight,
                System::currentTimeMillis);
    }
}

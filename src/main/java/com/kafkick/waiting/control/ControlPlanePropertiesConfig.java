package com.kafkick.waiting.control;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 시간 예산은 <b>배분 토글 밖</b>이다.
 *
 * <p>배분을 안 도는 노드도 리스와 샤드를 알아야 한다. 토글 뒤에 두면 그 노드가
 * 값을 못 찾고, 그러면 상수로 복제하게 된다 — 사본이 생기는 순간 검증기가 안
 * 보는 값이 실제로 쓰이는 값이 된다.
 */
@Configuration
public class ControlPlanePropertiesConfig {

    @Bean
    ControlPlaneProperties controlPlaneProperties() {
        return ControlPlaneProperties.defaults();
    }
}

package com.kafkick.waiting.gateway;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * 형식 검증을 <b>자기 자리에</b> 등록한다. 남의 설정에 얹어 두면 그쪽을 정리하는
 * 것만으로 검증이 조용히 사라진다.
 */
@Configuration
public class IdentityConfig {

    @Bean
    @Order(FilterOrder.IDENTITY)
    public MemberIdentityFilter memberIdentityFilter() {
        return MemberIdentityFilter.create();
    }
}

package com.kafkick.waiting.gateway;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * 형식 검증을 <b>자기 자리에</b> 등록한다. 남의 설정에 얹어 두면 그쪽을 정리하는
 * 것만으로 검증이 조용히 사라진다.
 */
@Configuration
public class IdentityConfig {

    /** 쿠폰 2,000개를 상정한 값. 넘으면 판정이 그 사실을 따로 알린다. */
    private static final int MAX_LIMITER_KEYS = 10_000;

    /** 한산한 쿠폰이 유휴 몫으로 쓸 수 있는 전역 크레딧 비율. */
    private static final double IDLE_CREDIT_RATIO = 0.2;

    @Bean
    @Order(FilterOrder.IDENTITY)
    public MemberIdentityFilter memberIdentityFilter() {
        return MemberIdentityFilter.of(Clock.systemUTC());
    }

    /**
     * 판정 필터. <b>라우트에 인스턴스로 붙는다</b> — 이름으로 적으면 안 풀렸을 때
     * 기동은 되고 판정만 사라진다.
     */
    @Bean
    public AdmissionGatewayFilter admissionGatewayFilter(SnapshotHolder holder,
            AdmissionDecider decider, MeterRegistry meters) {
        return AdmissionGatewayFilter.of(holder, decider, Clock.systemUTC(), meters);
    }

    /**
     * 리미터의 키 상한. <b>무제한이면 쿠폰 식별자를 바꿔가며 메모리를 밀어낼 수
     * 있다</b> — 상한을 넘긴 것은 예산 고갈과 다른 판정으로 나간다.
     */
    @Bean
    public AdmissionDecider admissionDecider() {
        return AdmissionDecider.of(SecondWindowLimiter.withMaxKeys(MAX_LIMITER_KEYS),
                IDLE_CREDIT_RATIO);
    }
}

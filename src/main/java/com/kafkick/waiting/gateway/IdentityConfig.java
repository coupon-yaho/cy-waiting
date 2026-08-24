package com.kafkick.waiting.gateway;

import com.kafkick.waiting.control.SnapshotHolder;
import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.queue.QueueToken;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * 형식 검증을 <b>자기 자리에</b> 등록한다. 남의 설정에 얹어 두면 그쪽을 정리하는
 * 것만으로 검증이 조용히 사라진다.
 */
@Configuration
@EnableConfigurationProperties(QueueTokenProperties.class)
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
            AdmissionDecider decider, MeterRegistry meters,
            QueuePort queue, QueueToken tokens) {
        return AdmissionGatewayFilter.of(holder, decider, Clock.systemUTC(), meters,
                queue, tokens);
    }

    /**
     * 순번 조회. <b>대상을 토큰으로 특정한다</b> — 회원 헤더로 고르면 헤더 하나
     * 바꿔서 남의 순번을 본다.
     */
    @Bean
    @Order(FilterOrder.QUEUE_STATUS)
    public QueueStatusFilter queueStatusFilter(SnapshotHolder holder, QueuePort queue,
            QueueToken tokens, MeterRegistry meters) {
        return QueueStatusFilter.of(holder, queue, tokens, Clock.systemUTC(), meters);
    }

    /** 비밀키가 없거나 짧으면 여기서 기동이 멎는다. 약한 키로 조용히 돌지 않는다. */
    @Bean
    public QueueToken queueToken(QueueTokenProperties properties) {
        return properties.queueToken();
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

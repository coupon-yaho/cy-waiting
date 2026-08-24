package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.queue.QueueToken;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 도메인 배선.
 *
 * <p><b>필터는 스스로 선다.</b> 여기 남은 둘은 도메인이라 스프링을 못 참조한다
 * (DS-1) — 값을 주고 만들어 주는 자리가 필요하다.
 */
@Configuration
@EnableConfigurationProperties(QueueTokenProperties.class)
public class IdentityConfig {

    /** 쿠폰 2,000개를 상정한 값. 넘으면 판정이 그 사실을 따로 알린다. */
    private static final int MAX_LIMITER_KEYS = 10_000;

    /** 한산한 쿠폰이 유휴 몫으로 쓸 수 있는 전역 크레딧 비율. */
    private static final double IDLE_CREDIT_RATIO = 0.2;


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
    public AdmissionDecider admissionDecider(SecondWindowLimiter limiter) {
        return AdmissionDecider.of(limiter, IDLE_CREDIT_RATIO);
    }

    /**
     * <b>리미터는 하나다.</b> 판정과 장애 개방이 각자 들면 한 초에 두 예산이
     * 겹쳐 나가고, 경로를 나누지 말라는 규칙이 막으려던 버스트가 그대로 난다.
     */
    @Bean
    public SecondWindowLimiter admissionLimiter() {
        return SecondWindowLimiter.withMaxKeys(MAX_LIMITER_KEYS);
    }
}

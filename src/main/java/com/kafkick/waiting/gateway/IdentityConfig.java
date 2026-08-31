package com.kafkick.waiting.gateway;

import com.kafkick.waiting.domain.admission.AdmissionDecider;
import com.kafkick.waiting.domain.admission.CouponKeys;
import com.kafkick.waiting.domain.admission.SecondWindowLimiter;
import com.kafkick.waiting.domain.queue.EntryToken;
import com.kafkick.waiting.domain.queue.QueueToken;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import java.time.Clock;
import com.kafkick.waiting.control.SnapshotHolder;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 도메인 배선.
 *
 * <p><b>필터는 스스로 선다.</b> 여기 남은 둘은 도메인이라 스프링을 못 참조한다
 * (DS-1) — 값을 주고 만들어 주는 자리가 필요하다.
 */
@Configuration
@EnableConfigurationProperties({QueueTokenProperties.class, ProxyProperties.class,
        CoalescingProperties.class, SoldOutCacheProperties.class})
public class IdentityConfig {

    /**
     * 한산한 쿠폰이 쓸 수 있는 노드 예산 비율 (B-13).
     *
     * <p><b>1 보다 작아야 한다.</b> 두 상한이 같으면 노드 상한이 먼저 차거나
     * 동시에 차서 쿠폰별 상한이 죽은 분기가 된다. Phase 9 를 통과하면 1.0 이다.
     */
    private static final double IDLE_CREDIT_RATIO = 0.7;


    /**
     * 조회를 모으는 필터.
     *
     * <p><b>화이트리스트로만 켠다.</b> 목록에 없는 경로는 그대로 프록시한다 —
     * 기본이 켜짐이면 개인화된 응답이 붙는 순간 남의 응답을 받는다.
     */
    @Bean
    public QueryCoalescingFilter queryCoalescingFilter(CoalescingProperties props,
            Clock clock, MeterRegistry meters) {
        QueryCoalescingFilter filter = QueryCoalescingFilter.of(props, clock, meters);
        // **상한에 닿으면 모으기가 조용히 멎는다.** 뒷단 도달 수만 원상복귀하고
        // 그림에는 아무것도 안 남으므로 게이지로 낸다 (6.10.9 · 6.10.10).
        filter.bindMetrics(meters);
        return filter;
    }

    /**
     * 매진 관찰을 담는 곳 (7.2 · B-10).
     *
     * <p><b>담는 쪽과 읽는 쪽이 같은 것을 봐야 한다</b> — 각자 만들면 뒷단이
     * 낸 매진을 판정이 영영 못 본다. 그래서 빈 하나로 둔다.
     */
    @Bean
    public SoldOutCache soldOutCache(SoldOutCacheProperties props, MeterRegistry meters) {
        SoldOutCache cache = SoldOutCache.of(props.ttl(), props.maxKeys());
        // **차오르는 중인지는 막힌 뒤에 오르는 카운터로 못 본다.** 상한에 닿아
        // 새 관찰을 못 받기 시작하면 그때부터 뒷단이 다시 다 맞는다.
        cache.bindMetrics(meters);
        return cache;
    }

    /** 뒷단이 낸 매진을 관찰만 한다. 응답은 안 바꾼다 (7.2.2). */
    @Bean
    public SoldOutObserver soldOutObserver(SoldOutCache cache, SnapshotHolder holder,
            MeterRegistry meters) {
        return SoldOutObserver.ofSnapshot(cache, holder, meters);
    }

    /** 못 읽는 대역은 여기서 버린다. 요청 경로에서 다시 풀면 그 파싱이 거기 붙는다. */
    @Bean
    public TrustedProxies trustedProxies(ProxyProperties properties) {
        return properties.trusted();
    }

    /** 비밀키가 없거나 짧으면 여기서 기동이 멎는다. 약한 키로 조용히 돌지 않는다. */
    @Bean
    public QueueToken queueToken(QueueTokenProperties properties) {
        return properties.queueToken();
    }

    /** 같은 비밀키를 쓰되 접두와 수명이 다르다. 한쪽 토큰이 다른 쪽에서 안 통한다. */
    @Bean
    public EntryToken entryToken(QueueTokenProperties properties) {
        return EntryToken.of(properties.secret());
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
     *
     * <p>이 인스턴스는 <b>쿠폰으로 센다.</b> 그래서 격벽·래치와 같은 상한을 쓴다
     * (6.3.5). 클라이언트 식별자로 세는 남용 리미터는 키 공간이 달라 별개다.
     */
    @Bean
    public SecondWindowLimiter admissionLimiter() {
        return SecondWindowLimiter.withMaxKeys(CouponKeys.MAX);
    }

    /**
     * 멱등 키는 <b>비밀키를 안 쓴다</b> (CY-830).
     *
     * <p>클라이언트가 준 UUID 를 그대로 넘긴다. 뒷단이 그 형식만 받고, 도용
     * 방어는 뒷단이 회원과 키의 쌍으로 저장해서 진다.
     */
    @Bean
    public IdempotencyKey idempotencyKey() {
        return IdempotencyKey.passThrough();
    }
}

package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;

/**
 * 배선된 점검기가 <b>도는 레디스에 실제로 묻는가</b>. 7.x 의 기본값도 noeviction 이라, 안전만 보면
 * 답과 무관하게 안전을 내는 배선을 못 가른다. 정책을 바꿔 위험도 받는다.
 */
@Tag("integration")
@SpringBootTest
class EvictionPolicyWiringTest extends RedisContainerSupport {

    @Autowired
    private EvictionPolicyCheck check;

    @Autowired
    private MeterRegistry meters;

    @Test
    @DisplayName("운영_설정의_레디스는_안전하다")
    void 운영_설정의_레디스는_안전하다() throws Exception {
        // 기본값과 다른 상한으로 설정 파일이 실제로 읽혔는지를 가른다.
        assertThat(설정을_읽는다("maxmemory")).as("전제 — 운영 설정 파일이 적용됐다").isEqualTo("1073741824");

        StepVerifier.create(check.check()).expectNext(EvictionPolicyCheck.SAFE).verifyComplete();
        assertThat(check.isRunning()).as("컨텍스트가 루프를 띄웠다").isTrue();
        assertThat(meters.get(EvictionPolicyCheck.METRIC).gauge().value())
                .as("스크레이프되는 레지스트리에 선다").isEqualTo(EvictionPolicyCheck.SAFE);
    }

    @Test
    @DisplayName("정책을_바꾸면_위험을_내고_되돌리면_안전으로_온다")
    void 정책을_바꾸면_위험을_내고_되돌리면_안전으로_온다() throws Exception {
        String 원래 = 설정을_읽는다("maxmemory-policy");
        REDIS.execInContainer("redis-cli", "CONFIG", "SET", "maxmemory-policy", "allkeys-lru");
        try {
            assertThat(설정을_읽는다("maxmemory-policy")).as("전제 — 정책을 바꿨다").isEqualTo("allkeys-lru");

            StepVerifier.create(check.check()).expectNext(EvictionPolicyCheck.UNSAFE).verifyComplete();
        } finally {
            // 컨테이너를 JVM 안의 다른 시험과 나눠 쓴다. 되돌린 값을 다시 읽어 확인한다.
            REDIS.execInContainer("redis-cli", "CONFIG", "SET", "maxmemory-policy", 원래);
            assertThat(설정을_읽는다("maxmemory-policy")).as("정책을 되돌렸다").isEqualTo(원래);
        }
        StepVerifier.create(check.check()).expectNext(EvictionPolicyCheck.SAFE).verifyComplete();
    }
}

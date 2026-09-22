package com.kafkick.waiting.adapter.redis;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;

/**
 * 배선된 점검기가 <b>도는 레디스에 실제로 묻는가</b>. 운영과 같은 설정 파일로 띄운 컨테이너라 답이 안전이어야 한다.
 */
@Tag("integration")
@SpringBootTest
class EvictionPolicyWiringTest extends RedisContainerSupport {

    @Autowired
    private EvictionPolicyCheck check;

    @Test
    @DisplayName("운영_설정의_레디스는_안전하다")
    void 운영_설정의_레디스는_안전하다() {
        StepVerifier.create(check.check()).expectNext(EvictionPolicyCheck.SAFE).verifyComplete();
    }
}

package com.kafkick.waiting;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 기동 스모크.
 *
 * <p>컨텍스트가 뜨는지만 본다. 빈 배선이 깨지면 여기서 먼저 걸리고,
 * 그러지 않으면 부하 시험 전까지 아무도 모른다.
 */
@SpringBootTest
class WaitingApplicationTest {

    @Test
    @DisplayName("애플리케이션_컨텍스트가_뜬다")
    void 애플리케이션_컨텍스트가_뜬다() {
        // 컨텍스트 로딩 실패 시 이 테스트가 실행되기 전에 예외가 난다.
        // 단언이 없는 것이 아니라, 로딩 자체가 단언이다.
    }
}

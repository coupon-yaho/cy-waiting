package com.kafkick.waiting;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 시계를 <b>한 곳에서</b> 준다.
 *
 * <p>각자 {@code Clock.systemUTC()} 를 부르면 시험이 고정한 시계가 어디에는 안
 * 들어가고, 그 자리만 실제 시계로 돈다 — 그 시험은 장비 속도에 걸린다.
 *
 * <p>기본값을 남긴 팩토리가 하나 있다. 운영 배선은 그걸 안 쓴다.
 *
 * <p>계층에 안 둔다. 시계는 어느 계층의 것도 아니다.
 */
@Configuration
public class TimeConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}

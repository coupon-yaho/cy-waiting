package com.kafkick.waiting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 적응형 대기열 게이트웨이 진입점.
 *
 * <p>이 게이트웨이는 <b>입장(admission)</b>만 소유한다. 발급과 재고 차감은
 * 쿠폰 서비스가 한다.
 */
@SpringBootApplication
public class WaitingApplication {

    public static void main(String[] args) {
        SpringApplication.run(WaitingApplication.class, args);
    }
}

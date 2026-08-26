package com.kafkick.waiting.control;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 종료할 때 쓰는 값입니다.
 *
 * <p><b>앞단 부하 분산기 설정과 짝입니다.</b> 한쪽만 바꾸면 어긋나므로 값의 근거를
 * {@code application.yml} 에 함께 적어 둡니다.
 *
 * @param lbRemovalWait readiness 를 내린 뒤 부하 분산기가 우리를 뺄 때까지 기다리는
 *     시간입니다. 앞단 체크 주기 × 실패 횟수보다 커야 합니다
 */
@ConfigurationProperties("waiting.shutdown")
public record ShutdownProperties(Duration lbRemovalWait) {

    public ShutdownProperties {
        // **기본값을 안 둡니다.** 코드에도 값이 있으면 yml 의 키를 잘못 적어도
        // 조용히 그 값으로 떨어지고, 기동은 성공합니다.
        if (lbRemovalWait == null) {
            throw new IllegalArgumentException("waiting.shutdown.lb-removal-wait 를 적어야 한다");
        }
    }
}

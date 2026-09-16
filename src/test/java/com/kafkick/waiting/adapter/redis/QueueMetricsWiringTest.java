package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 등록 왕복 지표가 <b>스크레이프에 실제로 나오는가</b> (CY-936).
 */
// 어댑터 시험은 제가 만든 레지스트리를 제가 읽는다. 착수 게이트가 읽는 것은 관리 포트의
// 프로메테우스 이름이고, 그 이름은 이미 한 번 바뀌었다. 빈이 레지스트리를 못 받거나 이름이
// 또 바뀌면 러너는 경고 한 줄, 판정기는 "없음" 한 줄만 내고 아무 데서도 안 빨개진다.
@Tag("context")
@SpringBootTest
class QueueMetricsWiringTest {

    /** 러너가 긁는 이름 그대로다. 여기를 고치면 test/load 의 grep 도 같이 고친다. */
    private static final String 등록_왕복 = "waiting_queue_enqueue_latency_seconds";

    @Autowired
    private PrometheusMeterRegistry registry;

    @Test
    @DisplayName("등록_왕복이_스크레이프에_나온다")
    void 등록_왕복이_스크레이프에_나온다() {
        String 스크레이프 = registry.scrape();

        // 타이머는 생성자에서 등록되므로 부하 없이도 나온다 — 나오지 않으면 배선이 끊긴 것이다.
        assertThat(스크레이프).contains(등록_왕복);
        // **성공과 실패를 가른 라벨까지 본다.** 라벨이 사라지면 판정기가 취소까지 섞인 값을 읽는다.
        assertThat(스크레이프).contains(등록_왕복 + "_count{application=\"waiting\",outcome=\"success\"");
        assertThat(스크레이프).contains(등록_왕복 + "_count{application=\"waiting\",outcome=\"error\"");
        assertThat(스크레이프).contains(등록_왕복 + "_count{application=\"waiting\",outcome=\"cancelled\"");
        // **분위수 줄까지 본다.** 히스토그램을 같이 내면 이 줄이 통째로 사라지는데, 어댑터 시험은 제
        // 레지스트리를 읽어 초록이고 러너는 "없음" 한 줄만 낸다 — 실제로 그렇게 회차 하나를 버렸다.
        registry.get(QueueRedisPort.ENQUEUE_LATENCY).tag("outcome", "success").timer()
                .record(Duration.ofMillis(3));
        assertThat(registry.scrape()).contains("quantile=\"0.99\"");
    }
}

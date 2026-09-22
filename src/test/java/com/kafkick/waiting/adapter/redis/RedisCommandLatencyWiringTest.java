package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * 명령별 지연이 <b>실제 연결에 붙고 스크레이프에 나오는가</b> (CY-936).
 *
 * <p>손으로 조립한 시험은 람다가 빌더를 부른다는 것만 본다. 스프링이 그 커스터마이저를 정말
 * Lettuce 에 넘기는지, 그 이름이 러너가 긁는 이름인지는 여기서만 갈린다.
 */
// 러너는 줄이 없으면 "안 켠 회차" 로 읽는다. 그래서 계기가 끊겨도 경고 한 줄로 흡수되고
// 아무 데서도 안 빨개진다 — 같은 이유로 QueueMetricsWiringTest 가 섰고, 그때 회차 하나를 버렸다.
@Tag("integration")
@SpringBootTest(properties = "waiting.redis.command-latency.enabled=true")
class RedisCommandLatencyWiringTest extends RedisContainerSupport {

    /** 러너가 긁는 이름 그대로다. 여기를 고치면 test/load 의 grep 도 같이 고친다. */
    private static final String 첫_응답 = "lettuce_command_firstresponse_seconds";

    private static final String 완료 = "lettuce_command_completion_seconds";

    private static final Duration 기다림 = Duration.ofSeconds(5);

    @Autowired
    private LettuceConnectionFactory factory;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private PrometheusMeterRegistry registry;

    /** 자동설정이 빈을 안 받아 가면 다섯 시험이 초록인 채로 회차에서 계기만 사라진다. */
    @Test
    @DisplayName("스프링이_기록기를_레티스에_넘긴다")
    void 스프링이_기록기를_레티스에_넘긴다() {
        assertThat(factory.getClientConfiguration().getClientResources())
                .get()
                .extracting(자원 -> 자원.commandLatencyRecorder())
                .isInstanceOf(MicrometerCommandLatencyRecorder.class);
    }

    /**
     * <b>이름까지 본다.</b> 타이머는 첫 명령에서 생기므로 한 번 친 뒤에 긁는다 — 안 치면 배선이
     * 멀쩡해도 줄이 없어, 러너와 같은 거짓 음성을 시험이 그대로 되풀이한다.
     */
    @Test
    @DisplayName("스크레이프에_명령별_지연이_나온다")
    void 스크레이프에_명령별_지연이_나온다() {
        redis.opsForValue().set("cy936:wiring", "1").block(기다림);

        String 스크레이프 = registry.scrape();

        // **둘 다 친 명령으로 찾는다.** 이름만 보면 다른 명령의 줄로도 통과하고, 그러면 한쪽
        // 타이머가 통째로 안 나와도 초록이다. 명령 라벨까지 봐야 등록이 폴링·배분과 한 통이라는
        // 사실도 보인다.
        assertThat(줄수(스크레이프, 첫_응답)).as("친 명령의 첫 응답 줄").isEqualTo(1);
        assertThat(줄수(스크레이프, 완료)).as("친 명령의 완료 줄").isEqualTo(1);
    }

    private long 줄수(String 스크레이프, String 이름) {
        return 스크레이프.lines()
                .filter(줄 -> 줄.startsWith(이름 + "_count{"))
                .filter(줄 -> 줄.contains("command=\"SET\""))
                .count();
    }
}

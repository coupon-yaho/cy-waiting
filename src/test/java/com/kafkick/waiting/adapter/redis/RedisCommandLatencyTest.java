package com.kafkick.waiting.adapter.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.data.redis.autoconfigure.ClientResourcesBuilderCustomizer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * 등록 왕복을 대기와 실행으로 가른다 (CY-936).
 *
 * <p>등록 타이머는 부른 시점부터 결과까지라 클라이언트 대기가 섞여 있다. 레디스가 한가한데 왕복이
 * 반 초였던 실측을 이 둘로 갈라야 읽는다 — 그 대기는 레디스를 쪼개서 안 줄어든다.
 */
class RedisCommandLatencyTest {

    private static final SocketAddress 로컬 = InetSocketAddress.createUnresolved("localhost", 6379);

    private SimpleMeterRegistry 레지스트리;

    private ClientResources 걸린_자원() {
        레지스트리 = new SimpleMeterRegistry();
        ClientResources.Builder builder = ClientResources.builder();
        new RedisCommandLatencyConfig().redisCommandLatency(레지스트리).customize(builder);
        return builder.build();
    }

    private static final Offset<Double> 오차 = Offset.offset(0.01);

    private double 잰_밀리초(String 이름, String 명령) {
        return 레지스트리.get(이름).tag("command", 명령).timer()
                .totalTime(TimeUnit.MILLISECONDS);
    }

    private double 첫_응답이_잰_밀리초(String 명령) {
        return 잰_밀리초("lettuce.command.firstresponse", 명령);
    }

    private double 완료가_잰_밀리초(String 명령) {
        return 잰_밀리초("lettuce.command.completion", 명령);
    }

    private void 명령을_기록한다(ClientResources 자원, ProtocolKeyword 명령,
            Duration 첫_응답, Duration 완료) {
        자원.commandLatencyRecorder().recordCommandLatency(
                로컬, 로컬, 명령, 첫_응답.toNanos(), 완료.toNanos());
    }

    /** 기록기가 안 붙으면 Lettuce 는 조용히 아무것도 안 낸다 — 빨개지는 자리가 없다. */
    @Test
    @DisplayName("명령_지연_기록기가_붙는다")
    void 명령_지연_기록기가_붙는다() {
        ClientResources 자원 = 걸린_자원();
        try {
            assertThat(자원.commandLatencyRecorder())
                    .isInstanceOf(MicrometerCommandLatencyRecorder.class);
            assertThat(자원.commandLatencyRecorder().isEnabled())
                    .as("꺼져 있으면 붙어도 안 센다").isTrue();
        } finally {
            자원.shutdown();
        }
    }

    /**
     * <b>스크립트 호출이 명령 이름으로 갈린다.</b> 명령을 안 가르면 등록의 실행 시간이 폴링·배분과
     * 한 통에 섞여, 어느 왕복이 느린지가 사라진다.
     */
    @Test
    @DisplayName("스크립트_호출의_실행_시간이_명령별로_나온다")
    void 스크립트_호출의_실행_시간이_명령별로_나온다() {
        ClientResources 자원 = 걸린_자원();
        try {
            명령을_기록한다(자원, CommandType.EVALSHA, Duration.ofMillis(3), Duration.ofMillis(4));
            명령을_기록한다(자원, CommandType.GET, Duration.ofMillis(1), Duration.ofMillis(1));

            assertThat(완료가_잰_밀리초("EVALSHA"))
                    .as("스크립트 호출이 제 이름으로 선다").isEqualTo(4.0, 오차);
            assertThat(완료가_잰_밀리초("GET"))
                    .as("다른 명령과 한 통에 안 섞인다").isEqualTo(1.0, 오차);
        } finally {
            자원.shutdown();
        }
    }

    /**
     * <b>첫 응답과 완료를 따로 낸다.</b> 둘의 차가 곧 클라이언트가 결과를 받아 넘기기까지의 몫이라,
     * 하나만 내면 등록 타이머와 견줄 것이 없어진다.
     */
    @Test
    @DisplayName("첫_응답과_완료가_따로_나온다")
    void 첫_응답과_완료가_따로_나온다() {
        ClientResources 자원 = 걸린_자원();
        try {
            명령을_기록한다(자원, CommandType.EVALSHA, Duration.ofMillis(3), Duration.ofMillis(9));

            // 둘이 뒤바뀌면 우리 쪽 몫이 음수로 나온다. 값까지 봐야 그것이 갈린다.
            assertThat(첫_응답이_잰_밀리초("EVALSHA"))
                    .as("첫 응답까지가 레디스가 실제로 문 시간이다").isEqualTo(3.0, 오차);
            assertThat(완료가_잰_밀리초("EVALSHA"))
                    .as("완료까지는 클라이언트 몫이 더 붙는다").isEqualTo(9.0, 오차);
        } finally {
            자원.shutdown();
        }
    }

    private ApplicationContextRunner 러너() {
        return new ApplicationContextRunner()
                .withUserConfiguration(RedisCommandLatencyConfig.class)
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new);
    }

    /** 켜는 값이 틀려도 시험이 통째로 통과하면, 회차에서 계기가 없는 것을 그때 안다. */
    @Test
    @DisplayName("기본은_안_붙는다")
    void 기본은_안_붙는다() {
        러너().run(맥락 -> assertThat(맥락).doesNotHaveBean(ClientResourcesBuilderCustomizer.class));
    }

    @Test
    @DisplayName("켜면_붙는다")
    void 켜면_붙는다() {
        러너().withPropertyValues("waiting.redis.command-latency.enabled=true")
                .run(맥락 -> assertThat(맥락).hasSingleBean(ClientResourcesBuilderCustomizer.class));
    }
}

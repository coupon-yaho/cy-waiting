package com.kafkick.waiting.gateway;

import io.netty.channel.ChannelOption;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.reactor.netty.NettyServerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 수락 대기열 크기. <b>기본값에 맡기면 한 초의 폭주를 못 받는다</b> — 넘친 연결 요청은 커널이
 * 버리고, 재시도마저 실패하면 처리도 응답도 없이 끊긴다. 실제 크기는 커널 한도와 작은 쪽이다.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AcceptBacklog.Properties.class)
public class AcceptBacklog {

    @Bean
    NettyServerCustomizer acceptBacklogCustomizer(Properties properties) {
        return customizer(properties);
    }

    static NettyServerCustomizer customizer(Properties properties) {
        return server -> server.option(ChannelOption.SO_BACKLOG, properties.acceptBacklog());
    }

    /** 커널 기본(4,096)의 네 배. 커널 쪽도 같이 올려야 뜻이 있다. */
    @ConfigurationProperties(prefix = "waiting.server")
    public record Properties(Integer acceptBacklog) {

        static final int DEFAULT = 16_384;

        public Properties {
            acceptBacklog = acceptBacklog == null ? DEFAULT : acceptBacklog;
            if (acceptBacklog <= 0) {
                throw new IllegalArgumentException(
                        "waiting.server.accept-backlog 는 양수여야 한다: " + acceptBacklog);
            }
        }
    }
}

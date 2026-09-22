package com.kafkick.waiting.gateway;

import io.netty.channel.ChannelOption;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.OptionalInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(AcceptBacklog.class);

    private static final Path SOMAXCONN = Path.of("/proc/sys/net/core/somaxconn");

    @Bean
    NettyServerCustomizer acceptBacklogCustomizer(Properties properties) {
        int wanted = properties.acceptBacklog();
        // **작아지면 말한다.** 커널 한도가 작으면 아무 말 없이 그 값이 된다 — 여기만 올리고
        // 넘침이 그대로인 배포를 기동 로그가 알린다. 한 번 찍고 끝난다.
        effective(wanted, SOMAXCONN).ifPresentOrElse(actual -> {
            if (actual < wanted) {
                log.warn("수락 대기열 {} 을 요청했는데 커널 한도가 {} 라 실제 {} 이다"
                        + " — net.core.somaxconn 을 같이 올린다", wanted, actual, actual);
            }
        }, () -> log.info("커널 수락 대기열 한도를 못 읽었다 — 실제 크기는 확인 못 함"));
        return customizer(properties);
    }

    /** 요청한 크기와 커널 한도 중 작은 쪽. 한도를 못 읽으면 모른다. */
    static OptionalInt effective(int wanted, Path limitFile) {
        try {
            // **크기에 기대 읽지 않는다.** proc 파일은 크기를 0 으로 보고해서, 그 크기로 읽는
            // 방식은 첫 글자만 가져온다 — 4096 이 4 가 된다. 줄 단위로 끝까지 읽는다.
            int limit = Integer.parseInt(Files.readAllLines(limitFile).getFirst().trim());
            return OptionalInt.of(Math.min(wanted, limit));
        } catch (IOException | NumberFormatException | NoSuchElementException e) {
            return OptionalInt.empty();
        }
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

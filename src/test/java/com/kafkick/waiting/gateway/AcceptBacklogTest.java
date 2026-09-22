package com.kafkick.waiting.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.channel.ChannelOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.netty.http.server.HttpServer;

/**
 * 연결 폭주 때 수락 대기열이 넘치지 않게 크기를 정하는가 (CY-986).
 *
 * <p>2 만 명이 1 초 안에 붙은 회차에서 커널이 연결 요청을 7 만 번 버렸고, 재시도마저 실패한 요청은
 * 응답 없이 끊겼다. 기본값에 맡겨 둔 자리다.
 */
class AcceptBacklogTest {

    @Test
    @DisplayName("기본 크기는 커널 기본값보다 크다")
    void 기본값() {
        assertThat(new AcceptBacklog.Properties(null).acceptBacklog())
                .as("커널 기본 4,096 으로는 한 초의 폭주를 못 받는다")
                .isGreaterThan(4_096);
    }

    @Test
    @DisplayName("설정한 크기가 서버 옵션으로 들어간다")
    void 서버_옵션() {
        HttpServer 서버 = AcceptBacklog.customizer(new AcceptBacklog.Properties(20_000))
                .apply(HttpServer.create());

        assertThat(서버.configuration().options().get(ChannelOption.SO_BACKLOG))
                .as("값만 받고 안 꽂으면 기본값 그대로다").isEqualTo(20_000);
    }

    @Test
    @DisplayName("0 이하는 막는다")
    void 잘못된_값() {
        assertThatThrownBy(() -> new AcceptBacklog.Properties(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

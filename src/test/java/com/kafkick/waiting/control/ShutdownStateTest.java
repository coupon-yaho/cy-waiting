package com.kafkick.waiting.control;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * 종료 신호를 두 판정이 함께 본다.
 *
 * <p>한쪽에 갇힌 플래그로는 "받는 것은 끊고 살아 있음은 유지" 를 표현할 수 없다.
 */
class ShutdownStateTest {

    private final ShutdownState state = ShutdownState.create();

    private ListAppender<ILoggingEvent> 로그;

    @BeforeEach
    void 로그를_받는다() {
        로그 = new ListAppender<>();
        로그.start();
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ShutdownState.class))
                .addAppender(로그);
    }

    @AfterEach
    void 로그를_뗀다() {
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ShutdownState.class))
                .detachAppender(로그);
    }

    @Test
    @DisplayName("처음에는_드레이닝이_아니다")
    void 처음에는_드레이닝이_아니다() {
        assertThat(state.isDraining()).isFalse();
    }

    @Test
    @DisplayName("알리면_드레이닝이다")
    void 알리면_드레이닝이다() {
        state.draining();

        assertThat(state.isDraining()).isTrue();
    }

    @Test
    @DisplayName("되돌릴_수_없는_전환이라_한_번만_남긴다")
    void 되돌릴_수_없는_전환이라_한_번만_남긴다() {
        // 전이 로그가 없으면 언제부터 뺐는지 사후에 못 찾는다. 그렇다고 부를
        // 때마다 찍으면 종료 경로가 여러 번 부를 때 줄이 는다.
        state.draining();
        state.draining();
        state.draining();

        assertThat(로그.list).filteredOn(e -> e.getLevel() == Level.INFO
                && e.getMessage().contains("드레이닝 시작")).hasSize(1);
    }
}

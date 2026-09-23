package com.kafkick.waiting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 아무도 안 듣는 자리를 고른다. <b>임시 포트 대역에서 고르면 남이 문다</b> — 실제로 CI 의 커버리지
 * 회차가 그렇게 빨개졌다. 물린 자리는 버리고 다시 고르고, 끝내 못 고르면 그 사실을 말한다.
 */
@Tag("unit")
class DeadPortsTest {

    @Test
    @DisplayName("아무도_안_듣는_자리를_고른다")
    void 아무도_안_듣는_자리를_고른다() throws IOException {
        int port = DeadPorts.pick();

        assertThat(port).isBetween(DeadPorts.LOW, DeadPorts.HIGH);
        try (Socket probe = new Socket()) {
            assertThatThrownBy(() -> probe.connect(new InetSocketAddress("127.0.0.1", port), 200))
                    .as("거절이 정상이다").isInstanceOf(IOException.class);
        }
    }

    /** 임시 포트 대역을 피한다. 거기서 고르면 다른 프로세스의 연결이 그 자리를 문다. */
    @Test
    @DisplayName("임시_포트_대역_밖에서_고른다")
    void 임시_포트_대역_밖에서_고른다() throws IOException {
        for (int i = 0; i < 20; i++) {
            assertThat(DeadPorts.pick()).isLessThan(32768);
        }
    }

    @Test
    @DisplayName("물린_자리는_버리고_다시_고른다")
    void 물린_자리는_버리고_다시_고른다() {
        List<Integer> 본_것 = new ArrayList<>();
        // 앞의 셋은 누가 듣고 있는 자리다. 넷째부터 비어 있다.
        IntPredicate 듣고있나 = port -> {
            본_것.add(port);
            return 본_것.size() <= 3;
        };

        int port = DeadPorts.pick(듣고있나);

        assertThat(본_것).hasSize(4);
        assertThat(port).isEqualTo(본_것.get(3));
    }

    /** 다 물렸으면 그 사실을 말한다. 아무 자리나 돌려주면 그 시험이 엉뚱한 프로세스로 프록시한다. */
    @Test
    @DisplayName("끝내_못_고르면_왜인지_말한다")
    void 끝내_못_고르면_왜인지_말한다() {
        assertThatThrownBy(() -> DeadPorts.pick(port -> true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(String.valueOf(DeadPorts.ATTEMPTS));
    }

    /** 고른 자리를 실제로 쓸 수 있어야 한다 — 거절을 재는 시험이 그 자리에 붙는다. */
    @Test
    @DisplayName("고른_자리는_다시_띄울_수_있다")
    void 고른_자리는_다시_띄울_수_있다() throws IOException {
        int port = DeadPorts.pick();

        try (ServerSocket server = new ServerSocket(port)) {
            assertThat(server.getLocalPort()).isEqualTo(port);
        }
    }
}

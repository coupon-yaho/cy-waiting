package com.kafkick.waiting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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

        // 상수가 아니라 값으로 못 박는다. 상수끼리 견주면 대역을 옮겨도 이 단언이 따라간다.
        assertThat(port).isBetween(20_000, 29_999);
        try (Socket probe = new Socket()) {
            assertThatThrownBy(() -> probe.connect(new InetSocketAddress("127.0.0.1", port), 200))
                    .as("타임아웃이 아니라 거절이다").isInstanceOf(ConnectException.class);
        }
    }

    /**
     * 임시 포트 대역을 피한다. 거기서 고르면 다른 프로세스의 연결이 그 자리를 문다.
     *
     * <p><b>전제를 커널에서 읽어 확인한다</b> — 이미지에 따라 그 대역이 1024 부터 열리고,
     * 그러면 이 파일의 대역이 조용히 무의미해진다.
     */
    @Test
    @DisplayName("임시_포트_대역_밖에서_고른다")
    void 임시_포트_대역_밖에서_고른다() throws IOException {
        // **전제를 이 호스트에서 확인한다.** 커널이 우리 대역을 나눠 주면 고르는 의미가 없다.
        List<ServerSocket> 임시 = new ArrayList<>();
        try {
            for (int i = 0; i < 30; i++) {
                ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress());
                임시.add(socket);
                assertThat(socket.getLocalPort())
                        .as("커널이 우리 대역을 나눠 준다 — 다른 대역을 골라야 한다")
                        .matches(임시포트 -> 임시포트 < 20_000 || 임시포트 > 29_999, "우리 대역 밖");
            }
        } finally {
            for (ServerSocket socket : 임시) {
                socket.close();
            }
        }
        for (int i = 0; i < 20; i++) {
            assertThat(DeadPorts.pick()).isLessThan(32_768);
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

    /**
     * 다 물렸으면 그 사실을 말한다. <b>몇 번 찾는지도 값으로 못 박는다</b> — 상수를 상수로 재면
     * 시도 수를 1 로 줄여도 초록이다.
     */
    @Test
    @DisplayName("끝내_못_고르면_왜인지_말한다")
    void 끝내_못_고르면_왜인지_말한다() {
        AtomicInteger 본_수 = new AtomicInteger();

        assertThatThrownBy(() -> DeadPorts.pick(port -> {
            본_수.incrementAndGet();
            return true;
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("50");
        assertThat(본_수).hasValue(50);
    }

    /** 고른 자리를 실제로 쓸 수 있어야 한다 — 거절을 재는 시험이 그 자리에 붙는다. */
    @Test
    @DisplayName("고른_자리는_다시_띄울_수_있다")
    void 고른_자리는_다시_띄울_수_있다() throws IOException {
        int port = DeadPorts.pick();

        // 탐침과 같은 주소로 묶는다. 와일드카드로 묶으면 다른 인터페이스에 물린 자리에서 갈린다.
        try (ServerSocket server = new ServerSocket(port, 0, InetAddress.getLoopbackAddress())) {
            assertThat(server.getLocalPort()).isEqualTo(port);
        }
    }
}

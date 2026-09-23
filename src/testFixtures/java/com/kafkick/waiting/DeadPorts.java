package com.kafkick.waiting;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntPredicate;

/**
 * 아무도 안 듣는 자리. 연결 거절을 재는 시험이 쓴다.
 *
 * <p><b>임시 포트 대역에서 고르면 남이 문다.</b> 띄웠다 내린 포트를 쓰던 방식이 CI 에서 실제로
 * 물려, 그 회차가 판정 불가로 끊겼다. 커널이 안 나눠 주는 대역에서 고르고 거절을 확인한다.
 */
public final class DeadPorts {

    /** 임시 포트 대역(리눅스 기본 32768 위) 아래에서 고른다. 커널이 이 자리를 안 나눠 준다. */
    public static final int LOW = 20_000;

    public static final int HIGH = 29_999;

    public static final int ATTEMPTS = 50;

    private DeadPorts() {
    }

    /** @throws IllegalStateException 시도한 자리가 다 물렸을 때 */
    public static int pick() {
        return pick(DeadPorts::listening);
    }

    /** @param listening 그 자리에 누가 듣고 있는가. 시험이 갈아 끼운다 */
    public static int pick(IntPredicate listening) {
        for (int i = 0; i < ATTEMPTS; i++) {
            int port = ThreadLocalRandom.current().nextInt(LOW, HIGH + 1);
            if (!listening.test(port)) {
                return port;
            }
        }
        throw new IllegalStateException(
                "안 듣는 자리를 %d 번 찾았는데 다 물렸다 — 이 실행으로는 못 잰다".formatted(ATTEMPTS));
    }

    private static boolean listening(int port) {
        try (Socket probe = new Socket()) {
            probe.connect(new InetSocketAddress("127.0.0.1", port), 200);
            return true;
        } catch (IOException refused) {
            return false;
        }
    }
}

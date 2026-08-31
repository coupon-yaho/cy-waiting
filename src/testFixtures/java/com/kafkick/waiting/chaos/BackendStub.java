package com.kafkick.waiting.chaos;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * 시나리오가 쓰는 뒷단. <b>받은 수와 중복을 센다.</b>
 *
 * <p>중복은 비율로 못 잡는다 — 로컬에서 끝난 요청이 분모에만 들어가 그만큼
 * 여유가 생기고, 그 안에 숨는다. 요청을 짚어 세면 그 여유가 없다.
 */
// 두 시나리오가 같은 블록을 글자 그대로 들고 있었다. 판정을 시나리오마다 다시
// 쓰면 "전 시나리오 중복 0" 같은 게이트가 시나리오마다 다른 것을 재게 된다.
public final class BackendStub implements AutoCloseable {

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong duplicated = new AtomicLong();
    private final Set<String> seen = ConcurrentHashMap.newKeySet();
    private final DisposableServer server;

    private BackendStub(BooleanSupplier stalled, BooleanSupplier failing) {
        this.server = HttpServer.create()
                .port(0)
                .handle((request, response) -> {
                    received.incrementAndGet();
                    // 회원 번호는 시험 전체에서 안 겹치게 발급한다. 겹쳐
                    // 도착하면 게이트웨이가 한 요청을 두 번 보낸 것이다.
                    String member = request.requestHeaders().get("X-Member-Id");
                    if (member != null && !seen.add(member)) {
                        duplicated.incrementAndGet();
                    }
                    // **멎은 것과 5xx 는 다른 갈래다.** 앞엣것은 응답이 아예
                    // 안 오는 것이고 뒤엣것은 오긴 오는데 실패인 것이다.
                    // 서킷이 여는 근거가 갈리므로 스텁이 둘을 구분해야 한다.
                    if (stalled.getAsBoolean()) {
                        return Mono.never();
                    }
                    return response.status(failing.getAsBoolean() ? 500 : 200).send();
                })
                .bindNow();
    }

    /** 늘 200 을 내는 뒷단. */
    public static BackendStub 항상_받는다() {
        return new BackendStub(() -> false, () -> false);
    }

    /** 스위치가 켜지면 응답을 안 내는 뒷단. 무응답 갈래를 만든다. */
    public static BackendStub 멎을_수_있다(BooleanSupplier 멎었나) {
        return new BackendStub(멎었나, () -> false);
    }

    /** 스위치가 켜지면 5xx 를 내는 뒷단. 응답은 오는데 실패인 갈래다. */
    public static BackendStub 실패할_수_있다(BooleanSupplier 실패하나) {
        return new BackendStub(() -> false, 실패하나);
    }

    public int port() {
        return server.port();
    }

    public long 받은_수() {
        return received.get();
    }

    /** 뒷단이 같은 요청을 두 번 받았는가. 발급 경로에서 그건 초과 발급이다. */
    public Optional<String> 중복_수신이_없다() {
        long 중복 = duplicated.get();
        return 중복 == 0 ? Optional.empty()
                : Optional.of("RC4 뒷단이 같은 요청을 %d 건 두 번 받았다".formatted(중복));
    }

    @Override
    public void close() {
        server.disposeNow();
    }
}

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
    private final ConcurrentHashMap<String, AtomicLong> perCoupon = new ConcurrentHashMap<>();
    private final AtomicLong duplicated = new AtomicLong();
    private final Set<String> seen = ConcurrentHashMap.newKeySet();
    private final DisposableServer server;

    private BackendStub(BooleanSupplier stalled) {
        this.server = HttpServer.create()
                .port(0)
                .handle((request, response) -> {
                    received.incrementAndGet();
                    perCoupon.computeIfAbsent(쿠폰을_뽑는다(request.uri()),
                            key -> new AtomicLong()).incrementAndGet();
                    // 회원 번호는 시험 전체에서 안 겹치게 발급한다. 겹쳐
                    // 도착하면 게이트웨이가 한 요청을 두 번 보낸 것이다.
                    String member = request.requestHeaders().get("X-Member-Id");
                    if (member != null && !seen.add(member)) {
                        duplicated.incrementAndGet();
                    }
                    // **멎은 것은 느린 것이지 거절이 아니다.** 상태를 돌려주면
                    // 서킷이 실패로 안 세고, 그러면 열리는 갈래를 못 밟는다.
                    return stalled.getAsBoolean() ? Mono.never()
                            : response.status(200).send();
                })
                .bindNow();
    }

    /** 늘 200 을 내는 뒷단. */
    public static BackendStub 항상_받는다() {
        return new BackendStub(() -> false);
    }

    /** 스위치가 켜지면 응답을 안 내는 뒷단. 서킷이 열리는 갈래를 만든다. */
    public static BackendStub 멎을_수_있다(BooleanSupplier 멎었나) {
        return new BackendStub(멎었나);
    }

    public int port() {
        return server.port();
    }

    public long 받은_수() {
        return received.get();
    }

    /**
     * 그 쿠폰으로 온 수. <b>전역 차분은 이름대로 안 잰다</b> — 배치 밖 도착 한
     * 건이 "줄을 추월했다" 로 읽히고, 그건 불변식 위반으로 보고되는 계수 오류다.
     */
    public long 받은_수(String couponId) {
        AtomicLong 계수 = perCoupon.get(couponId);
        return 계수 == null ? 0 : 계수.get();
    }

    // /api/v1/coupons/{id}/issue 에서 {id} 를 뗀다. 모양이 다르면 통째로 한
    // 바구니에 담는다 — 못 뗀 것을 조용히 버리면 계수가 소리 없이 샌다.
    private static String 쿠폰을_뽑는다(String uri) {
        String[] 조각 = uri.split("/");
        for (int i = 0; i < 조각.length - 1; i++) {
            if ("coupons".equals(조각[i])) {
                return 조각[i + 1];
            }
        }
        return "";
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

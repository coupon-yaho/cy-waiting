package com.kafkick.waiting.chaos;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import reactor.core.publisher.Flux;
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

    private BackendStub(BooleanSupplier stalled, Predicate<String> failing) {
        this(stalled, () -> false, failing, member -> false, Duration.ZERO);
    }

    private BackendStub(BooleanSupplier stalled, BooleanSupplier bodyStalled,
            Predicate<String> failing, Predicate<String> slowMember, Duration delay) {
        this.server = HttpServer.create()
                .port(0)
                .handle((request, response) -> {
                    received.incrementAndGet();
                    String couponId = 쿠폰을_뽑는다(request.uri());
                    perCoupon.computeIfAbsent(couponId, key -> new AtomicLong()).incrementAndGet();
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
                    // 헤더와 첫 조각만 보내고 본문을 안 끝낸다. 헤더가 나갔으니 서킷은 성공으로 센다.
                    if (bodyStalled.getAsBoolean()) {
                        return response.status(200)
                                .sendString(Flux.concat(Mono.just("{"), Flux.never()));
                    }
                    int status = failing.test(couponId) ? 500 : 200;
                    // **느린 것은 늦게라도 답한다.** 응답 상한 안에 오므로 실패가 아니라
                    // 느린 호출로만 세어진다.
                    if (member != null && slowMember.test(member)) {
                        return Mono.delay(delay).then(response.status(status).send());
                    }
                    return response.status(status).send();
                })
                .bindNow();
    }

    /** 늘 200 을 내는 뒷단. */
    public static BackendStub 항상_받는다() {
        return new BackendStub(() -> false, couponId -> false);
    }

    /** 스위치가 켜지면 응답을 안 내는 뒷단. 무응답 갈래를 만든다. */
    public static BackendStub 멎을_수_있다(BooleanSupplier 멎었나) {
        return new BackendStub(멎었나, couponId -> false);
    }

    /** 스위치가 켜지면 5xx 를 내는 뒷단. 응답은 오는데 실패인 갈래다. */
    public static BackendStub 실패할_수_있다(BooleanSupplier 실패하나) {
        return new BackendStub(() -> false, couponId -> 실패하나.getAsBoolean());
    }

    /**
     * 고른 쿠폰만 5xx 를 내는 뒷단. <b>서킷은 뒷단 전체 하나라</b> 쿠폰 하나의 실패가 무관한
     * 쿠폰까지 막는지를 이것으로 잰다. 쿠폰 아닌 경로(프로브)는 빈 이름으로 묻는다.
     */
    public static BackendStub 쿠폰만_실패한다(Predicate<String> 실패하는_쿠폰) {
        return new BackendStub(() -> false, 실패하는_쿠폰);
    }

    /** 고른 회원에게만 {@code 지연} 뒤 200 을 내는 뒷단. 느린 호출 갈래를 만든다. */
    public static BackendStub 늦게_답한다(Predicate<String> 느린_회원, Duration 지연) {
        return new BackendStub(() -> false, () -> false, couponId -> false, 느린_회원, 지연);
    }

    /** 스위치가 켜지면 헤더 200 뒤 본문을 안 끝내는 뒷단. */
    public static BackendStub 본문을_안_끝낼_수_있다(BooleanSupplier 본문이_멎었나) {
        return new BackendStub(() -> false, 본문이_멎었나, couponId -> false, member -> false,
                Duration.ZERO);
    }

    public int port() {
        return server.port();
    }

    public long 받은_수() {
        return received.get();
    }

    /**
     * 그 쿠폰으로 온 수. <b>추월 판정은 이걸로 센다</b> — 전역 차분은 배치 밖
     * 도착 한 건을 "줄을 추월했다" 로 읽어, 계수 오류가 불변식 위반으로 보고된다.
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

package com.kafkick.waiting.gateway;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/**
 * 같은 키로 동시에 온 요청을 <b>뒷단 한 번</b>으로 모읍니다.
 *
 * <p>발급은 판정이 막아 주는데 조회는 그대로 통과합니다. 오픈 순간 사용자는 큐에
 * 서기 전에 쿠폰 페이지를 먼저 열어서, 그때 뒷단에 전량이 꽂힙니다.
 *
 * @param <T> 뒷단이 돌려주는 것
 */
public final class SingleFlight<T> {

    /** 상한을 안 두면 키 하나가 모으고 있는 사이 맵이 무한히 자란다. */
    private static final int DEFAULT_MAX_KEYS = 10_000;

    private final int maxKeys;

    private final Map<String, Mono<T>> flights = new HashMap<>();

    private SingleFlight(int maxKeys) {
        if (maxKeys < 1) {
            throw new IllegalArgumentException("maxKeys 는 1 이상이어야 한다: " + maxKeys);
        }
        this.maxKeys = maxKeys;
    }

    public static <T> SingleFlight<T> create() {
        return new SingleFlight<>(DEFAULT_MAX_KEYS);
    }

    public static <T> SingleFlight<T> withMaxKeys(int maxKeys) {
        return new SingleFlight<>(maxKeys);
    }

    /**
     * 도는 것이 있으면 거기 붙고, 없으면 시작합니다.
     *
     * <p><b>먼저 온 요청이 끊겨도 나머지는 답을 받아야 합니다.</b> 그래서 뒷단
     * 호출을 구독자와 떼어 놓습니다 — 붙어 있으면 첫 구독자가 취소할 때 뒷단
     * 호출까지 같이 끊겨 뒤엣사람 전부가 빈손이 됩니다.
     *
     * @param call 도는 것이 없을 때 부를 것. 있으면 <b>안 부릅니다</b>
     */
    public Mono<T> join(String key, Supplier<Mono<T>> call) {
        Mono<T> running;
        synchronized (this) {
            running = flights.get(key);
            if (running == null) {
                // **상한을 넘으면 모으지 않고 그냥 보냅니다.** 여기서 거절하면
                // 보호 장치가 조회를 끊는 것이 되고, 그건 없느니만 못합니다.
                if (flights.size() >= maxKeys) {
                    return call.get();
                }
                running = start(key, call);
                flights.put(key, running);
            }
        }
        return running;
    }

    /**
     * 구독자와 뗀 뒷단 호출.
     *
     * <p>{@code cache()} 가 결과를 여럿에게 나눠 주고, {@code publish().refCount()}
     * 를 안 쓰는 것이 요점입니다 — 그쪽은 구독자가 0 이 되면 원본을 끊습니다.
     */
    private Mono<T> start(String key, Supplier<Mono<T>> call) {
        return Mono.defer(call)
                // 어느 쪽으로 끝나도 자리를 비운다. 안 비우면 다음 요청이 지난
                // 응답을 받고, 그때부터 그 키는 영영 갱신 안 된다.
                .doFinally(signal -> remove(key))
                .cache();
    }

    private synchronized void remove(String key) {
        flights.remove(key);
    }

    /** 지금 모으고 있는 키 수. 지표가 이 값을 읽습니다. */
    public synchronized int inFlight() {
        return flights.size();
    }

    /**
     * 상한에 닿았는가.
     *
     * <p><b>닿으면 모으기가 조용히 멎습니다.</b> 뒷단 도달 수만 원상복귀하고
     * 그림에는 아무것도 안 남으므로, 부르는 쪽이 그 사실을 남깁니다.
     */
    public synchronized boolean isFull() {
        return flights.size() >= maxKeys;
    }
}

package com.kafkick.waiting.control;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/**
 * 받아 온 재료와 <b>그것을 잰 시각</b>.
 *
 * <p>두 벽시계의 차로 재면 같은 스냅샷이 노드마다 다르게 낡는다.
 *
 * @param now 읽은 순간의 <b>레디스</b> 시각(초)
 */
public record TimedSnapshot(Map<String, String> hash, long now) {

    public TimedSnapshot {
        Objects.requireNonNull(hash, "hash 는 필수다");
        hash = Map.copyOf(hash);
    }

    /**
     * 시각 없이 온 재료. <b>홀더가 자기 시계로 나이를 잰다</b> — 시험이 쓰는 길이고,
     * 운영 배선은 한 왕복으로 시각을 같이 받는다.
     */
    public static Supplier<Mono<TimedSnapshot>> untimed(
            Supplier<Mono<Map<String, String>>> source) {
        return () -> Mono.defer(source).map(hash -> new TimedSnapshot(hash, 0));
    }
}
